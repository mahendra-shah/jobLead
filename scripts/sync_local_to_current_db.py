#!/usr/bin/env python3
"""
Sync Local Database to Current/Remote Database

This script exports data from your local database and imports it into 
the current/remote database.

Usage:
    python scripts/sync_local_to_current_db.py
    
    Or with custom URLs:
    LOCAL_DB_URL=postgresql://user:pass@localhost:5432/local_db \
    CURRENT_DB_URL=postgresql://user:pass@remote:5432/remote_db \
    python scripts/sync_local_to_current_db.py
"""

import os
import sys
from pathlib import Path
from typing import Optional
import subprocess
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def get_database_url(env_var: str, default: Optional[str] = None) -> str:
    """Get database URL from environment variable."""
    url = os.getenv(env_var, default)
    if not url:
        raise ValueError(f"Environment variable {env_var} is not set")
    return url


def convert_async_url_to_sync(url: str) -> str:
    """Convert asyncpg URL to psycopg2 URL."""
    if url.startswith("postgresql+asyncpg://"):
        url = url.replace("postgresql+asyncpg://", "postgresql://")
    return url


def get_table_names(engine):
    """Get all table names from database."""
    inspector = inspect(engine)
    return inspector.get_table_names()


def export_table_data(engine, table_name: str, output_file: str):
    """Export table data to SQL file using pg_dump."""
    # Extract connection details from URL
    url = str(engine.url)
    # Parse URL: postgresql://user:pass@host:port/dbname
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "")
    
    # Extract components
    if "@" in url:
        auth, rest = url.split("@", 1)
        if ":" in auth:
            user, password = auth.split(":", 1)
        else:
            user = auth
            password = ""
        
        if "/" in rest:
            host_port, dbname = rest.split("/", 1)
            if ":" in host_port:
                host, port = host_port.split(":", 1)
            else:
                host = host_port
                port = "5432"
        else:
            host = rest
            port = "5432"
            dbname = "postgres"
    else:
        raise ValueError("Invalid database URL format")
    
    # Build pg_dump command
    cmd = [
        "pg_dump",
        "-h", host,
        "-p", port,
        "-U", user,
        "-d", dbname,
        "-t", table_name,
        "--data-only",  # Only data, no schema
        "--column-inserts",  # Use INSERT statements
        "-f", output_file
    ]
    
    # Set password via environment variable
    env = os.environ.copy()
    if password:
        env["PGPASSWORD"] = password
    
    try:
        result = subprocess.run(
            cmd,
            env=env,
            check=True,
            capture_output=True,
            text=True
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error exporting {table_name}: {e.stderr}")
        return False
    except FileNotFoundError:
        print("pg_dump not found. Using Python-based export instead...")
        return export_table_data_python(engine, table_name, output_file)


def export_table_data_python(engine, table_name: str, output_file: str):
    """Export table data using SQLAlchemy (fallback method)."""
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {table_name}"))
        rows = result.fetchall()
        
        if not rows:
            print(f"  Table {table_name} is empty, skipping...")
            return True
        
        # Get column names
        columns = result.keys()
        
        # Write to file
        with open(output_file, 'w') as f:
            f.write(f"-- Data for table: {table_name}\n")
            f.write(f"-- Exported at: {datetime.now()}\n\n")
            
            for row in rows:
                values = []
                for val in row:
                    if val is None:
                        values.append("NULL")
                    elif isinstance(val, str):
                        # Escape single quotes
                        val = val.replace("'", "''")
                        values.append(f"'{val}'")
                    elif isinstance(val, (int, float)):
                        values.append(str(val))
                    elif isinstance(val, bool):
                        values.append("TRUE" if val else "FALSE")
                    else:
                        values.append(f"'{str(val)}'")
                
                cols_str = ", ".join(columns)
                vals_str = ", ".join(values)
                f.write(f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str});\n")
        
        return True


def import_table_data(engine, sql_file: str):
    """Import table data from SQL file."""
    if not os.path.exists(sql_file):
        print(f"  SQL file not found: {sql_file}")
        return False
    
    # Read SQL file
    with open(sql_file, 'r') as f:
        sql_content = f.read()
    
    if not sql_content.strip():
        print(f"  SQL file is empty: {sql_file}")
        return True
    
    # Execute SQL
    with engine.connect() as conn:
        try:
            # Split by semicolons and execute each statement
            statements = [s.strip() for s in sql_content.split(';') if s.strip() and not s.strip().startswith('--')]
            
            for statement in statements:
                if statement:
                    conn.execute(text(statement))
            
            conn.commit()
            return True
        except Exception as e:
            print(f"  Error importing data: {e}")
            conn.rollback()
            return False


def sync_databases(
    local_db_url: str,
    current_db_url: str,
    tables: Optional[list] = None,
    clear_existing: bool = False
):
    """
    Sync data from local database to current database.
    
    Args:
        local_db_url: Local database connection URL
        current_db_url: Current/remote database connection URL
        tables: List of table names to sync (None = all tables)
        clear_existing: Whether to clear existing data before importing
    """
    print("🔄 Database Sync: Local → Current")
    print("=" * 60)
    print()
    
    # Convert async URLs to sync
    local_url = convert_async_url_to_sync(local_db_url)
    current_url = convert_async_url_to_sync(current_db_url)
    
    # Create engines
    print("📡 Connecting to databases...")
    local_engine = create_engine(local_url)
    current_engine = create_engine(current_url)
    
    # Get table names
    if tables is None:
        print("📋 Discovering tables...")
        tables = get_table_names(local_engine)
        print(f"   Found {len(tables)} tables")
    
    print()
    print(f"📦 Tables to sync: {', '.join(tables)}")
    print()
    
    # Create temp directory for exports
    temp_dir = Path(__file__).parent.parent / "temp_db_exports"
    temp_dir.mkdir(exist_ok=True)
    
    # Sync each table
    success_count = 0
    failed_tables = []
    
    for table_name in tables:
        print(f"🔄 Syncing table: {table_name}")
        
        # Export from local
        export_file = temp_dir / f"{table_name}.sql"
        print(f"  📤 Exporting from local database...")
        
        if not export_table_data(local_engine, table_name, str(export_file)):
            print(f"  ❌ Failed to export {table_name}")
            failed_tables.append(table_name)
            continue
        
        # Clear existing data if requested
        if clear_existing:
            print(f"  🗑️  Clearing existing data...")
            with current_engine.connect() as conn:
                try:
                    conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))
                    conn.commit()
                except Exception as e:
                    print(f"  ⚠️  Could not truncate: {e}")
        
        # Import to current
        print(f"  📥 Importing to current database...")
        if import_table_data(current_engine, str(export_file)):
            print(f"  ✅ Successfully synced {table_name}")
            success_count += 1
        else:
            print(f"  ❌ Failed to import {table_name}")
            failed_tables.append(table_name)
        
        # Clean up
        if export_file.exists():
            export_file.unlink()
        
        print()
    
    # Summary
    print("=" * 60)
    print("📊 Sync Summary")
    print("=" * 60)
    print(f"✅ Successfully synced: {success_count}/{len(tables)} tables")
    if failed_tables:
        print(f"❌ Failed tables: {', '.join(failed_tables)}")
    print()
    
    # Clean up temp directory
    if temp_dir.exists():
        try:
            temp_dir.rmdir()
        except:
            pass
    
    return success_count == len(tables)


def main():
    """Main function."""
    # Get database URLs
    try:
        local_db_url = get_database_url(
            "LOCAL_DATABASE_URL",
            os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/placement_db")
        )
        
        current_db_url = get_database_url(
            "CURRENT_DATABASE_URL",
            os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/placement_db")
        )
    except ValueError as e:
        print(f"❌ Error: {e}")
        print()
        print("Please set the following environment variables:")
        print("  LOCAL_DATABASE_URL - Your local database URL")
        print("  CURRENT_DATABASE_URL - The current/remote database URL to update")
        print()
        print("Example:")
        print("  export LOCAL_DATABASE_URL='postgresql://user:pass@localhost:5432/local_db'")
        print("  export CURRENT_DATABASE_URL='postgresql://user:pass@remote:5432/remote_db'")
        sys.exit(1)
    
    # Get options
    tables = os.getenv("SYNC_TABLES")
    if tables:
        tables = [t.strip() for t in tables.split(",")]
    else:
        tables = None
    
    clear = os.getenv("CLEAR_EXISTING", "false").lower() == "true"
    
    # Confirm
    print("⚠️  WARNING: This will sync data from your local database to the current database.")
    if clear:
        print("⚠️  EXISTING DATA WILL BE CLEARED before importing!")
    print()
    print(f"Local DB:  {local_db_url}")
    print(f"Current DB: {current_db_url}")
    print()
    
    response = input("Continue? (yes/no): ").strip().lower()
    if response not in ["yes", "y"]:
        print("Cancelled.")
        sys.exit(0)
    
    print()
    
    # Run sync
    success = sync_databases(
        local_db_url=local_db_url,
        current_db_url=current_db_url,
        tables=tables,
        clear_existing=clear
    )
    
    if success:
        print("🎉 Database sync completed successfully!")
        sys.exit(0)
    else:
        print("⚠️  Database sync completed with errors.")
        sys.exit(1)


if __name__ == "__main__":
    main()






