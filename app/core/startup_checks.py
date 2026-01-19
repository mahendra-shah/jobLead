"""Startup validation checks for the application."""

import logging as log_module
import os
from pathlib import Path

logger = log_module.getLogger(__name__)


def check_ml_system() -> bool:
    """
    Check if ML system is properly configured and ready.
    
    Returns:
        bool: True if ML system is ready, False otherwise
    """
    try:
        logger.info("Checking ML system...")
        
        # Check if model file exists
        model_path = Path("app/ml/training/models/job_classifier.pkl")
        if not model_path.exists():
            logger.error(f"ML model file not found: {model_path}")
            logger.error("Please run: python app/ml/training/train_sklearn.py")
            return False
        
        # Check if model metadata exists
        metadata_path = Path("app/ml/training/models/model_metadata.json")
        if not metadata_path.exists():
            logger.warning(f"ML model metadata not found: {metadata_path}")
        
        # Try to load the ML classifier
        from app.ml.ensemble_classifier import job_classifier
        
        # Get model info
        model_info = job_classifier.get_model_info()
        
        # Check if all components are loaded
        if not model_info.get("ensemble", {}).get("is_loaded"):
            logger.error("ML ensemble classifier not loaded")
            return False
        
        if not model_info.get("sklearn", {}).get("is_loaded"):
            logger.error("Scikit-learn classifier not loaded")
            return False
        
        if not model_info.get("spacy", {}).get("is_loaded"):
            logger.warning("spaCy not loaded (optional, will use basic extraction)")
        
        # Log model version
        model_version = model_info.get("ensemble", {}).get("model_version", "unknown")
        last_trained = model_info.get("ensemble", {}).get("last_trained", "unknown")
        
        logger.info(f"✅ ML system ready!")
        logger.info(f"   Model version: {model_version}")
        logger.info(f"   Last trained: {last_trained}")
        logger.info(f"   Sklearn: {'✓' if model_info.get('sklearn', {}).get('is_loaded') else '✗'}")
        logger.info(f"   spaCy: {'✓' if model_info.get('spacy', {}).get('is_loaded') else '✗'}")
        
        return True
        
    except ImportError as e:
        logger.error(f"ML dependencies not installed: {e}")
        logger.error("Please install: pip install scikit-learn spacy")
        logger.error("And download model: python -m spacy download en_core_web_sm")
        return False
        
    except Exception as e:
        logger.error(f"Error checking ML system: {e}", exc_info=True)
        return False


def check_database() -> bool:
    """
    Check if database connection is working.
    
    Returns:
        bool: True if database is ready, False otherwise
    """
    try:
        logger.info("Checking database connection...")
        
        # TODO: Implement database connection check
        # from app.db.session import get_db
        # Check if can connect and query
        
        logger.info("✅ Database check passed (placeholder)")
        return True
        
    except Exception as e:
        logger.error(f"Database check failed: {e}", exc_info=True)
        return False


def check_storage() -> bool:
    """
    Check if storage service is configured properly.
    
    Returns:
        bool: True if storage is ready, False otherwise
    """
    try:
        logger.info("Checking storage configuration...")
        
        # Check if storage directory exists for local storage
        storage_type = os.getenv("STORAGE_TYPE", "local")
        
        if storage_type == "local":
            data_dir = Path("app/data")
            if not data_dir.exists():
                logger.warning(f"Local storage directory not found: {data_dir}")
                data_dir.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created local storage directory: {data_dir}")
        
        logger.info(f"✅ Storage configured: {storage_type}")
        return True
        
    except Exception as e:
        logger.error(f"Storage check failed: {e}", exc_info=True)
        return False


def check_environment() -> bool:
    """
    Check if required environment variables are set.
    
    Returns:
        bool: True if environment is configured, False otherwise
    """
    try:
        logger.info("Checking environment configuration...")
        
        required_vars = []
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
            return False
        
        # Optional variables with warnings
        optional_vars = {
            "OPENAI_API_KEY": "OpenAI API (for optional AI-powered extraction)",
            "GOOGLE_API_KEY": "Google Gemini API (for optional AI-powered extraction)",
        }
        
        for var, description in optional_vars.items():
            if not os.getenv(var):
                logger.warning(f"Optional variable not set: {var} ({description})")
        
        logger.info("✅ Environment configuration checked")
        return True
        
    except Exception as e:
        logger.error(f"Environment check failed: {e}", exc_info=True)
        return False


def run_all_startup_checks() -> bool:
    """
    Run all startup validation checks.
    
    Returns:
        bool: True if all checks pass, False otherwise
    """
    logger.info("=" * 60)
    logger.info("Running startup validation checks...")
    logger.info("=" * 60)
    
    checks = [
        ("Environment", check_environment),
        ("ML System", check_ml_system),
        ("Database", check_database),
        ("Storage", check_storage),
    ]
    
    results = {}
    all_passed = True
    
    for name, check_func in checks:
        try:
            passed = check_func()
            results[name] = passed
            if not passed:
                all_passed = False
        except Exception as e:
            logger.error(f"Check '{name}' failed with exception: {e}")
            results[name] = False
            all_passed = False
    
    logger.info("=" * 60)
    logger.info("Startup checks complete:")
    for name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        logger.info(f"  {name}: {status}")
    logger.info("=" * 60)
    
    if not all_passed:
        logger.error("Some startup checks failed. Please fix issues before proceeding.")
    else:
        logger.info("🎉 All startup checks passed! System ready.")
    
    return all_passed


if __name__ == "__main__":
    # Run checks when script is executed directly
    import sys
    
    log_module.basicConfig(
        level=log_module.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    success = run_all_startup_checks()
    sys.exit(0 if success else 1)
