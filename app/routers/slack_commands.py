"""
Slack Slash Commands Router
===========================
Handles Slack slash commands for on-demand system queries.

Commands:
- /pd-visibility - System health and visibility
- /pd-jobs - Today's job statistics
- /pd-accounts - Account health status
- /pd-scraping - Scraping performance
- /pd-help - Show available commands
"""

from fastapi import APIRouter, Request, Response, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, text
from datetime import datetime, timedelta
from typing import Dict, Any
import structlog
import hashlib
import hmac

from app.api.deps import get_db
from app.models.telegram_account import TelegramAccount, HealthStatus
from app.models.telegram_group import TelegramGroup
from app.models.job import Job
from app.services.telegram_scraper_service import get_scraper_service
from app.config import settings

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/api/slack", tags=["Slack Commands"])


def verify_slack_request(request: Request, body: bytes) -> bool:
    """
    Verify that request came from Slack.
    
    Returns True if valid, False otherwise.
    """
    # For now, skip verification in development
    # In production, you should verify using SLACK_SIGNING_SECRET
    if settings.ENVIRONMENT == "development":
        return True
    
    # TODO: Implement Slack signature verification
    # slack_signature = request.headers.get("X-Slack-Signature")
    # slack_timestamp = request.headers.get("X-Slack-Request-Timestamp")
    # if not slack_signature or not slack_timestamp:
    #     return False
    
    return True


def format_slack_response(text: str, blocks: list = None) -> Dict[str, Any]:
    """Format response for Slack with blocks."""
    response = {
        "response_type": "ephemeral",  # Only visible to user who invoked command
        "text": text,
    }
    if blocks:
        response["blocks"] = blocks
    return response


async def get_visibility_data(db: AsyncSession) -> Dict[str, Any]:
    """Get system visibility data."""
    # Account health
    result = await db.execute(select(TelegramAccount))
    accounts = result.scalars().all()
    
    healthy = sum(1 for a in accounts if a.health_status == HealthStatus.HEALTHY)
    degraded = sum(1 for a in accounts if a.health_status == HealthStatus.DEGRADED)
    banned = sum(1 for a in accounts if a.health_status == HealthStatus.BANNED)
    
    # Channel stats
    result = await db.execute(
        select(func.count()).select_from(TelegramGroup).where(TelegramGroup.is_active == True)
    )
    total_channels = result.scalar()
    
    # Last scrape time
    result = await db.execute(
        select(TelegramGroup.last_scraped_at)
        .where(TelegramGroup.last_scraped_at.isnot(None))
        .order_by(TelegramGroup.last_scraped_at.desc())
        .limit(1)
    )
    last_scrape = result.scalar()
    hours_since = None
    if last_scrape:
        hours_since = (datetime.utcnow() - last_scrape).total_seconds() / 3600
    
    # MongoDB stats
    total_messages = 0
    try:
        scraper = get_scraper_service()
        if scraper._initialized:
            mongo_db = scraper.mongo_client[settings.MONGODB_DATABASE]
            total_messages = mongo_db.raw_messages.count_documents({})
    except Exception:
        pass
    
    return {
        "accounts": {
            "total": len(accounts),
            "healthy": healthy,
            "degraded": degraded,
            "banned": banned,
        },
        "channels": {
            "total": total_channels,
            "last_scrape_hours": hours_since,
        },
        "messages": {
            "total": total_messages,
        },
    }


async def get_jobs_today_data(db: AsyncSession) -> Dict[str, Any]:
    """Get today's job statistics."""
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Jobs created today
    result = await db.execute(
        select(func.count())
        .select_from(Job)
        .where(Job.created_at >= today)
    )
    jobs_today = result.scalar()
    
    # Active jobs
    result = await db.execute(
        select(func.count())
        .select_from(Job)
        .where(Job.is_active == True)
    )
    active_jobs = result.scalar()
    
    # Jobs by type (today)
    result = await db.execute(
        select(Job.job_type, func.count())
        .where(Job.created_at >= today)
        .group_by(Job.job_type)
    )
    jobs_by_type = {row[0]: row[1] for row in result.all()}
    
    return {
        "today": jobs_today,
        "active_total": active_jobs,
        "by_type": jobs_by_type,
    }


async def get_account_health_data(db: AsyncSession) -> list:
    """Get detailed account health."""
    result = await db.execute(select(TelegramAccount))
    accounts = result.scalars().all()
    
    account_list = []
    for acc in accounts:
        account_list.append({
            "phone": acc.phone,
            "status": acc.health_status.value,
            "consecutive_errors": acc.consecutive_errors,
            "last_error": acc.last_error_message,
            "last_used": acc.last_used_at,
        })
    
    return account_list


async def get_scraping_stats_data(db: AsyncSession) -> Dict[str, Any]:
    """Get scraping statistics."""
    yesterday = datetime.utcnow() - timedelta(days=1)
    
    # Channels scraped in last 24h
    result = await db.execute(
        select(func.count())
        .select_from(TelegramGroup)
        .where(TelegramGroup.last_scraped_at >= yesterday)
    )
    channels_24h = result.scalar()
    
    # Messages and jobs today
    try:
        scraper = get_scraper_service()
        messages_today = 0
        if scraper._initialized:
            mongo_db = scraper.mongo_client[settings.MONGODB_DATABASE]
            today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            messages_today = mongo_db.raw_messages.count_documents({
                "fetched_at": {"$gte": today_start}
            })
    except Exception:
        messages_today = 0
    
    return {
        "channels_24h": channels_24h,
        "messages_today": messages_today,
    }


@router.post("/commands/visibility")
async def slack_command_visibility(
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """Handle /pd-visibility command."""
    body = await request.body()
    if not verify_slack_request(request, body):
        raise HTTPException(status_code=401, detail="Invalid request signature")
    
    try:
        data = await get_visibility_data(db)
        
        # Format response
        accounts = data["accounts"]
        channels = data["channels"]
        messages = data["messages"]
        
        # Determine status emoji
        if accounts["healthy"] == 0:
            status_emoji = "🔴"
            status_text = "CRITICAL"
        elif accounts["healthy"] <= 2:
            status_emoji = "🟡"
            status_text = "DEGRADED"
        else:
            status_emoji = "🟢"
            status_text = "HEALTHY"
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{status_emoji} System Visibility",
                    "emoji": True
                }
            },
            {"type": "divider"},
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Status:*\n{status_text}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Environment:*\n{settings.ENVIRONMENT}"
                    }
                ]
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*📡 Telegram Accounts*"
                },
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Healthy:*\n{accounts['healthy']}/{accounts['total']}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Degraded:*\n{accounts['degraded']}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Banned:*\n{accounts['banned']}"
                    }
                ]
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*📊 Scraping Performance*"
                },
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Active Channels:*\n{channels['total']}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Total Messages:*\n{messages['total']:,}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Last Scrape:*\n{f'{channels[\"last_scrape_hours\"]:.1f}h ago' if channels['last_scrape_hours'] else 'Never'}"
                    }
                ]
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"🕒 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
                    }
                ]
            }
        ]
        
        return format_slack_response(
            text=f"{status_emoji} System Status: {status_text}",
            blocks=blocks
        )
        
    except Exception as e:
        logger.error("slack_command_visibility_failed", error=str(e), exc_info=True)
        return format_slack_response(
            text=f"❌ Error fetching visibility data: {str(e)}"
        )


@router.post("/commands/jobs")
async def slack_command_jobs(
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """Handle /pd-jobs command."""
    body = await request.body()
    if not verify_slack_request(request, body):
        raise HTTPException(status_code=401, detail="Invalid request signature")
    
    try:
        data = await get_jobs_today_data(db)
        
        # Format job types
        job_types_text = "\n".join([
            f"• {job_type or 'Unknown'}: {count}"
            for job_type, count in data["by_type"].items()
        ]) if data["by_type"] else "No jobs today"
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "💼 Job Statistics",
                    "emoji": True
                }
            },
            {"type": "divider"},
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Jobs Today:*\n{data['today']}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Total Active:*\n{data['active_total']}"
                    }
                ]
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Jobs by Type (Today):*\n{job_types_text}"
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"🕒 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
                    }
                ]
            }
        ]
        
        return format_slack_response(
            text=f"💼 Jobs today: {data['today']}",
            blocks=blocks
        )
        
    except Exception as e:
        logger.error("slack_command_jobs_failed", error=str(e), exc_info=True)
        return format_slack_response(
            text=f"❌ Error fetching job data: {str(e)}"
        )


@router.post("/commands/accounts")
async def slack_command_accounts(
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """Handle /pd-accounts command."""
    body = await request.body()
    if not verify_slack_request(request, body):
        raise HTTPException(status_code=401, detail="Invalid request signature")
    
    try:
        accounts = await get_account_health_data(db)
        
        # Format account list
        account_fields = []
        for acc in accounts:
            status_emoji = {
                "healthy": "🟢",
                "degraded": "🟡",
                "banned": "🔴"
            }.get(acc["status"], "⚪")
            
            account_fields.append({
                "type": "mrkdwn",
                "text": f"*{acc['phone']}*\n{status_emoji} {acc['status'].upper()}\nErrors: {acc['consecutive_errors']}"
            })
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "📱 Account Health",
                    "emoji": True
                }
            },
            {"type": "divider"},
            {
                "type": "section",
                "fields": account_fields
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"🕒 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
                    }
                ]
            }
        ]
        
        return format_slack_response(
            text=f"📱 Account Health for {len(accounts)} accounts",
            blocks=blocks
        )
        
    except Exception as e:
        logger.error("slack_command_accounts_failed", error=str(e), exc_info=True)
        return format_slack_response(
            text=f"❌ Error fetching account data: {str(e)}"
        )


@router.post("/commands/scraping")
async def slack_command_scraping(
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """Handle /pd-scraping command."""
    body = await request.body()
    if not verify_slack_request(request, body):
        raise HTTPException(status_code=401, detail="Invalid request signature")
    
    try:
        data = await get_scraping_stats_data(db)
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "📊 Scraping Stats",
                    "emoji": True
                }
            },
            {"type": "divider"},
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Channels (24h):*\n{data['channels_24h']}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Messages (Today):*\n{data['messages_today']}"
                    }
                ]
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"🕒 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
                    }
                ]
            }
        ]
        
        return format_slack_response(
            text=f"📊 Scraping Stats: {data['messages_today']} messages today",
            blocks=blocks
        )
        
    except Exception as e:
        logger.error("slack_command_scraping_failed", error=str(e), exc_info=True)
        return format_slack_response(
            text=f"❌ Error fetching scraping data: {str(e)}"
        )


@router.post("/commands/help")
async def slack_command_help(request: Request):
    """Handle /pd-help command."""
    body = await request.body()
    if not verify_slack_request(request, body):
        raise HTTPException(status_code=401, detail="Invalid request signature")
    
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "💡 Available Commands",
                "emoji": True
            }
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*Available Slash Commands:*\n\n"
                    "`/pd-visibility` - System health & visibility\n"
                    "`/pd-jobs` - Today's job statistics\n"
                    "`/pd-accounts` - Account health status\n"
                    "`/pd-scraping` - Scraping performance\n"
                    "`/pd-help` - Show this help message\n\n"
                    "*Auto-Alerts:*\n"
                    "You'll receive automatic alerts only for:\n"
                    "• 🚨 All accounts down (0 healthy)\n"
                    "• 🚨 Critical capacity (1-2 healthy accounts)\n"
                    "• ❌ Zero messages scraped\n"
                    "• 💥 Database failures\n\n"
                    "*Note:* All commands show data visible only to you (ephemeral messages)"
                )
            }
        }
    ]
    
    return format_slack_response(
        text="💡 Placement Dashboard Commands",
        blocks=blocks
    )
