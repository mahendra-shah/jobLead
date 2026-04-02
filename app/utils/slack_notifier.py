"""Slack notification utility for critical alerts."""

import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, List
from collections import deque
import structlog
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.config import settings
from app.utils.job_board_report import read_job_board_report
from app.utils.timezone import IST, ist_today_utc_window

logger = structlog.get_logger(__name__)


class SlackNotifier:
    """Send critical alerts to Slack with rate limiting."""

    def __init__(self):
        """Initialize Slack notifier with rate limiting."""
        self.enabled = (
            settings.SLACK_ALERTS_ENABLED 
            and bool(settings.SLACK_BOT_TOKEN) 
            and bool(settings.SLACK_CHANNEL_ID)
        )
        self.channel_id = settings.SLACK_CHANNEL_ID
        self.max_alerts_per_hour = settings.MAX_ALERTS_PER_HOUR
        self.alert_history: deque = deque(maxlen=100)  # Track recent alerts
        
        # Initialize Slack Web API client
        if self.enabled:
            self.client = WebClient(token=settings.SLACK_BOT_TOKEN)
            logger.info(
                "slack_notifier_initialized", 
                max_alerts_per_hour=self.max_alerts_per_hour,
                channel_id=self.channel_id
            )
        else:
            self.client = None
            logger.info("slack_notifier_disabled")

    def _check_rate_limit(self) -> bool:
        """
        Check if we've exceeded alert rate limit.
        
        Returns:
            True if within limit, False if exceeded
        """
        if not self.enabled:
            return False
            
        # Count alerts sent in last hour
        one_hour_ago = time.time() - 3600
        recent_alerts = sum(1 for ts in self.alert_history if ts > one_hour_ago)
        
        if recent_alerts >= self.max_alerts_per_hour:
            logger.warning(
                "slack_rate_limit_exceeded",
                recent_alerts=recent_alerts,
                max_allowed=self.max_alerts_per_hour,
            )
            return False
        
        return True

    @staticmethod
    def _fmt_report_time(value: object) -> str:
        """Format report timestamp value for Slack display."""
        if not value:
            return "-"
        text = str(value)
        try:
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            return str(value)

    def _get_job_board_report(self) -> Dict[str, object]:
        """Read the latest JobBoard run report from Redis."""
        report = read_job_board_report(redis_url=settings.REDIS_URL)
        if isinstance(report, dict):
            return report
        return {
            "status": "unavailable",
            "note": "Invalid JobBoard report payload.",
        }

    async def send_alert(
        self,
        title: str,
        message: str,
        level: str = "error",
        context: Optional[Dict] = None,
        link: Optional[str] = None,
        notify_channel: bool = False,
    ) -> bool:
        """
        Send alert to Slack with formatted blocks.
        
        Args:
            title: Alert title
            message: Alert message
            level: Severity level (info, warning, error, critical)
            context: Additional context dictionary
            link: Optional link to logs/dashboard
            notify_channel: If True, adds @here to notify all active users (for critical alerts)
            
        Returns:
            True if sent successfully
        """
        if not self.enabled:
            logger.debug("slack_alert_skipped_disabled", title=title)
            return False
            
        if not self._check_rate_limit():
            logger.warning("slack_alert_skipped_rate_limit", title=title)
            return False

        # Map severity to emoji and color
        severity_map = {
            "info": {"emoji": "ℹ️", "color": "#36a64f", "priority": "Low"},
            "warning": {"emoji": "⚠️", "color": "#ff9900", "priority": "Medium"},
            "error": {"emoji": "❌", "color": "#ff0000", "priority": "High"},
            "critical": {"emoji": "🚨", "color": "#ff0000", "priority": "CRITICAL"},
        }
        
        severity = severity_map.get(level.lower(), severity_map["error"])
        
        # Auto-notify channel for critical alerts
        if level.lower() == "critical":
            notify_channel = True
        
        # Build Slack message blocks with improved formatting
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{severity['emoji']} {title}",
                    "emoji": True
                }
            },
        ]
        
        # Add @here mention for critical alerts
        if notify_channel:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "<!here> *Immediate attention required*"
                }
            })
        
        blocks.append({"type": "divider"})
        
        # Main message with better formatting
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": message,
            },
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Priority:*\n{severity['priority']}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Environment:*\n{settings.ENVIRONMENT}"
                }
            ]
        })
        
        # Add context as formatted fields if provided
        if context:
            blocks.append({"type": "divider"})
            
            # Split context into pairs for better layout
            context_items = list(context.items())
            for i in range(0, len(context_items), 2):
                fields = []
                for key, value in context_items[i:i+2]:
                    fields.append({
                        "type": "mrkdwn",
                        "text": f"*{key}:*\n{value}"
                    })
                blocks.append({
                    "type": "section",
                    "fields": fields
                })
        
        # Add timestamp
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"🕒 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
                }
            ]
        })
        
        # Add link if provided
        if link:
            blocks.append({
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "View Details"
                        },
                        "url": link,
                    }
                ]
            })

        try:
            # Send message using Slack Web API
            response = self.client.chat_postMessage(
                channel=self.channel_id,
                blocks=blocks,
                attachments=[
                    {
                        "color": severity["color"],
                        "fallback": f"{title}: {message}",
                    }
                ],
                text=f"{title}: {message}"  # Fallback text for notifications
            )
            
            # Track alert for rate limiting
            self.alert_history.append(time.time())
            
            logger.info(
                "slack_alert_sent",
                title=title,
                level=level,
                message_ts=response.get("ts"),
            )
            return True
            
        except SlackApiError as e:
            logger.error(
                "slack_alert_failed_api",
                title=title,
                error=e.response["error"],
                status_code=e.response.status_code,
            )
            return False
        except Exception as e:
            logger.error(
                "slack_alert_failed",
                title=title,
                error=str(e),
            )
            return False

    async def send_scraper_failure_alert(
        self,
        reason: str,
        details: Optional[Dict] = None,
    ) -> bool:
        """
        Send alert for scraper failures.
        
        Args:
            reason: Failure reason
            details: Additional details
            
        Returns:
            True if sent successfully
        """
        return await self.send_alert(
            title="Telegram Scraper Failure",
            message=f"The Telegram scraper has encountered a critical failure:\n\n*Reason:* {reason}",
            level="critical",
            context=details,
        )

    async def send_account_health_alert(
        self,
        active_accounts: int,
        degraded_accounts: int,
        banned_accounts: int,
        details: Optional[List[Dict]] = None,
        force_send: bool = False,
    ) -> bool:
        """
        Send alert when account health is degraded.
        
        Only auto-sends for critical situations (0-2 healthy accounts).
        For 3+ healthy accounts, alert is skipped unless force_send=True.
        
        Args:
            active_accounts: Number of healthy accounts
            degraded_accounts: Number of degraded accounts
            banned_accounts: Number of banned accounts
            details: List of account details
            force_send: Force sending even for non-critical levels
            
        Returns:
            True if sent successfully
        """
        if active_accounts == 0:
            level = "critical"
            message = "🚨 *ALL ACCOUNTS DOWN*\n\nTelegram scraping has stopped completely! All session files need immediate attention."
        elif active_accounts <= 2:
            level = "critical"
            message = f"🚨 *CRITICAL: Only {active_accounts}/5 accounts operational*\n\nScraping capacity severely degraded. Immediate action required."
        else:
            # For 3+ accounts, only send if forced (via slash command)
            if not force_send:
                logger.info(
                    "account_health_alert_skipped",
                    active_accounts=active_accounts,
                    reason="non_critical_level",
                )
                return False
            level = "warning"
            message = f"⚠️ Some accounts need attention ({active_accounts}/5 healthy)"

        context = {
            "Healthy Accounts": f"{active_accounts}/5",
            "Degraded": str(degraded_accounts),
            "Banned": str(banned_accounts),
            "Status": "🔴 Critical" if active_accounts <= 2 else "🟡 Warning",
        }
        
        if details and len(details) <= 5:
            account_items = []
            for d in details:
                error_info = f"- {d.get('error', '')}" if d.get('error') else '✓'
                account_items.append(f"• Account {d.get('id')}: {d.get('status')} {error_info}")
            account_list = "\n".join(account_items)
            message += f"\n\n*Account Details:*\n{account_list}"

        return await self.send_alert(
            title="Account Health Alert",
            message=message,
            level=level,
            context=context,
        )

    async def send_zero_messages_alert(
        self,
        last_successful_fetch: Optional[datetime] = None,
    ) -> bool:
        """
        Send alert when no messages fetched in last scrape cycle.
        
        Args:
            last_successful_fetch: Last time messages were successfully fetched
            
        Returns:
            True if sent successfully
        """
        message = "The Telegram scraper completed but fetched ZERO messages."
        
        context = {}
        if last_successful_fetch:
            hours_ago = (datetime.now(timezone.utc) - last_successful_fetch).total_seconds() / 3600
            context["Last Successful Fetch"] = f"{hours_ago:.1f} hours ago"
            context["Timestamp"] = last_successful_fetch.strftime('%Y-%m-%d %H:%M:%S UTC')

        return await self.send_alert(
            title="Zero Messages Fetched",
            message=message,
            level="error",
            context=context,
        )

    async def send_session_error_alert(
        self,
        account_id: int,
        error_message: str,
    ) -> bool:
        """
        Send alert when session validation fails.
        
        Args:
            account_id: Account with session error
            error_message: Error details
            
        Returns:
            True if sent successfully
        """
        message = (
            f"Session file validation failed for Account {account_id}.\n\n"
            f"*Error:* {error_message}\n\n"
            f"*Action Required:* Regenerate session file using `python generate_telegram_session.py`"
        )

        return await self.send_alert(
            title=f"Session Error - Account {account_id}",
            message=message,
            level="error",
            context={"Account ID": str(account_id)},
        )

    async def send_database_error_alert(
        self,
        database: str,
        error: str,
    ) -> bool:
        """
        Send alert when database connection fails.
        
        Args:
            database: Database name (MongoDB, PostgreSQL)
            error: Error message
            
        Returns:
            True if sent successfully
        """
        message = (
            f"Failed to connect to {database}.\n\n"
            f"*Error:* {error}\n\n"
            f"Scraping operations may be failing or data may not be persisted."
        )

        return await self.send_alert(
            title=f"{database} Connection Error",
            message=message,
            level="critical",
            context={"Database": database},
        )

    async def send_morning_update(self, db: AsyncSession) -> bool:
        """
        Send daily morning update with system health and yesterday's statistics.
        
        Args:
            db: Database session
            
        Returns:
            True if sent successfully
        """
        try:
            # Import here to avoid circular imports
            from app.models.telegram_account import TelegramAccount, HealthStatus
            from app.models.telegram_group import TelegramGroup
            from app.models.job import Job
            from app.services.telegram_scraper_service import get_scraper_service
            
            # Build IST-based "yesterday" window and query with UTC bounds.
            # PostgreSQL columns are TIMESTAMP WITHOUT TIME ZONE (naive UTC).
            yesterday_ref = datetime.now(IST) - timedelta(days=1)
            yesterday, today, yesterday_ist_str = ist_today_utc_window(yesterday_ref)

            # Mongo fetched_at uses timezone-aware UTC datetimes.
            yesterday_utc = yesterday.replace(tzinfo=timezone.utc)
            today_utc = today.replace(tzinfo=timezone.utc)
            
            # Account health
            result = await db.execute(select(TelegramAccount))
            accounts = result.scalars().all()
            
            healthy = sum(1 for a in accounts if a.health_status == HealthStatus.HEALTHY)
            degraded = sum(1 for a in accounts if a.health_status == HealthStatus.DEGRADED)
            banned = sum(1 for a in accounts if a.health_status == HealthStatus.BANNED)
            
            # Yesterday's jobs
            result = await db.execute(
                select(func.count())
                .select_from(Job)
                .where(Job.created_at >= yesterday, Job.created_at < today)
            )
            jobs_yesterday = result.scalar() or 0
            
            # Active jobs total
            result = await db.execute(
                select(func.count())
                .select_from(Job)
                .where(Job.is_active == True)
            )
            active_jobs = result.scalar() or 0
            
            # Channels scraped yesterday
            result = await db.execute(
                select(func.count())
                .select_from(TelegramGroup)
                .where(
                    TelegramGroup.last_scraped_at >= yesterday,
                    TelegramGroup.last_scraped_at < today
                )
            )
            channels_yesterday = result.scalar() or 0
            channels_source = "postgres"
            
            # Total active channels
            result = await db.execute(
                select(func.count())
                .select_from(TelegramGroup)
                .where(TelegramGroup.is_active == True)
            )
            total_channels = result.scalar() or 0
            
            # Yesterday's messages from MongoDB
            messages_yesterday = 0
            mongo_db = None
            try:
                scraper = get_scraper_service()
                if not scraper._initialized:
                    await scraper.initialize()
                mongo_db = scraper.mongo_client[settings.MONGODB_DATABASE]
                messages_yesterday = mongo_db.raw_messages.count_documents({
                    "fetched_at": {"$gte": yesterday_utc, "$lt": today_utc}
                })
            except Exception as e:
                logger.warning("failed_to_get_mongodb_stats", error=str(e))

            # Fallback only for false-zero cases from Postgres channel timestamps.
            if channels_yesterday == 0 and mongo_db is not None:
                try:
                    mongo_filter = {
                        "fetched_at": {"$gte": yesterday_utc, "$lt": today_utc}
                    }
                    channel_usernames = set(
                        mongo_db.raw_messages.distinct(
                            "channel_username",
                            mongo_filter,
                        )
                    )
                    legacy_usernames = set(
                        mongo_db.raw_messages.distinct(
                            "username",
                            mongo_filter,
                        )
                    )
                    normalized_channels = {
                        str(value).strip().lower()
                        for value in (channel_usernames | legacy_usernames)
                        if value
                    }
                    mongo_channels = len(normalized_channels)
                    if mongo_channels > 0:
                        channels_yesterday = mongo_channels
                        channels_source = "mongo_fallback"
                except Exception as e:
                    logger.warning(
                        "failed_to_get_mongodb_channel_fallback",
                        error=str(e),
                    )

            logger.info(
                "morning_update_channels_count",
                source=channels_source,
                channels_yesterday=channels_yesterday,
                total_channels=total_channels,
                window_start_utc=yesterday_utc.isoformat(),
                window_end_utc=today_utc.isoformat(),
            )

            job_board_report = self._get_job_board_report()
            jb_status = str(job_board_report.get("status") or "unknown")
            jb_started_at = self._fmt_report_time(job_board_report.get("started_at"))
            jb_ended_at = self._fmt_report_time(job_board_report.get("ended_at"))
            jb_duration = int(job_board_report.get("duration_seconds") or 0)
            jb_error = str(job_board_report.get("last_error") or "")

            source_counts = job_board_report.get("source_counts") or {}
            source_attempted = int(source_counts.get("attempted") or 0)
            source_succeeded = int(source_counts.get("succeeded") or 0)
            source_failed = int(source_counts.get("failed") or 0)

            ml_counts = job_board_report.get("ml_counts") or {}
            ml_pending = int(ml_counts.get("pending") or 0)
            ml_processing = int(ml_counts.get("processing") or 0)
            ml_verified = int(ml_counts.get("verified") or 0)
            ml_rejected = int(ml_counts.get("rejected") or 0)

            postgres_sync_count = int(job_board_report.get("postgres_sync_count") or 0)
            sheets_export_count = int(job_board_report.get("sheets_export_count") or 0)
            
            # Determine system status
            if healthy == 0:
                status_emoji = "🔴"
                status_text = "CRITICAL"
            elif healthy <= 2:
                status_emoji = "🟡"
                status_text = "DEGRADED"
            else:
                status_emoji = "🟢"
                status_text = "HEALTHY"
            
            # Build issues list
            issues = []
            if healthy == 0:
                issues.append("🚨 All Telegram accounts are down")
            elif healthy <= 2:
                issues.append(f"⚠️ Only {healthy}/{len(accounts)} accounts healthy - reduced capacity")
            if degraded > 0:
                issues.append(f"⚠️ {degraded} account(s) degraded")
            if banned > 0:
                issues.append(f"🔴 {banned} account(s) banned")
            if jobs_yesterday == 0:
                issues.append("❌ No jobs created yesterday")
            if messages_yesterday == 0:
                issues.append("❌ No messages scraped yesterday")
            if jb_status in {"failed", "start_failed"}:
                issues.append("❌ JobBoard daily ingest failed")
            
            issues_text = "\n".join(f"• {issue}" for issue in issues) if issues else "✅ No issues detected"
            
            # Format message
            yesterday_date = datetime.strptime(
                yesterday_ist_str,
                "%Y-%m-%d",
            ).strftime("%B %d, %Y")
            
            message = (
                f"*System Status:* {status_emoji} {status_text}\n"
                f"*Environment:* {settings.ENVIRONMENT}\n\n"
                f"*📊 Yesterday's Performance ({yesterday_date}):*\n"
                f"• Jobs Created: {jobs_yesterday}\n"
                f"• Messages Scraped: {messages_yesterday:,}\n"
                f"• Channels Scraped: {channels_yesterday}/{total_channels}\n\n"
                f"*🧭 JobBoard Daily Ingest (latest run):*\n"
                f"• Status: {jb_status}\n"
                f"• Started: {jb_started_at}\n"
                f"• Ended: {jb_ended_at}\n"
                f"• Duration: {jb_duration}s\n"
                f"• Sources: attempted={source_attempted}, "
                f"succeeded={source_succeeded}, failed={source_failed}\n"
                f"• ML: pending={ml_pending}, processing={ml_processing}, "
                f"verified={ml_verified}, rejected={ml_rejected}\n"
                f"• Postgres synced: {postgres_sync_count}\n"
                f"• Sheets exported: {sheets_export_count}\n"
                f"• Last error: {jb_error or '-'}\n\n"
                f"*📡 Current Account Health:*\n"
                f"• Healthy: {healthy}/{len(accounts)}\n"
                f"• Degraded: {degraded}\n"
                f"• Banned: {banned}\n\n"
                f"*💼 Active Jobs:* {active_jobs:,}\n\n"
                f"*⚠️ Issues to Address:*\n{issues_text}\n\n"
                f"_Use slash commands for detailed queries: /pd-help_"
            )
            
            # Send with info level (not critical, but automatic)
            # Add @here only if there are critical issues
            notify_channel = healthy <= 2 or not issues
            
            return await self.send_alert(
                title="☀️ Good Morning - Daily System Update",
                message=message,
                level="info",
                notify_channel=notify_channel,
                context={
                    "Jobs Yesterday": jobs_yesterday,
                    "Messages Yesterday": messages_yesterday,
                    "Channels Source": channels_source,
                    "Healthy Accounts": f"{healthy}/{len(accounts)}",
                    "JobBoard Status": jb_status,
                    "JobBoard Verified": ml_verified,
                },
            )
            
        except Exception as e:
            logger.error("failed_to_send_morning_update", error=str(e), exc_info=True)
            return False


# Global instance
slack_notifier = SlackNotifier()
