"""Slack notification utility for critical alerts."""

import time
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from collections import deque
import structlog
import httpx

from app.config import settings

logger = structlog.get_logger(__name__)


class SlackNotifier:
    """Send critical alerts to Slack with rate limiting."""

    def __init__(self):
        """Initialize Slack notifier with rate limiting."""
        self.enabled = settings.SLACK_ALERTS_ENABLED and bool(settings.SLACK_WEBHOOK_URL)
        self.webhook_url = settings.SLACK_WEBHOOK_URL
        self.max_alerts_per_hour = settings.MAX_ALERTS_PER_HOUR
        self.alert_history: deque = deque(maxlen=100)  # Track recent alerts
        
        if self.enabled:
            logger.info("slack_notifier_initialized", max_alerts_per_hour=self.max_alerts_per_hour)
        else:
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

    async def send_alert(
        self,
        title: str,
        message: str,
        level: str = "error",
        context: Optional[Dict] = None,
        link: Optional[str] = None,
    ) -> bool:
        """
        Send alert to Slack with formatted blocks.
        
        Args:
            title: Alert title
            message: Alert message
            level: Severity level (info, warning, error, critical)
            context: Additional context dictionary
            link: Optional link to logs/dashboard
            
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
            "info": {"emoji": "ℹ️", "color": "#36a64f"},
            "warning": {"emoji": "⚠️", "color": "#ff9900"},
            "error": {"emoji": "❌", "color": "#ff0000"},
            "critical": {"emoji": "🚨", "color": "#ff0000"},
        }
        
        severity = severity_map.get(level.lower(), severity_map["error"])
        
        # Build Slack message blocks
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{severity['emoji']} {title}",
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": message,
                }
            },
        ]
        
        # Add context if provided
        if context:
            context_text = "\n".join([f"*{k}:* {v}" for k, v in context.items()])
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": context_text,
                }
            })
        
        # Add timestamp
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"🕒 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')} | Environment: *{settings.ENVIRONMENT}*"
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

        payload = {
            "blocks": blocks,
            "attachments": [
                {
                    "color": severity["color"],
                    "fallback": f"{title}: {message}",
                }
            ]
        }

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    self.webhook_url,
                    json=payload,
                )
                response.raise_for_status()
                
            # Track alert for rate limiting
            self.alert_history.append(time.time())
            
            logger.info(
                "slack_alert_sent",
                title=title,
                level=level,
            )
            return True
            
        except httpx.HTTPError as e:
            logger.error(
                "slack_alert_failed_http",
                title=title,
                error=str(e),
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
    ) -> bool:
        """
        Send alert when account health is degraded.
        
        Args:
            active_accounts: Number of healthy accounts
            degraded_accounts: Number of degraded accounts
            banned_accounts: Number of banned accounts
            details: List of account details
            
        Returns:
            True if sent successfully
        """
        if active_accounts == 0:
            level = "critical"
            message = "🚨 *ALL ACCOUNTS DOWN* - Telegram scraping has stopped completely!"
        elif active_accounts <= 2:
            level = "error"
            message = f"⚠️ Only {active_accounts} accounts are healthy. Scraping capacity severely degraded."
        else:
            level = "warning"
            message = f"Some Telegram accounts are experiencing issues."

        context = {
            "Active": f"{active_accounts}/5",
            "Degraded": str(degraded_accounts),
            "Banned": str(banned_accounts),
        }
        
        if details:
            account_status = "\n".join([
                f"Account {d.get('id')}: {d.get('status')} - {d.get('error', 'N/A')}"
                for d in details
            ])
            message += f"\n\n*Account Status:*\n```{account_status}```"

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
            hours_ago = (datetime.utcnow() - last_successful_fetch).total_seconds() / 3600
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


# Global instance
slack_notifier = SlackNotifier()
