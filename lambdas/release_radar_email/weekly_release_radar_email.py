"""
XOMIFY Release Radar Email Sender
=================================
Sends weekly release radar emails to enrolled users.

Runs every Friday at 8:15 AM Eastern (15 min after playlist cron).
"""

from lambdas.common.logger import get_logger
from lambdas.common.wrapped_helper import get_active_release_radar_users
from lambdas.common.release_radar_dynamo import (
    get_week_key,
    get_release_radar_week
)
from lambdas.common.ses_helper import send_release_radar_email
from lambdas.common.constants import XOMIFY_URL

log = get_logger(__file__)

async def release_radar_email_cron_job(event) -> dict:
    """
    Main entry point for sending release radar emails.
    
    Returns:
        Dict with success/failure counts
    """
    log.info("=" * 50)
    log.info("📧 Starting Release Radar Email Sender")
    log.info("=" * 50)
    
    # Get current week key
    week_key = get_week_key()
    log.info(f"Sending emails for week: {week_key}")
    
    # Get active users
    users = get_active_release_radar_users()
    log.info(f"Found {len(users)} enrolled users")
    
    if not users:
        log.info("No users to email")
        return {"sent": 0, "failed": 0, "skipped": 0}
    
    sent = 0
    failed = 0
    skipped = 0
    
    for user in users:
        email = user.get('email')
        user_name = user.get('displayName') or user.get('userId', 'there')
        
        try:
            # Get this week's release radar data
            week_data = get_release_radar_week(email, week_key)
            
            if not week_data:
                log.warning(f"[{email}] No release radar data for {week_key}, skipping")
                skipped += 1
                continue
            
            releases = week_data.get('releases', [])
            stats = week_data.get('stats', {})
            playlist_id = user.get('releasRadarId')
            
            # Skip if no releases
            if not releases or stats.get('totalTracks', 0) == 0:
                log.info(f"[{email}] No releases this week, skipping email")
                skipped += 1
                continue
            
            # Build playlist URL
            playlist_url = f"https://open.spotify.com/playlist/{playlist_id}" if playlist_id else XOMIFY_URL
            
            # Send email
            success = send_release_radar_email(
                to_email=email,
                user_name=user_name,
                week_key=week_key,
                stats=stats,
                releases=releases,
                playlist_url=playlist_url
            )
            
            if success:
                log.info(f"[{email}] ✅ Email sent")
                sent += 1
            else:
                log.error(f"[{email}] ❌ Email failed")
                failed += 1
                
        except Exception as err:
            log.error(f"[{email}] ❌ Error: {err}")
            failed += 1
    
    log.info("=" * 50)
    log.info(f"📧 Release Radar Email Sender Complete!")
    log.info(f"   ✅ Sent: {sent}")
    log.info(f"   ❌ Failed: {failed}")
    log.info(f"   ⏭️ Skipped: {skipped}")
    log.info("=" * 50)
    
    return sent, failed, skipped

