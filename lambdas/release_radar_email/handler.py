"""
XOMIFY Release Radar Email Handler
============================
Monthly cron job to send release radar preview emails.
"""

import asyncio

from lambdas.common.logger import get_logger
from lambdas.common.errors import ReleaseRadarEmailError, handle_errors
from lambdas.common.utility_helpers import success_response, is_cron_event
from weekly_release_radar_email import release_radar_email_cron_job

log = get_logger(__file__)

HANDLER = 'release-radar-email'


@handle_errors(HANDLER)
def handler(event, context):
    """
    Main Lambda handler for release radar emails.
    
    Triggered weekly by CloudWatch Events to send
    release radar preview emails to all enrolled users.
    """
    
    # Only allow cron job invocation
    if not is_cron_event(event):
        raise ReleaseRadarEmailError(
            message="Invalid call - must be triggered by cron job",
            function="handler"
        )
    
    log.info("📧 Starting weekly release radar email cron job...")

    successes, failures, skipped = asyncio.run(release_radar_email_cron_job(event))

    log.info(f"✅ Release radar email complete - {len(successes)} sent, {len(failures)} failed")
    
    return success_response(
        {
            "successfulUsers": successes,
            "failedUsers": failures,
            "skippedUsers": skipped
        },
        is_api=False
    )
