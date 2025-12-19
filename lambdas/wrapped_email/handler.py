"""
XOMIFY Wrapped Email Handler
============================
Monthly cron job to send wrapped preview emails.
"""

import asyncio

from lambdas.common.logger import get_logger
from lambdas.common.errors import WrappedEmailError, handle_errors
from lambdas.common.utility_helpers import success_response, is_cron_event
from monthly_wrapped_email import wrapped_email_cron_job

log = get_logger(__file__)

HANDLER = 'wrapped-email'


@handle_errors(HANDLER)
def handler(event, context):
    """
    Main Lambda handler for wrapped emails.
    
    Triggered monthly by CloudWatch Events to send
    wrapped preview emails to all enrolled users.
    """
    
    # Only allow cron job invocation
    if not is_cron_event(event):
        raise WrappedEmailError(
            message="Invalid call - must be triggered by cron job",
            function="handler"
        )
    
    log.info("📧 Starting monthly wrapped email cron job...")
    
    successes, failures = asyncio.run(wrapped_email_cron_job(event))
    
    log.info(f"✅ Wrapped email complete - {len(successes)} sent, {len(failures)} failed")
    
    return success_response(
        {
            "successfulUsers": successes,
            "failedUsers": failures
        },
        is_api=False
    )
