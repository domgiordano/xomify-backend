"""
XOMIFY Release Radar Handler
============================
Weekly cron job to update release radar playlists.
"""

import asyncio

from lambdas.common.logger import get_logger
from lambdas.common.errors import ReleaseRadarError, handle_errors
from lambdas.common.utility_helpers import success_response, is_cron_event
from weekly_release_radar_aiohttp import aiohttp_release_radar_chron_job

log = get_logger(__file__)

HANDLER = 'release-radar'


@handle_errors(HANDLER)
def handler(event, context):
    """
    Main Lambda handler for release radar.
    
    Triggered weekly by CloudWatch Events to update
    release radar playlists for all enrolled users.
    """
    
    # Only allow cron job invocation
    if not is_cron_event(event):
        raise ReleaseRadarError(
            message="Invalid call - must be triggered by cron job",
            function="handler"
        )
    
    log.info("📻 Starting weekly release radar cron job...")
    
    successes, failures = asyncio.run(aiohttp_release_radar_chron_job(event))
    
    log.info(f"✅ Release radar complete - {len(successes)} success, {len(failures)} failed")
    
    return success_response(
        {
            "successfulUsers": successes,
            "failedUsers": failures
        },
        is_api=False
    )
