import asyncio
import traceback
from lambdas.common.utility_helpers import build_successful_handler_response, build_error_handler_response
from lambdas.common.errors import ReleaseRadarError
from weekly_release_radar_aiohttp import aiohttp_release_radar_chron_job

from lambdas.common.constants import LOGGER

log = LOGGER.get_logger(__file__)

HANDLER = 'release-radar'


def handler(event, context):
    try:

        # Monthly Wrapped Chron Job
        if 'body' not in event and event.get("source") == 'aws.events':
            success, failures = asyncio.run(aiohttp_release_radar_chron_job(event))
            return build_successful_handler_response({"successfulUsers": success, "failedUsers": failures}, False)

        else:
            raise Exception("Invalid Call: Must call from chron job.", 400)

    except Exception as err:
        message = err.args[0]
        function = f'handler.{__name__}'
        if len(err.args) > 1:
            function = err.args[1]
        log.error(traceback.print_exc())
        error = ReleaseRadarError(message, HANDLER, function) if 'Invalid User Input' not in message else ReleaseRadarError(message, HANDLER, function, 400)
        return build_error_handler_response(str(error))
