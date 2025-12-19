"""
XOMIFY Wrapped Handler
======================
Handles wrapped cron jobs and API endpoints.
"""

import asyncio

from lambdas.common.logger import get_logger
from lambdas.common.errors import WrappedError, ValidationError, handle_errors
from lambdas.common.utility_helpers import (
    success_response,
    is_api_request,
    is_cron_event,
    parse_body,
    get_query_params,
    require_fields
)
from wrapped_data import update_wrapped_data, get_wrapped_data, get_wrapped_month, get_wrapped_year
from monthly_wrapped_aiohttp import aiohttp_wrapped_chron_job

log = get_logger(__file__)

HANDLER = 'wrapped'


@handle_errors(HANDLER)
def handler(event, context):
    """
    Main Lambda handler for wrapped endpoints.
    
    Endpoints:
        POST /wrapped/data - Update user enrollment
        GET  /wrapped/data - Get all wrapped data for user
        GET  /wrapped/month - Get specific month's data
        GET  /wrapped/year - Get specific year's data
        
    Cron:
        Triggered monthly to generate wrapped playlists
    """
    
    # Cron job - Monthly Wrapped
    if is_cron_event(event):
        log.info("🎵 Starting monthly wrapped cron job...")
        users_processed = asyncio.run(aiohttp_wrapped_chron_job(event))
        log.info(f"✅ Wrapped complete - {len(users_processed)} users processed")
        return success_response({"usersDownloaded": users_processed}, is_api=False)
    
    # API requests
    is_api = is_api_request(event)
    path = event.get("path", "").lower()
    http_method = event.get("httpMethod", "POST")
    
    log.info(f"API Request: {http_method} {path}")
    
    # POST /wrapped/data - Update user enrollment
    if path == f"/{HANDLER}/data" and http_method == "POST":
        body = parse_body(event)
        require_fields(body, 'email', 'userId', 'refreshToken', 'active')
        
        optional_fields = {'releaseRadarId'}
        response = update_wrapped_data(body, optional_fields)
        return success_response(response, is_api=is_api)
    
    # GET /wrapped/data - Get all wrapped data for user
    if path == f"/{HANDLER}/data" and http_method == "GET":
        params = get_query_params(event)
        require_fields(params, 'email')
        
        response = get_wrapped_data(params['email'])
        return success_response(response, is_api=is_api)
    
    # GET /wrapped/month - Get specific month's data
    if path == f"/{HANDLER}/month" and http_method == "GET":
        params = get_query_params(event)
        require_fields(params, 'email', 'monthKey')
        
        response = get_wrapped_month(params['email'], params['monthKey'])
        if response is None:
            response = {"message": "No wrapped data found for this month"}
        return success_response(response, is_api=is_api)
    
    # GET /wrapped/year - Get specific year's data
    if path == f"/{HANDLER}/year" and http_method == "GET":
        params = get_query_params(event)
        require_fields(params, 'email', 'year')
        
        response = get_wrapped_year(params['email'], params['year'])
        return success_response(response, is_api=is_api)
    
    # Unknown endpoint
    raise ValidationError(
        message=f"Invalid endpoint: {http_method} {path}",
        handler=HANDLER,
        function="handler"
    )
