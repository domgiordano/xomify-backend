"""
XOMIFY Release Radar API Handler
================================
API endpoints for release radar.

Endpoints:
- GET /release-radar/history - Get user's release radar history
- GET /release-radar/check - Check enrollment status

Cron:
- Weekly cron job runs Saturday morning via CloudWatch Events
"""

import json
import asyncio

from lambdas.common.logger import get_logger
from lambdas.common.utility_helpers import is_cron_event, success_response
from lambdas.common.dynamo_helpers import get_user_table_data
from lambdas.common.release_radar_dynamo import (
    get_user_release_radar_history,
    check_user_has_history,
    get_week_key,
    get_week_date_range,
    format_week_display
)

# Import cron job
from weekly_release_radar_aiohttp import release_radar_cron_job

log = get_logger(__file__)


def handler(event, context):
    """
    Main Lambda handler for release radar.
    Routes to cron job or API endpoints.
    """
    try:
        # ========================================
        # CRON JOB - Weekly Release Radar
        # ========================================
        if is_cron_event(event):
            log.info("📻 Starting weekly release radar cron job...")
            successes, failures = asyncio.run(release_radar_cron_job(event))
            return success_response({
                "successfulUsers": successes,
                "failedUsers": failures
            }, is_api=False)
        
        # ========================================
        # API REQUESTS
        # ========================================
        http_method = event.get('httpMethod', event.get('requestContext', {}).get('http', {}).get('method'))
        path = event.get('path', event.get('rawPath', ''))
        
        log.info(f"Release Radar API: {http_method} {path}")
        
        # Parse request
        query_params = event.get('queryStringParameters') or {}
        
        # Route request
        if 'history' in path and http_method == 'GET':
            return get_history(query_params)
        
        elif 'check' in path and http_method == 'GET':
            return check_status(query_params)
        
        else:
            return response(404, {'error': 'Not found'})
            
    except Exception as err:
        log.error(f"Release Radar handler error: {err}")
        return response(500, {'error': str(err)})


# ============================================
# GET /release-radar/history
# ============================================

def get_history(params: dict) -> dict:
    """
    GET /release-radar/history
    
    Get user's release radar history from database.
    
    Query params:
    - email: User's email (required)
    - limit: Max results (optional, default 26)
    """
    email = params.get('email')
    if not email:
        return response(400, {'error': 'Missing email parameter'})
    
    try:
        limit = int(params.get('limit', 26))
        weeks = get_user_release_radar_history(email, limit=limit)
        
        # Add display name to each week
        for week in weeks:
            week['weekDisplay'] = format_week_display(week.get('weekKey', ''))
        
        # Get current week info
        current_week = get_week_key()
        
        return response(200, {
            'email': email,
            'weeks': weeks,
            'count': len(weeks),
            'currentWeek': current_week,
            'currentWeekDisplay': format_week_display(current_week)
        })
        
    except Exception as err:
        log.error(f"Get history error: {err}")
        return response(500, {'error': str(err)})


# ============================================
# GET /release-radar/check
# ============================================

def check_status(params: dict) -> dict:
    """
    GET /release-radar/check
    
    Check user's release radar status.
    
    Query params:
    - email: User's email (required)
    """
    email = params.get('email')
    if not email:
        return response(400, {'error': 'Missing email parameter'})
    
    try:
        has_history = check_user_has_history(email)
        current_week = get_week_key()
        start_date, end_date = get_week_date_range(current_week)
        
        # Check if user is enrolled
        user = get_user_table_data(email)
        is_enrolled = user.get('activeReleaseRadar', False) if user else False
        
        return response(200, {
            'email': email,
            'enrolled': is_enrolled,
            'hasHistory': has_history,
            'currentWeek': current_week,
            'currentWeekDisplay': format_week_display(current_week),
            'weekStartDate': start_date.strftime('%Y-%m-%d'),
            'weekEndDate': end_date.strftime('%Y-%m-%d')
        })
        
    except Exception as err:
        log.error(f"Check status error: {err}")
        return response(500, {'error': str(err)})


# ============================================
# Response Helper
# ============================================

def response(status_code: int, body: dict) -> dict:
    """Build API Gateway response."""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,Authorization',
            'Access-Control-Allow-Methods': 'GET,OPTIONS'
        },
        'body': json.dumps(body, default=str)
    }