"""
XOMIFY Release Radar API Handler
================================
API endpoints for release radar history.

Endpoints:
- GET /release-radar/history - Get user's release radar history
- GET /release-radar/week/{weekKey} - Get specific week's data
- POST /release-radar/backfill - Trigger backfill for user
"""

import json
import asyncio

from lambdas.common.logger import get_logger
from lambdas.common.release_radar_dynamo import (
    get_user_release_radar_history,
    get_release_radar_week,
    get_release_radar_in_range,
    check_user_has_history,
    get_week_key
)
from release_radar_backfill import backfill_release_radar_history

log = get_logger(__file__)

HANDLER = 'release-radar'

def handler(event, context):
    """
    Main API Gateway handler for release radar endpoints.
    """
    try:
        http_method = event.get('httpMethod', event.get('requestContext', {}).get('http', {}).get('method'))
        path = event.get('path', event.get('rawPath', ''))
        
        log.info(f"Release Radar API: {http_method} {path}")
        
        # Parse request
        query_params = event.get('queryStringParameters') or {}
        path_params = event.get('pathParameters') or {}
        body = {}
        if event.get('body'):
            try:
                body = json.loads(event['body'])
            except:
                pass
        
        # Route request
        if 'history' in path and http_method == 'GET':
            return get_history(query_params)
        
        elif 'week' in path and http_method == 'GET':
            week_key = path_params.get('weekKey') or query_params.get('weekKey')
            return get_week(query_params.get('email'), week_key)
        
        elif 'backfill' in path and http_method == 'POST':
            return trigger_backfill(body)
        
        elif 'check' in path and http_method == 'GET':
            return check_history(query_params.get('email'))
        
        else:
            return response(404, {'error': 'Not found'})
            
    except Exception as err:
        log.error(f"Release Radar API error: {err}")
        return response(500, {'error': str(err)})


def get_history(params: dict) -> dict:
    """
    GET /release-radar/history
    
    Query params:
    - email: User's email (required)
    - limit: Max results (optional, default 26 = ~6 months)
    - startWeek: Start of range (optional)
    - endWeek: End of range (optional)
    """
    email = params.get('email')
    if not email:
        return response(400, {'error': 'Missing email parameter'})
    
    try:
        # Check for range query
        start_week = params.get('startWeek')
        end_week = params.get('endWeek')
        
        if start_week and end_week:
            weeks = get_release_radar_in_range(email, start_week, end_week)
        else:
            limit = int(params.get('limit', 26))
            weeks = get_user_release_radar_history(email, limit=limit)
        
        return response(200, {
            'email': email,
            'weeks': weeks,
            'count': len(weeks),
            'currentWeek': get_week_key()
        })
        
    except Exception as err:
        log.error(f"Get history error: {err}")
        return response(500, {'error': str(err)})


def get_week(email: str, week_key: str) -> dict:
    """
    GET /release-radar/week/{weekKey}
    
    Query params:
    - email: User's email (required)
    """
    if not email:
        return response(400, {'error': 'Missing email parameter'})
    
    if not week_key:
        return response(400, {'error': 'Missing weekKey parameter'})
    
    try:
        week_data = get_release_radar_week(email, week_key)
        
        if not week_data:
            return response(404, {
                'error': 'Week not found',
                'email': email,
                'weekKey': week_key
            })
        
        return response(200, week_data)
        
    except Exception as err:
        log.error(f"Get week error: {err}")
        return response(500, {'error': str(err)})


def check_history(email: str) -> dict:
    """
    GET /release-radar/check
    
    Check if user has any history (for triggering backfill).
    
    Query params:
    - email: User's email (required)
    """
    if not email:
        return response(400, {'error': 'Missing email parameter'})
    
    try:
        has_history = check_user_has_history(email)
        
        return response(200, {
            'email': email,
            'hasHistory': has_history,
            'currentWeek': get_week_key()
        })
        
    except Exception as err:
        log.error(f"Check history error: {err}")
        return response(500, {'error': str(err)})


def trigger_backfill(body: dict) -> dict:
    """
    POST /release-radar/backfill
    
    Trigger history backfill for a user.
    
    Body:
    - user: User object with email, refreshToken, etc.
    """
    user = body.get('user')
    if not user:
        return response(400, {'error': 'Missing user data'})
    
    email = user.get('email')
    if not email:
        return response(400, {'error': 'Missing email in user data'})
    
    try:
        # Check if already has history
        if check_user_has_history(email):
            return response(200, {
                'email': email,
                'status': 'skipped',
                'reason': 'history_exists'
            })
        
        # Run backfill
        result = asyncio.run(backfill_release_radar_history(user))
        
        return response(200, result)
        
    except Exception as err:
        log.error(f"Backfill error: {err}")
        return response(500, {'error': str(err)})


def response(status_code: int, body: dict) -> dict:
    """Build API Gateway response."""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,Authorization',
            'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
        },
        'body': json.dumps(body, default=str)
    }
