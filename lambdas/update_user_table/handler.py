"""
XOMIFY User Table Handler
=========================
API endpoints for user management.
"""

from lambdas.common.logger import get_logger
from lambdas.common.errors import UserTableError, ValidationError, handle_errors
from lambdas.common.utility_helpers import (
    success_response,
    is_api_request,
    parse_body,
    get_query_params,
    require_fields
)
from lambdas.common.dynamo_helpers import (
    update_user_table_refresh_token,
    update_user_table_enrollments,
    get_user_table_data
)

log = get_logger(__file__)

HANDLER = 'user'


@handle_errors(HANDLER)
def handler(event, context):
    """
    Main Lambda handler for user table operations.
    
    Endpoints:
        POST /user/user-table - Update user (refresh token or enrollments)
        GET  /user/user-table - Get user data
    """
    
    path = event.get("path", "").lower()
    http_method = event.get("httpMethod", "POST")
    
    log.info(f"API Request: {http_method} {path}")
    
    # GET /user/user-table - Get user data
    if path == f"/{HANDLER}/user-table" and http_method == "GET":
        params = get_query_params(event)
        require_fields(params, 'email')
        
        response = get_user_table_data(params['email'])
        log.info(f"Retrieved data for {params['email']}")
        
        return success_response(response)  # is_api=True by default, JSON stringifies body
    
    # POST /user/user-table - Update user
    if path == f"/{HANDLER}/user-table" and http_method == "POST":
        body = parse_body(event)
        require_fields(body, 'email')
        
        # Determine which update type based on fields present
        has_enrollment_fields = 'wrappedEnrolled' in body or 'releaseRadarEnrolled' in body
        has_token_fields = 'refreshToken' in body and 'userId' in body
        
        if has_token_fields:
            # Update refresh token (also returns current enrollment status)
            response = update_user_table_refresh_token(
                body['email'],
                body['userId'],
                body['refreshToken']
            )
            log.info(f"Updated refresh token for {body['email']}")
            
        elif has_enrollment_fields:
            # Update enrollments
            wrapped = body.get('wrappedEnrolled', False)
            radar = body.get('releaseRadarEnrolled', False)
            
            response = update_user_table_enrollments(
                body['email'],
                wrapped,
                radar
            )
            log.info(f"Updated enrollments for {body['email']}: wrapped={wrapped}, radar={radar}")
            
        else:
            raise ValidationError(
                message="Invalid request - must include either (refreshToken, userId) or (wrappedEnrolled/releaseRadarEnrolled)",
                handler=HANDLER,
                function="handler"
            )
        
        return success_response(response)  # is_api=True by default, JSON stringifies body
    
    # Unknown endpoint
    raise ValidationError(
        message=f"Invalid endpoint: {http_method} {path}",
        handler=HANDLER,
        function="handler"
    )