"""
XOMIFY Wrapped Data
===================
Data operations for wrapped feature.
"""

from datetime import datetime, timezone

from lambdas.common.logger import get_logger
from lambdas.common.errors import DynamoDBError, NotFoundError
from lambdas.common.dynamo_helpers import (
    update_table_item, 
    get_item_by_key, 
    check_if_item_exist,
    get_user_wrap_history,
    get_user_wrap_by_month,
    get_user_wraps_in_range
)
from lambdas.common.constants import USERS_TABLE_NAME

log = get_logger(__file__)


def update_wrapped_data(data: dict, optional_fields: set = None) -> str:
    """
    Update user's enrollment status in the main user table.
    
    Args:
        data: User data with email, userId, refreshToken, active
        optional_fields: Optional fields to include
        
    Returns:
        Success message
        
    Raises:
        DynamoDBError: If database operation fails
    """
    try:
        # Set optional fields to None if not provided
        if optional_fields:
            for field in optional_fields:
                if field not in data:
                    data[field] = None
        
        # Add timestamp
        data['updatedAt'] = _get_timestamp()
        
        # Save to DynamoDB
        response = update_table_item(USERS_TABLE_NAME, data)
        
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            log.info(f"User {data.get('email')} enrolled in wrapped")
            return 'User opted into Monthly Wrapped successfully.'
        else:
            raise DynamoDBError(
                message="Failed to update user table",
                function="update_wrapped_data",
                table=USERS_TABLE_NAME
            )
            
    except DynamoDBError:
        raise
    except Exception as err:
        log.error(f"Update wrapped data failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="update_wrapped_data",
            table=USERS_TABLE_NAME
        )


def get_wrapped_data(email: str) -> dict:
    """
    Get user's wrapped data including enrollment status and listening history.
    
    Args:
        email: User's email address
        
    Returns:
        {
            "active": bool,
            "activeWrapped": bool,
            "activeReleaseRadar": bool,
            "wraps": [...]
        }
    """
    try:
        response = {
            'active': False,
            'activeWrapped': False,
            'activeReleaseRadar': False,
            'wraps': []
        }
        
        # Get user enrollment status from main table
        if check_if_item_exist(USERS_TABLE_NAME, 'email', email, override=True):
            user_data = get_item_by_key(USERS_TABLE_NAME, 'email', email)
            response['active'] = user_data.get('active', False)
            response['activeWrapped'] = user_data.get('activeWrapped', False)
            response['activeReleaseRadar'] = user_data.get('activeReleaseRadar', False)
        
        # Get wrapped history from history table (newest first)
        wraps = get_user_wrap_history(email, ascending=False)
        response['wraps'] = wraps
        
        log.info(f"Retrieved wrapped data for {email}: {len(wraps)} months of history")
        return response
        
    except Exception as err:
        log.error(f"Get wrapped data failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="get_wrapped_data"
        )


def get_wrapped_month(email: str, month_key: str) -> dict | None:
    """
    Get a specific month's wrapped data for a user.
    
    Args:
        email: User's email
        month_key: Format "YYYY-MM" e.g. "2024-12"
        
    Returns:
        Wrap object or None if not found
    """
    try:
        wrap = get_user_wrap_by_month(email, month_key)
        
        if wrap:
            log.info(f"Retrieved {month_key} wrapped for {email}")
        else:
            log.info(f"No wrapped data for {email} in {month_key}")
            
        return wrap
        
    except Exception as err:
        log.error(f"Get wrapped month failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="get_wrapped_month"
        )


def get_wrapped_year(email: str, year: str) -> list:
    """
    Get all wrapped data for a specific year.
    
    Args:
        email: User's email
        year: Format "YYYY" e.g. "2024"
        
    Returns:
        List of wrap objects for that year
    """
    try:
        start_month = f"{year}-01"
        end_month = f"{year}-12"
        
        wraps = get_user_wraps_in_range(email, start_month, end_month)
        log.info(f"Retrieved {len(wraps)} months of wrapped data for {email} in {year}")
        
        return wraps
        
    except Exception as err:
        log.error(f"Get wrapped year failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="get_wrapped_year"
        )


def _get_timestamp() -> str:
    """Get current UTC timestamp."""
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
