"""
XOMIFY Wrapped Helper
=====================
Helper functions for wrapped and release radar user queries.
"""

from lambdas.common.logger import get_logger
from lambdas.common.errors import DynamoDBError
from lambdas.common.dynamo_helpers import full_table_scan
from lambdas.common.constants import USERS_TABLE_NAME

log = get_logger(__file__)


def get_active_wrapped_users() -> list:
    """
    Get all users enrolled in monthly wrapped.
    
    Returns:
        List of user dicts with activeWrapped=True
    """
    try:
        log.info("Fetching active wrapped users...")
        
        all_users = full_table_scan(USERS_TABLE_NAME)
        active_users = [
            user for user in all_users 
            if user.get('activeWrapped', False) and user.get('active', False)
        ]
        
        log.info(f"Found {len(active_users)} active wrapped users")
        return active_users
        
    except Exception as err:
        log.error(f"Get active wrapped users failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="get_active_wrapped_users",
            table=USERS_TABLE_NAME
        )


def get_active_release_radar_users() -> list:
    """
    Get all users enrolled in release radar.
    
    Returns:
        List of user dicts with activeReleaseRadar=True
    """
    try:
        log.info("Fetching active release radar users...")
        
        all_users = full_table_scan(USERS_TABLE_NAME)
        active_users = [
            user for user in all_users 
            if user.get('activeReleaseRadar', False) and user.get('active', False)
        ]
        
        log.info(f"Found {len(active_users)} active release radar users")
        return active_users
        
    except Exception as err:
        log.error(f"Get active release radar users failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="get_active_release_radar_users",
            table=USERS_TABLE_NAME
        )
