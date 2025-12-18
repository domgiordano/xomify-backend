from datetime import datetime, timezone

from lambdas.common.dynamo_helpers import (
    update_table_item, 
    get_item_by_key, 
    check_if_item_exist,
    get_user_wrap_history,
    get_user_wrap_by_month,
    get_user_wraps_in_range
)
from lambdas.common.constants import USERS_TABLE_NAME, LOGGER

log = LOGGER.get_logger(__file__)


def update_wrapped_data(data: dict, optional_fields={}):
    """
    Update user's enrollment status in the main user table.
    This handles opt-in/opt-out for wrapped feature.
    """
    try:
        for field in optional_fields:
            if field not in data:
                data[field] = None
        db_entry = add_time_stamp(data)
        response = update_table_item(USERS_TABLE_NAME, db_entry)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return 'User Opted into Monthly Wrapped Success.'
        else:
            raise Exception('Failed to Opt User into Monthly Wrapped')
    except Exception as err:
        log.error(f"Update Wrapped Data: {err}")
        raise Exception(f"Update Wrapped Data: {err}")


def get_wrapped_data(email: str):
    """
    Get user's wrapped data including enrollment status and listening history.
    
    Returns:
        {
            "active": bool,
            "activeWrapped": bool,
            "activeReleaseRadar": bool,
            "wraps": [
                {
                    "monthKey": "2024-12",
                    "topSongIds": { "short_term": [], "medium_term": [], "long_term": [] },
                    "topArtistIds": { "short_term": [], "medium_term": [], "long_term": [] },
                    "topGenres": { "short_term": {}, "medium_term": {}, "long_term": {} },
                    "createdAt": "2025-01-01 00:00:00"
                },
                ...
            ]
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
        if check_if_item_exist(USERS_TABLE_NAME, 'email', email, True):
            user_data = get_item_by_key(USERS_TABLE_NAME, 'email', email)
            response['active'] = user_data.get('active', False)
            response['activeWrapped'] = user_data.get('activeWrapped', False)
            response['activeReleaseRadar'] = user_data.get('activeReleaseRadar', False)
        
        # Get wrapped history from history table (newest first)
        wraps = get_user_wrap_history(email, ascending=False)
        response['wraps'] = wraps
        
        log.info(f"Returning wrapped data for {email}: {len(wraps)} months of history")
        return response
        
    except Exception as err:
        log.error(f"Get Wrapped Data: {err}")
        raise Exception(f"Get Wrapped Data: {err}")


def get_wrapped_month(email: str, month_key: str):
    """
    Get a specific month's wrapped data for a user.
    
    Args:
        email: User's email
        month_key: Format "YYYY-MM" e.g. "2024-12"
    
    Returns:
        Wrap object or None
    """
    try:
        wrap = get_user_wrap_by_month(email, month_key)
        return wrap
    except Exception as err:
        log.error(f"Get Wrapped Month: {err}")
        raise Exception(f"Get Wrapped Month: {err}")


def get_wrapped_year(email: str, year: str):
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
        return wraps
    except Exception as err:
        log.error(f"Get Wrapped Year: {err}")
        raise Exception(f"Get Wrapped Year: {err}")


def add_time_stamp(data):
    time_stamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    data['updatedAt'] = time_stamp
    return data