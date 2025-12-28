"""
XOMIFY Release Radar DynamoDB Helpers
=====================================
Database operations for release radar history table.

Table Structure:
- PK: email (string)
- SK: weekKey (string) - format "YYYY-WW" (e.g., "2024-51")
- releases: list of release objects
- stats: { totalTracks, albumCount, singleCount, appearsOnCount }
- playlistId: string
- finalized: boolean (True after cron runs, False during week)
- lastUpdated: date string "YYYY-MM-DD" (for daily refresh check)
- createdAt: ISO timestamp

Week Definition: Sunday 00:00:00 to Saturday 23:59:59
- Cron runs Sunday ~2 AM and processes the PREVIOUS week (last Sun-Sat)
"""

from datetime import datetime, timezone, timedelta
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal

from lambdas.common.logger import get_logger
from lambdas.common.errors import DynamoDBError
from lambdas.common.constants import RELEASE_RADAR_HISTORY_TABLE_NAME

log = get_logger(__file__)

# Initialize DynamoDB
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")


def _get_timestamp() -> str:
    """Get current UTC timestamp."""
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


def _get_today_date() -> str:
    """Get current date as YYYY-MM-DD string."""
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')


# ============================================
# Week Key Calculations (Sunday-Saturday)
# ============================================

def get_week_key(target_date: datetime = None) -> str:
    """
    Get the week key for a given date in YYYY-WW format.
    
    Uses Sunday-Saturday weeks:
    - Week starts on Sunday 00:00:00
    - Week ends on Saturday 23:59:59
    
    Args:
        target_date: Date to get week key for (defaults to now)
        
    Returns:
        Week key string like "2024-51"
    """
    if target_date is None:
        target_date = datetime.now()
    
    # If target_date is a date (not datetime), convert it
    if hasattr(target_date, 'hour'):
        d = target_date
    else:
        d = datetime.combine(target_date, datetime.min.time())
    
    # Find the Sunday that starts this week
    # weekday(): Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6
    days_since_sunday = (d.weekday() + 1) % 7  # Sun=0, Mon=1, ..., Sat=6
    week_start_sunday = d - timedelta(days=days_since_sunday)
    
    # Get ISO week number using Thursday of our week (ISO standard)
    thursday_of_week = week_start_sunday + timedelta(days=4)
    iso_year, iso_week, _ = thursday_of_week.isocalendar()
    
    return f"{iso_year}-{iso_week:02d}"


def get_previous_week_key() -> str:
    """
    Get the week key for the PREVIOUS week (the one that just ended).
    
    Used by the cron job which runs Sunday morning to process
    the week that ended Saturday night.
    
    Returns:
        Week key for last Sunday-Saturday
    """
    # Go back 1 day to get into the previous week
    yesterday = datetime.now() - timedelta(days=1)
    return get_week_key(yesterday)


def get_week_date_range(week_key: str) -> tuple[datetime, datetime]:
    """
    Get the Sunday-Saturday date range for a week key.
    
    Args:
        week_key: Week key in "YYYY-WW" format
        
    Returns:
        Tuple of (start_date, end_date) as datetime objects
        start_date = Sunday 00:00:00
        end_date = Saturday 23:59:59
    """
    year, week = map(int, week_key.split('-'))
    
    # Find Monday of that ISO week
    jan_4 = datetime(year, 1, 4)  # Jan 4 is always in week 1
    start_of_week_1 = jan_4 - timedelta(days=jan_4.weekday())  # Monday of week 1
    monday_of_week = start_of_week_1 + timedelta(weeks=week - 1)
    
    # Our week starts on Sunday (1 day before Monday)
    sunday = monday_of_week - timedelta(days=1)
    sunday = sunday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Saturday is 6 days after Sunday
    saturday = sunday + timedelta(days=6)
    saturday = saturday.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    return sunday, saturday


def get_current_week_date_range() -> tuple[datetime, datetime]:
    """
    Get the date range for the current (incomplete) week.
    
    Returns:
        Tuple of (start_date, end_date) for current week
    """
    current_week_key = get_week_key()
    return get_week_date_range(current_week_key)


def is_release_in_week(release_date_str: str, week_key: str) -> bool:
    """
    Check if a release date falls within a specific week.
    
    Args:
        release_date_str: Release date string (YYYY-MM-DD, YYYY-MM, or YYYY)
        week_key: Week key in "YYYY-WW" format
        
    Returns:
        True if release is within the week
    """
    try:
        if not release_date_str or len(release_date_str) < 4:
            return False
        
        start_date, end_date = get_week_date_range(week_key)
        
        # Parse release date based on format
        if len(release_date_str) == 4:
            # Year only - can't determine week
            return False
        elif len(release_date_str) == 7:
            # Year-month only - treat as first of month
            release_date = datetime.strptime(release_date_str, "%Y-%m")
        else:
            # Full date
            release_date = datetime.strptime(release_date_str[:10], "%Y-%m-%d")
        
        return start_date <= release_date <= end_date
        
    except Exception:
        return False


# ============================================
# Save Release Radar History
# ============================================

def save_release_radar_week(
    email: str,
    week_key: str,
    releases: list,
    playlist_id: str = None,
    finalized: bool = False
) -> dict:
    """
    Save a single week's release radar data to the history table.
    
    Args:
        email: User's email (partition key)
        week_key: Format "YYYY-WW" (sort key)
        releases: List of release objects
        playlist_id: Optional Spotify playlist ID for this week
        finalized: True if cron has processed this week (locks it)
        
    Returns:
        The saved item
    """
    try:
        log.info(f"Saving release radar week for {email} - {week_key} (finalized={finalized})")
        
        table = dynamodb.Table(RELEASE_RADAR_HISTORY_TABLE_NAME)
        
        # Calculate stats
        stats = {
            'totalTracks': len(releases),
            'albumCount': len([r for r in releases if r.get('albumType') == 'album' or r.get('album_type') == 'album']),
            'singleCount': len([r for r in releases if r.get('albumType') == 'single' or r.get('album_type') == 'single']),
            'appearsOnCount': len([r for r in releases if r.get('albumType') == 'appears_on' or r.get('album_type') == 'appears_on'])
        }
        
        # Normalize releases for storage
        stored_releases = []
        for r in releases:
            # Handle both formats (from Spotify API vs already normalized)
            if 'artistName' in r:
                # Already normalized
                stored_releases.append(r)
            else:
                # From Spotify API - normalize it
                stored_releases.append({
                    'id': r.get('id'),
                    'name': r.get('name'),
                    'artistName': r.get('artists', [{}])[0].get('name', 'Unknown'),
                    'artistId': r.get('artists', [{}])[0].get('id'),
                    'imageUrl': r.get('images', [{}])[0].get('url') if r.get('images') else None,
                    'albumType': r.get('album_type'),
                    'releaseDate': r.get('release_date'),
                    'totalTracks': r.get('total_tracks', 1),
                    'uri': r.get('uri')
                })
        
        item = {
            'email': email,
            'weekKey': week_key,
            'releases': stored_releases,
            'stats': stats,
            'playlistId': playlist_id,
            'finalized': finalized,
            'lastUpdated': _get_today_date(),
            'createdAt': _get_timestamp()
        }
        
        response = table.put_item(Item=item)
        
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            log.info(f"Saved release radar for {email} - {week_key}: {stats['totalTracks']} releases")
            return item
        
        raise DynamoDBError(
            message="Failed to save release radar week",
            function="save_release_radar_week",
            table=RELEASE_RADAR_HISTORY_TABLE_NAME
        )
        
    except DynamoDBError:
        raise
    except Exception as err:
        log.error(f"Save release radar week failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="save_release_radar_week",
            table=RELEASE_RADAR_HISTORY_TABLE_NAME
        )


# ============================================
# Get Release Radar History
# ============================================

def get_user_release_radar_history(
    email: str, 
    limit: int = None, 
    ascending: bool = False,
    finalized_only: bool = False
) -> list:
    """
    Get all release radar history for a user.
    
    Args:
        email: User's email
        limit: Optional limit on results
        ascending: If True, oldest first (default: newest first)
        finalized_only: If True, only return finalized weeks
        
    Returns:
        List of week records
    """
    try:
        log.info(f"Getting release radar history for {email} (finalized_only={finalized_only})")
        
        table = dynamodb.Table(RELEASE_RADAR_HISTORY_TABLE_NAME)
        
        query_params = {
            'KeyConditionExpression': Key('email').eq(email),
            'ScanIndexForward': ascending
        }
        
        if limit:
            query_params['Limit'] = limit
        
        response = table.query(**query_params)
        weeks = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response and (limit is None or len(weeks) < limit):
            query_params['ExclusiveStartKey'] = response['LastEvaluatedKey']
            response = table.query(**query_params)
            weeks.extend(response.get('Items', []))
        
        if limit:
            weeks = weeks[:limit]
        
        # Filter to finalized only if requested
        if finalized_only:
            weeks = [w for w in weeks if w.get('finalized', False)]
        
        log.info(f"Found {len(weeks)} release radar weeks for {email}")
        return weeks
        
    except Exception as err:
        log.error(f"Get release radar history failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="get_user_release_radar_history",
            table=RELEASE_RADAR_HISTORY_TABLE_NAME
        )


def get_release_radar_week(email: str, week_key: str) -> dict | None:
    """
    Get a specific week's release radar data.
    
    Args:
        email: User's email
        week_key: Week key in "YYYY-WW" format
        
    Returns:
        Week record or None if not found
    """
    try:
        log.info(f"Getting release radar for {email} - {week_key}")
        
        table = dynamodb.Table(RELEASE_RADAR_HISTORY_TABLE_NAME)
        
        response = table.get_item(
            Key={'email': email, 'weekKey': week_key}
        )
        
        if 'Item' in response:
            return response['Item']
        
        log.info(f"No release radar found for {email} - {week_key}")
        return None
        
    except Exception as err:
        log.error(f"Get release radar week failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="get_release_radar_week",
            table=RELEASE_RADAR_HISTORY_TABLE_NAME
        )


def get_release_radar_in_range(
    email: str, 
    start_week: str, 
    end_week: str,
    finalized_only: bool = False
) -> list:
    """
    Get release radar data within a week range.
    
    Args:
        email: User's email
        start_week: Start week key (inclusive)
        end_week: End week key (inclusive)
        finalized_only: If True, only return finalized weeks
        
    Returns:
        List of week records in the range
    """
    try:
        log.info(f"Getting release radar for {email} from {start_week} to {end_week}")
        
        table = dynamodb.Table(RELEASE_RADAR_HISTORY_TABLE_NAME)
        
        response = table.query(
            KeyConditionExpression=Key('email').eq(email) & Key('weekKey').between(start_week, end_week),
            ScanIndexForward=False  # Newest first
        )
        
        weeks = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression=Key('email').eq(email) & Key('weekKey').between(start_week, end_week),
                ScanIndexForward=False,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            weeks.extend(response.get('Items', []))
        
        # Filter to finalized only if requested
        if finalized_only:
            weeks = [w for w in weeks if w.get('finalized', False)]
        
        log.info(f"Found {len(weeks)} release radar weeks in range for {email}")
        return weeks
        
    except Exception as err:
        log.error(f"Get release radar in range failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="get_release_radar_in_range",
            table=RELEASE_RADAR_HISTORY_TABLE_NAME
        )


def check_user_has_history(email: str, finalized_only: bool = True) -> bool:
    """
    Check if a user has any release radar history.
    
    Args:
        email: User's email
        finalized_only: If True, only check for finalized weeks
        
    Returns:
        True if user has at least one week of history
    """
    try:
        table = dynamodb.Table(RELEASE_RADAR_HISTORY_TABLE_NAME)
        
        response = table.query(
            KeyConditionExpression=Key('email').eq(email),
            Limit=5 if finalized_only else 1
        )
        
        items = response.get('Items', [])
        
        if finalized_only:
            return any(item.get('finalized', False) for item in items)
        
        return len(items) > 0
        
    except Exception as err:
        log.error(f"Check user has history failed: {err}")
        return False


def check_week_needs_refresh(email: str, week_key: str) -> bool:
    """
    Check if a week needs to be refreshed (not updated today).
    
    Args:
        email: User's email
        week_key: Week key to check
        
    Returns:
        True if week doesn't exist, is not finalized, and wasn't updated today
    """
    try:
        week_data = get_release_radar_week(email, week_key)
        
        if not week_data:
            # No data exists, needs refresh
            return True
        
        if week_data.get('finalized', False):
            # Finalized weeks never need refresh
            return False
        
        # Check if already updated today
        last_updated = week_data.get('lastUpdated', '')
        today = _get_today_date()
        
        return last_updated != today
        
    except Exception as err:
        log.error(f"Check week needs refresh failed: {err}")
        return True  # Default to needing refresh on error


def delete_user_release_radar_history(email: str) -> int:
    """
    Delete all release radar history for a user.
    
    Args:
        email: User's email
        
    Returns:
        Number of items deleted
    """
    try:
        log.info(f"Deleting all release radar history for {email}")
        
        table = dynamodb.Table(RELEASE_RADAR_HISTORY_TABLE_NAME)
        
        # Get all items for user
        weeks = get_user_release_radar_history(email)
        
        # Delete each item
        deleted = 0
        with table.batch_writer() as batch:
            for week in weeks:
                batch.delete_item(
                    Key={
                        'email': email,
                        'weekKey': week['weekKey']
                    }
                )
                deleted += 1
        
        log.info(f"Deleted {deleted} release radar weeks for {email}")
        return deleted
        
    except Exception as err:
        log.error(f"Delete user release radar history failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="delete_user_release_radar_history",
            table=RELEASE_RADAR_HISTORY_TABLE_NAME
        )
