"""
XOMIFY Release Radar DynamoDB Helpers
=====================================
Database operations for release radar history table.

Table Structure:
- PK: email (string)
- SK: weekKey (string) - format "YYYY-WW" (e.g., "2025-02")
- releases: list of release objects
- stats: { artistCount, releaseCount, trackCount, albumCount, singleCount }
- playlistId: string
- startDate: string "YYYY-MM-DD" (Saturday)
- endDate: string "YYYY-MM-DD" (Friday)
- createdAt: ISO timestamp

Week Definition: Saturday 00:00:00 to Friday 23:59:59
- Cron runs Saturday morning to process the week that just ended (last Sat - yesterday Fri)
- This captures all "New Music Friday" releases
"""

from datetime import datetime, timezone, timedelta
import boto3
from boto3.dynamodb.conditions import Key

from lambdas.common.logger import get_logger
from lambdas.common.errors import DynamoDBError
from lambdas.common.constants import RELEASE_RADAR_HISTORY_TABLE_NAME

log = get_logger(__file__)

# Initialize DynamoDB
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")


def _get_timestamp() -> str:
    """Get current UTC timestamp."""
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


# ============================================
# Week Key Calculations (Saturday-Friday)
# ============================================

def get_week_key(target_date: datetime = None) -> str:
    """
    Get the week key for a given date in YYYY-WW format.
    
    Uses Saturday-Friday weeks:
    - Week starts on Saturday 00:00:00
    - Week ends on Friday 23:59:59
    
    Args:
        target_date: Date to get week key for (defaults to now)
        
    Returns:
        Week key string like "2025-02"
    """
    if target_date is None:
        target_date = datetime.now()
    
    # If target_date is a date (not datetime), convert it
    if hasattr(target_date, 'hour'):
        d = target_date
    else:
        d = datetime.combine(target_date, datetime.min.time())
    
    # Find the Saturday that starts this week
    # weekday(): Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6
    days_since_saturday = (d.weekday() - 5) % 7  # Sat=0, Sun=1, Mon=2, Tue=3, Wed=4, Thu=5, Fri=6
    week_start_saturday = d - timedelta(days=days_since_saturday)
    
    # Use ISO week number of the Saturday
    iso_year, iso_week, _ = week_start_saturday.isocalendar()
    
    return f"{iso_year}-{iso_week:02d}"


def get_previous_week_key() -> str:
    """
    Get the week key for the PREVIOUS week (the one that just ended Friday).
    
    Used by the cron job which runs Saturday morning to process
    the week that ended Friday night.
    
    Returns:
        Week key for last Saturday-Friday
    """
    # Go back 1 day to get into the previous week (from Saturday -> Friday)
    yesterday = datetime.now() - timedelta(days=1)
    return get_week_key(yesterday)


def get_week_date_range(week_key: str) -> tuple[datetime, datetime]:
    """
    Get the Saturday-Friday date range for a week key.
    
    Args:
        week_key: Week key in "YYYY-WW" format
        
    Returns:
        Tuple of (start_date, end_date) as datetime objects
        start_date = Saturday 00:00:00
        end_date = Friday 23:59:59
    """
    year, week = map(int, week_key.split('-'))
    
    # Find Monday of that ISO week
    jan_4 = datetime(year, 1, 4)  # Jan 4 is always in week 1
    start_of_week_1 = jan_4 - timedelta(days=jan_4.weekday())  # Monday of week 1
    monday_of_week = start_of_week_1 + timedelta(weeks=week - 1)
    
    # Our week starts on Saturday (5 days after Monday)
    saturday = monday_of_week + timedelta(days=5)
    saturday = saturday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Friday is 6 days after Saturday
    friday = saturday + timedelta(days=6)
    friday = friday.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    return saturday, friday


def get_current_week_date_range() -> tuple[datetime, datetime]:
    """Get the date range for the current week."""
    current_week_key = get_week_key()
    return get_week_date_range(current_week_key)


def format_week_display(week_key: str) -> str:
    """
    Format week key for human display.
    
    Returns:
        Human readable string like "Jan 4 - Jan 10, 2025"
    """
    try:
        start_date, end_date = get_week_date_range(week_key)
        
        if start_date.month == end_date.month:
            return f"{start_date.strftime('%b %d')} - {end_date.strftime('%d, %Y')}"
        else:
            return f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}"
    except:
        return f"Week {week_key}"


# ============================================
# Save Release Radar Week
# ============================================

def save_release_radar_week(
    email: str,
    week_key: str,
    releases: list,
    playlist_id: str = None
) -> dict:
    """
    Save a week's release radar data to the history table.
    
    Args:
        email: User's email (partition key)
        week_key: Format "YYYY-WW" (sort key)
        releases: List of release objects
        playlist_id: Optional Spotify playlist ID
        
    Returns:
        The saved item
    """
    try:
        log.info(f"Saving release radar week for {email} - {week_key}")
        
        table = dynamodb.Table(RELEASE_RADAR_HISTORY_TABLE_NAME)
        
        # Get date range for this week
        start_date, end_date = get_week_date_range(week_key)
        
        # Calculate stats
        unique_artists = set()
        album_count = 0
        single_count = 0
        total_tracks = 0
        
        for r in releases:
            artist_id = r.get('artistId')
            if artist_id:
                unique_artists.add(artist_id)
            
            album_type = (r.get('albumType') or r.get('album_type') or '').lower()
            if album_type == 'album':
                album_count += 1
            elif album_type == 'single':
                single_count += 1
            
            total_tracks += r.get('totalTracks') or r.get('total_tracks') or 1
        
        stats = {
            'artistCount': len(unique_artists),
            'releaseCount': len(releases),
            'trackCount': total_tracks,
            'albumCount': album_count,
            'singleCount': single_count
        }
        
        item = {
            'email': email,
            'weekKey': week_key,
            'releases': releases,
            'stats': stats,
            'playlistId': playlist_id,
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
            'createdAt': _get_timestamp()
        }
        
        table.put_item(Item=item)
        
        log.info(f"Saved release radar for {email} - {week_key}: {len(releases)} releases")
        return item
        
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

def get_user_release_radar_history(email: str, limit: int = 26) -> list:
    """
    Get release radar history for a user (newest first).
    
    Args:
        email: User's email
        limit: Max results (default 26 = ~6 months)
        
    Returns:
        List of week records
    """
    try:
        log.info(f"Getting release radar history for {email}")
        
        table = dynamodb.Table(RELEASE_RADAR_HISTORY_TABLE_NAME)
        
        response = table.query(
            KeyConditionExpression=Key('email').eq(email),
            ScanIndexForward=False,  # Newest first
            Limit=limit
        )
        
        weeks = response.get('Items', [])
        
        # Handle pagination if needed
        while 'LastEvaluatedKey' in response and len(weeks) < limit:
            response = table.query(
                KeyConditionExpression=Key('email').eq(email),
                ScanIndexForward=False,
                Limit=limit - len(weeks),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            weeks.extend(response.get('Items', []))
        
        log.info(f"Found {len(weeks)} release radar weeks for {email}")
        return weeks[:limit]
        
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


def check_user_has_history(email: str) -> bool:
    """Check if a user has any release radar history."""
    try:
        table = dynamodb.Table(RELEASE_RADAR_HISTORY_TABLE_NAME)
        
        response = table.query(
            KeyConditionExpression=Key('email').eq(email),
            Limit=1
        )
        
        return len(response.get('Items', [])) > 0
        
    except Exception as err:
        log.error(f"Check user has history failed: {err}")
        return False


def delete_user_release_radar_history(email: str) -> int:
    """Delete all release radar history for a user."""
    try:
        log.info(f"Deleting all release radar history for {email}")
        
        table = dynamodb.Table(RELEASE_RADAR_HISTORY_TABLE_NAME)
        weeks = get_user_release_radar_history(email, limit=100)
        
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
