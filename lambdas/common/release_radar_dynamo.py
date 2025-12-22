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
- createdAt: ISO timestamp
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


def get_week_key(target_date: datetime = None) -> str:
    """
    Get the week key for a given date in YYYY-WW format.
    
    Uses Saturday-Friday weeks:
    - Week starts on Saturday
    - Week ends on Friday
    
    Args:
        target_date: Date to get week key for (defaults to today)
        
    Returns:
        Week key string like "2024-51"
    """
    if target_date is None:
        target_date = datetime.now()
    
    # Find the Saturday that starts this week
    # weekday(): Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6
    days_since_saturday = (target_date.weekday() - 5) % 7
    if days_since_saturday == 0 and target_date.weekday() != 5:
        days_since_saturday = 7
    
    week_start_saturday = target_date - timedelta(days=days_since_saturday)
    
    # Get ISO week number of that Saturday
    iso_year, iso_week, _ = week_start_saturday.isocalendar()
    
    return f"{iso_year}-{iso_week:02d}"


def get_week_date_range(week_key: str) -> tuple[datetime, datetime]:
    """
    Get the Saturday-Friday date range for a week key.
    
    Args:
        week_key: Week key in "YYYY-WW" format
        
    Returns:
        Tuple of (start_date, end_date) as datetime objects
    """
    year, week = map(int, week_key.split('-'))
    
    # Find the first day of that ISO week (Monday)
    jan_4 = datetime(year, 1, 4)  # Jan 4 is always in week 1
    start_of_week_1 = jan_4 - timedelta(days=jan_4.weekday())
    monday_of_week = start_of_week_1 + timedelta(weeks=week - 1)
    
    # Our week starts on Saturday (5 days before Monday of next week, or 2 days before this Monday... wait)
    # Actually, Saturday before this Monday
    saturday = monday_of_week - timedelta(days=2)
    friday = saturday + timedelta(days=6)
    
    return saturday, friday


# ============================================
# Save Release Radar History
# ============================================

def save_release_radar_week(
    email: str,
    week_key: str,
    releases: list,
    playlist_id: str = None
) -> dict:
    """
    Save a single week's release radar data to the history table.
    
    Args:
        email: User's email (partition key)
        week_key: Format "YYYY-WW" (sort key)
        releases: List of release objects with structure:
            {
                id: string,
                name: string,
                artists: [{ id, name }],
                images: [{ url }],
                album_type: 'album' | 'single' | 'appears_on',
                release_date: string,
                total_tracks: number,
                uri: string
            }
        playlist_id: Optional Spotify playlist ID for this week
        
    Returns:
        The saved item
    """
    try:
        log.info(f"Saving release radar week for {email} - {week_key}")
        
        table = dynamodb.Table(RELEASE_RADAR_HISTORY_TABLE_NAME)
        
        # Calculate stats
        stats = {
            'totalTracks': len(releases),
            'albumCount': len([r for r in releases if r.get('album_type') == 'album']),
            'singleCount': len([r for r in releases if r.get('album_type') == 'single']),
            'appearsOnCount': len([r for r in releases if r.get('album_type') == 'appears_on'])
        }
        
        # Simplify releases for storage (don't store everything)
        stored_releases = []
        for r in releases:
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

def get_user_release_radar_history(email: str, limit: int = None, ascending: bool = False) -> list:
    """
    Get all release radar history for a user.
    
    Args:
        email: User's email
        limit: Optional limit on results
        ascending: If True, oldest first (default: newest first)
        
    Returns:
        List of week records
    """
    try:
        log.info(f"Getting release radar history for {email}")
        
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


def get_release_radar_in_range(email: str, start_week: str, end_week: str) -> list:
    """
    Get release radar data within a week range.
    
    Args:
        email: User's email
        start_week: Start week key (inclusive)
        end_week: End week key (inclusive)
        
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
        
        log.info(f"Found {len(weeks)} release radar weeks in range for {email}")
        return weeks
        
    except Exception as err:
        log.error(f"Get release radar in range failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="get_release_radar_in_range",
            table=RELEASE_RADAR_HISTORY_TABLE_NAME
        )


def check_user_has_history(email: str) -> bool:
    """
    Check if a user has any release radar history.
    
    Args:
        email: User's email
        
    Returns:
        True if user has at least one week of history
    """
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
