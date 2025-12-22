"""
XOMIFY DynamoDB Helpers
=======================
Database operations for DynamoDB tables.
"""

from datetime import datetime, timezone
import boto3
from boto3.dynamodb.conditions import Key

from lambdas.common.logger import get_logger
from lambdas.common.errors import DynamoDBError, NotFoundError
from lambdas.common.constants import (
    AWS_DEFAULT_REGION,
    DYNAMODB_KMS_ALIAS,
    USERS_TABLE_NAME,
    WRAPPED_HISTORY_TABLE_NAME
)

log = get_logger(__file__)

# Initialize clients
dynamodb = boto3.resource("dynamodb", region_name=AWS_DEFAULT_REGION)
dynamodb_client = boto3.client("dynamodb", region_name=AWS_DEFAULT_REGION)
kms_client = boto3.client("kms")


# ============================================
# Generic Table Operations
# ============================================

def full_table_scan(table_name: str, **kwargs) -> list:
    """
    Perform a full table scan with optional sorting.
    
    Args:
        table_name: Name of the DynamoDB table
        attribute_name_to_sort_by: Optional field to sort by
        is_reverse: If True, sort descending
        
    Returns:
        List of all items in the table
    """
    try:
        table = dynamodb.Table(table_name)
        
        # Initial scan
        response = table.scan()
        data = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response.get('Items', []))
        
        # Sort if requested
        if 'attribute_name_to_sort_by' in kwargs:
            is_reverse = kwargs.get('is_reverse', False)
            data = sorted(
                data,
                key=lambda x: x.get(kwargs['attribute_name_to_sort_by'], ''),
                reverse=is_reverse
            )
        
        return data
        
    except Exception as err:
        log.error(f"Full table scan failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="full_table_scan",
            table=table_name
        )


def get_item_by_key(table_name: str, key_name: str, key_value: str) -> dict:
    """
    Get a single item by its primary key.
    
    Raises:
        NotFoundError: If item doesn't exist
    """
    try:
        table = dynamodb.Table(table_name)
        response = table.get_item(Key={key_name: key_value})
        
        if 'Item' in response:
            return response['Item']
        
        raise NotFoundError(
            message=f"Item not found: {key_value}",
            function="get_item_by_key",
            resource=f"{table_name}/{key_value}"
        )
        
    except NotFoundError:
        raise
    except Exception as err:
        log.error(f"Get item failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="get_item_by_key",
            table=table_name
        )


def check_if_item_exist(table_name: str, key_name: str, key_value: str, override: bool = False) -> bool:
    """
    Check if an item exists in the table.
    
    Args:
        override: If True, return False instead of raising error when not found
    """
    try:
        table = dynamodb.Table(table_name)
        response = table.get_item(Key={key_name: key_value})
        
        if 'Item' in response:
            return True
        
        if override:
            return False
        
        raise NotFoundError(
            message=f"Item not found: {key_value}",
            function="check_if_item_exist",
            resource=f"{table_name}/{key_value}"
        )
        
    except NotFoundError:
        raise
    except Exception as err:
        log.error(f"Check item exist failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="check_if_item_exist",
            table=table_name
        )


def update_table_item(table_name: str, item: dict) -> dict:
    """
    Put/update an entire item in the table.
    """
    try:
        table = dynamodb.Table(table_name)
        response = table.put_item(Item=item)
        return response
        
    except Exception as err:
        log.error(f"Update item failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="update_table_item",
            table=table_name
        )


def update_table_item_field(
    table_name: str,
    key_name: str,
    key_value: str,
    attr_key: str,
    attr_value
) -> dict:
    """
    Update a single field of an item.
    """
    try:
        # Verify item exists
        check_if_item_exist(table_name, key_name, key_value)
        
        table = dynamodb.Table(table_name)
        response = table.update_item(
            Key={key_name: key_value},
            UpdateExpression="SET #attr = :val",
            ExpressionAttributeNames={'#attr': attr_key},
            ExpressionAttributeValues={':val': attr_value},
            ReturnValues="UPDATED_NEW"
        )
        return response
        
    except (NotFoundError, DynamoDBError):
        raise
    except Exception as err:
        log.error(f"Update field failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="update_table_item_field",
            table=table_name
        )


def delete_table_item(table_name: str, key_name: str, key_value: str) -> dict:
    """
    Delete an item from the table.
    """
    try:
        check_if_item_exist(table_name, key_name, key_value)
        
        table = dynamodb.Table(table_name)
        response = table.delete_item(Key={key_name: key_value})
        return response
        
    except (NotFoundError, DynamoDBError):
        raise
    except Exception as err:
        log.error(f"Delete item failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="delete_table_item",
            table=table_name
        )


def query_table_by_key(table_name: str, key_name: str, key_value: str, ascending: bool = False) -> dict:
    """
    Query items by partition key.
    """
    try:
        table = dynamodb.Table(table_name)
        response = table.query(
            KeyConditionExpression=Key(key_name).eq(key_value),
            ScanIndexForward=ascending
        )
        return response
        
    except Exception as err:
        log.error(f"Query table failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="query_table_by_key",
            table=table_name
        )


# ============================================
# User Table Operations
# ============================================

def _get_timestamp() -> str:
    """Get current UTC timestamp."""
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


def update_user_table_refresh_token(email: str, user_id: str, display_name: str, refresh_token: str) -> dict:
    """
    Update or create user with new refresh token.
    """
    try:
        # Get existing user or create new
        user_exists = check_if_item_exist(USERS_TABLE_NAME, 'email', email, override=True)
        user = get_item_by_key(USERS_TABLE_NAME, 'email', email) if user_exists else {}
        
        # Update fields
        user['email'] = email
        user['userId'] = user_id
        user['displayName'] = display_name
        user['refreshToken'] = refresh_token
        user['active'] = True
        user['updatedAt'] = _get_timestamp()
        
        update_table_item(USERS_TABLE_NAME, user)
        log.info(f"Updated refresh token for {email}")
        
        return user
        
    except Exception as err:
        log.error(f"Update refresh token failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="update_user_table_refresh_token",
            table=USERS_TABLE_NAME
        )


def update_user_table_enrollments(email: str, wrapped_enrolled: bool, release_radar_enrolled: bool) -> dict:
    """
    Update user enrollment status.
    """
    try:
        user = get_item_by_key(USERS_TABLE_NAME, 'email', email)
        
        user['activeWrapped'] = wrapped_enrolled
        user['activeReleaseRadar'] = release_radar_enrolled
        user['updatedAt'] = _get_timestamp()
        
        update_table_item(USERS_TABLE_NAME, user)
        log.info(f"Updated enrollments for {email}: wrapped={wrapped_enrolled}, radar={release_radar_enrolled}")
        
        return user
        
    except Exception as err:
        log.error(f"Update enrollments failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="update_user_table_enrollments",
            table=USERS_TABLE_NAME
        )


def update_user_table_release_radar_id(user: dict, playlist_id: str):
    """
    Update user's release radar playlist ID.
    """
    try:
        user['releaseRadarId'] = playlist_id
        user['updatedAt'] = _get_timestamp()
        update_table_item(USERS_TABLE_NAME, user)
        log.info(f"Updated release radar ID for {user.get('email')}: {playlist_id}")
        
    except Exception as err:
        log.error(f"Update release radar ID failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="update_user_table_release_radar_id",
            table=USERS_TABLE_NAME
        )


def get_user_table_data(email: str) -> dict:
    """
    Get user data by email.
    """
    try:
        return get_item_by_key(USERS_TABLE_NAME, 'email', email)
    except Exception as err:
        log.error(f"Get user data failed: {err}")
        raise


# ============================================
# Wrapped History Table Operations
# ============================================

def save_monthly_wrap(
    email: str,
    month_key: str,
    top_song_ids: dict,
    top_artist_ids: dict,
    top_genres: dict
) -> dict:
    """
    Save a single month's wrapped data to the history table.
    
    Args:
        email: User's email (partition key)
        month_key: Format "YYYY-MM" (sort key)
        top_song_ids: { short_term: [], medium_term: [], long_term: [] }
        top_artist_ids: { short_term: [], medium_term: [], long_term: [] }
        top_genres: { short_term: {}, medium_term: {}, long_term: {} }
    """
    try:
        log.info(f"Saving monthly wrap for {email} - {month_key}")
        
        table = dynamodb.Table(WRAPPED_HISTORY_TABLE_NAME)
        
        item = {
            'email': email,
            'monthKey': month_key,
            'topSongIds': top_song_ids,
            'topArtistIds': top_artist_ids,
            'topGenres': top_genres,
            'createdAt': _get_timestamp()
        }
        
        response = table.put_item(Item=item)
        
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            log.info(f"Saved wrap for {email} - {month_key}")
            return item
        
        raise DynamoDBError(
            message="Failed to save wrap",
            function="save_monthly_wrap",
            table=WRAPPED_HISTORY_TABLE_NAME
        )
        
    except DynamoDBError:
        raise
    except Exception as err:
        log.error(f"Save monthly wrap failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="save_monthly_wrap",
            table=WRAPPED_HISTORY_TABLE_NAME
        )


def get_user_wrap_history(email: str, limit: int = None, ascending: bool = False) -> list:
    """
    Get all wrapped history for a user.
    
    Args:
        email: User's email
        limit: Optional limit on results
        ascending: If True, oldest first
    """
    try:
        log.info(f"Getting wrap history for {email}")
        
        table = dynamodb.Table(WRAPPED_HISTORY_TABLE_NAME)
        
        query_params = {
            'KeyConditionExpression': Key('email').eq(email),
            'ScanIndexForward': ascending
        }
        
        if limit:
            query_params['Limit'] = limit
        
        response = table.query(**query_params)
        wraps = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response and (limit is None or len(wraps) < limit):
            query_params['ExclusiveStartKey'] = response['LastEvaluatedKey']
            response = table.query(**query_params)
            wraps.extend(response.get('Items', []))
        
        if limit:
            wraps = wraps[:limit]
        
        log.info(f"Found {len(wraps)} wraps for {email}")
        return wraps
        
    except Exception as err:
        log.error(f"Get wrap history failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="get_user_wrap_history",
            table=WRAPPED_HISTORY_TABLE_NAME
        )


def get_user_wrap_by_month(email: str, month_key: str) -> dict | None:
    """
    Get a specific month's wrap data.
    """
    try:
        log.info(f"Getting wrap for {email} - {month_key}")
        
        table = dynamodb.Table(WRAPPED_HISTORY_TABLE_NAME)
        
        response = table.get_item(
            Key={'email': email, 'monthKey': month_key}
        )
        
        if 'Item' in response:
            return response['Item']
        
        log.info(f"No wrap found for {email} - {month_key}")
        return None
        
    except Exception as err:
        log.error(f"Get wrap by month failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="get_user_wrap_by_month",
            table=WRAPPED_HISTORY_TABLE_NAME
        )


def get_user_wraps_in_range(email: str, start_month: str, end_month: str) -> list:
    """
    Get wrap data within a date range.
    """
    try:
        log.info(f"Getting wraps for {email} from {start_month} to {end_month}")
        
        table = dynamodb.Table(WRAPPED_HISTORY_TABLE_NAME)
        
        response = table.query(
            KeyConditionExpression=Key('email').eq(email) & Key('monthKey').between(start_month, end_month),
            ScanIndexForward=False
        )
        
        wraps = response.get('Items', [])
        
        # Handle pagination
        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression=Key('email').eq(email) & Key('monthKey').between(start_month, end_month),
                ScanIndexForward=False,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            wraps.extend(response.get('Items', []))
        
        log.info(f"Found {len(wraps)} wraps in range for {email}")
        return wraps
        
    except Exception as err:
        log.error(f"Get wraps in range failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="get_user_wraps_in_range",
            table=WRAPPED_HISTORY_TABLE_NAME
        )


# ============================================
# Table Management (Admin)
# ============================================

def delete_table(table_name: str) -> dict:
    """Delete a DynamoDB table."""
    try:
        return dynamodb_client.delete_table(TableName=table_name)
    except Exception as err:
        log.error(f"Delete table failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="delete_table",
            table=table_name
        )


def create_table(table_name: str, hash_key: str, hash_key_type: str) -> dict:
    """Create a new DynamoDB table with encryption."""
    try:
        # Wait for any existing table to be deleted
        waiter = dynamodb_client.get_waiter('table_not_exists')
        waiter.wait(TableName=table_name)
        
        # Get KMS key
        kms_key = kms_client.describe_key(KeyId=DYNAMODB_KMS_ALIAS)
        
        # Create table
        table = dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[{'AttributeName': hash_key, 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': hash_key, 'AttributeType': hash_key_type}],
            StreamSpecification={
                'StreamEnabled': True,
                'StreamViewType': 'NEW_AND_OLD_IMAGES'
            },
            SSESpecification={
                'Enabled': True,
                'SSEType': 'KMS',
                'KMSMasterKeyId': kms_key['KeyMetadata']['Arn']
            },
            BillingMode='PAY_PER_REQUEST'
        )
        
        # Wait for table to be active
        waiter = dynamodb_client.get_waiter('table_exists')
        waiter.wait(TableName=table_name)
        
        return table
        
    except Exception as err:
        log.error(f"Create table failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="create_table",
            table=table_name
        )


def empty_table(table_name: str, hash_key: str, hash_key_type: str):
    """Delete and recreate a table (empties all data)."""
    try:
        delete_table(table_name)
        return create_table(table_name, hash_key, hash_key_type)
    except Exception as err:
        log.error(f"Empty table failed: {err}")
        raise DynamoDBError(
            message=str(err),
            function="empty_table",
            table=table_name
        )
