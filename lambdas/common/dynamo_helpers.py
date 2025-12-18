from datetime import datetime, timezone
import boto3
from boto3.dynamodb.conditions import Key
from lambdas.common.constants import AWS_DEFAULT_REGION, DYNAMODB_KMS_ALIAS, LOGGER, USERS_TABLE_NAME, WRAPPED_HISTORY_TABLE_NAME

log = LOGGER.get_logger(__file__)

dynamodb_res = boto3.resource("dynamodb", region_name=AWS_DEFAULT_REGION)
dynamodb_client = boto3.client("dynamodb", region_name=AWS_DEFAULT_REGION)
kms_res = boto3.client("kms")

HANDLER = 'dynamo_helpers'

# Performs full table scan, and fetches ALL data from table in pages...
def full_table_scan(table_name, **kwargs):
    try:
        table = dynamodb_res.Table(table_name)
        response = table.scan()
        data = response['Items']  # We've got our data now!
        while 'LastEvaluatedKey' in response:  # If we have this field in response...
            # It tells us where we left off, and signifies there's more data to fetch in "pages" after this particular key.
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])  # Add more data as each "page" comes in until we're done (LastEvaluatedKey gone)

        # If we passed in these optional keyword args, let's...
        # SORT the data...default is ascending order even if there are no sort args present.
        if 'attribute_name_to_sort_by' in kwargs:
            is_reverse = kwargs['is_reverse'] if 'is_reverse' in kwargs else False
            data = sorted(data, key=lambda i: i[kwargs['attribute_name_to_sort_by']], reverse=is_reverse)

        return data
    except Exception as err:
        log.error(f"Dynamodb Full Table Scan: {err}")
        raise Exception(f"Dynamodb Full Table Scan: {err}") from err
    
def table_scan_by_ids(table_name, key, ids, goal_filter, **kwargs):
    try:
        table = dynamodb_res.Table(table_name)
        keys = {
            table.name: {
                'Keys': [{key: id} for id in ids]
            }
        }

        response = dynamodb_res.batch_get_item(RequestItems=keys)
        data = response['Responses'][table.name]

        for offering in data:
            if len(offering['rank_dict']) > 0:
                offering['rank'] = offering['rank_dict'][goal_filter]

        # Sort data
        if 'attribute_name_to_sort_by' in kwargs:
            is_reverse = kwargs['is_reverse'] if 'is_reverse' in kwargs else False
            data = sorted(data, key=lambda i: i[kwargs['attribute_name_to_sort_by']], reverse=is_reverse)

        return data
    except Exception as err:
        log.error(f"Dynamodb Table Scan by IDs: {err}")
        raise Exception(f"Dynamodb Table Scan by IDs: {err}") from err

# Update Entire Table Item - Send in full dict of item
def delete_table_item(table_name, primary_key, primary_key_value):
    try:
        check_if_item_exist(table_name, primary_key, primary_key_value)
        table = dynamodb_res.Table(table_name)
        response = table.delete_item(
            Key={
                primary_key: primary_key_value
            }
        )
        return response
    except Exception as err:
        log.error(f"Dynamodb Table Delete Table Item: {err}")
        raise Exception(f"Dynamodb Table Delete Table Item: {err}") from err


# Update Entire Table Item - Send in full dict of item
def update_table_item(table_name, table_item):
    try:
        table = dynamodb_res.Table(table_name)
        response = table.put_item(
            Item=table_item
        )
        return response
    except Exception as err:
        log.error(f"Dynamodb Table Update Table Item: {err}")
        raise Exception(f"Dynamodb Table Update Table Item: {err}") from err


# Update single field of Table - send in one attribute and key
def update_table_item_field(table_name, primary_key, primary_key_value, attr_key, attr_val):
    try:
        check_if_item_exist(table_name, primary_key, primary_key_value)

        table = dynamodb_res.Table(table_name)
        response = table.update_item(
            Key={
                primary_key: primary_key_value
            },
            UpdateExpression="set #attr_key = :attr_val",
            ExpressionAttributeValues={
                ':attr_val': attr_val
            },
            ExpressionAttributeNames={
                '#attr_key': attr_key
            },
            ReturnValues="UPDATED_NEW"
        )
        return response
    except Exception as err:
        log.error(f"Dynamodb Table Update Table Item Field: {err}")
        raise Exception(f"Dynamodb Table Update Table Item Field: {err}") from err

def check_if_item_exist(table_name, id_key, id_val, override=False):
    try:
        table = dynamodb_res.Table(table_name)
        response = table.get_item(
            Key={
                id_key: id_val,
            }
        )
        if 'Item' in response:
            return True
        elif override:
            return False
        else:
            raise Exception("Invalid ID (" + id_val + "): Item Does not Exist.")
    except Exception as err:
        log.error(f"Dynamodb Table Check If Item Exists: {err}")
        raise Exception(f"Dynamodb Table Check If Item Exists: {err}") from err

def get_item_by_key(table_name, id_key, id_val):
    try:

        table = dynamodb_res.Table(table_name)
        response = table.get_item(
            Key={
                id_key: id_val,
            }
        )
        if 'Item' in response:
            return response['Item']
        else:
            raise Exception("Invalid ID (" + id_val + "): Item Does not Exist.")
    except Exception as err:
        log.error(f"Dynamodb Table Get Item By Key: {err}")
        raise Exception(f"Dynamodb Table Get Item By Key: {err}") from err

def query_table_by_key(table_name, id_key, id_val, ascending=False):
    try:
        table = dynamodb_res.Table(table_name)
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key(id_key).eq(id_val),
            ScanIndexForward=ascending
        )
        return response
    except Exception as err:
        log.error(f"Dynamodb Table Query Table By Key: {err}")
        raise Exception(f"Dynamodb Query Table Item By Key: {err}") from err
    
def item_has_property(item, property):
    for field in item:
        if field == property:
            return True

    return False

def emptyTable(table_name, hash_key, hash_key_type):
    try:
        deleteTable(table_name)
        table = createTable(table_name, hash_key, hash_key_type)
        return table
    except Exception as err:
        log.error(f"Dynamodb Table Empty Table: {err}")
        raise Exception(f"Dynamodb Table Empty Table: {err}")

def deleteTable(table_name):
    try:
        return dynamodb_client.delete_table(TableName=table_name)
    except Exception as err:
        log.error(f"Dynamodb Table Delete Table: {err}")
        raise Exception(f"Dynamodb Table Delete Table: {err}") from err
    
def createTable(table_name, hash_key, hash_key_type):
    try:
        #Wait for table to be deleted
        waiter = dynamodb_client.get_waiter('table_not_exists')
        waiter.wait(TableName=table_name)
        # Get KMS Key
        kms_key = kms_res.describe_key(
            KeyId=DYNAMODB_KMS_ALIAS
        )
        #Create table
        table = dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': hash_key,
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions= [
                {
                    'AttributeName': hash_key,
                    'AttributeType': hash_key_type
                }
            ],
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

        #Wait for table to exist
        waiter = dynamodb_client.get_waiter('table_exists')
        waiter.wait(TableName=table_name)

        return table
    except Exception as err:
        log.error(f"Dynamodb Table Create Table: {err}")
        raise Exception(f"Dynamodb Table Create Table: {err}") from err
    

## USER TABLE
def update_user_table_release_radar_id(user: dict, playlist_id: str):
    try:
        # Release Radar Id
        user['releaseRadarId'] = playlist_id
        # Time Stamp
        user['updatedAt'] = __get_time_stamp()
        update_table_item(USERS_TABLE_NAME, user)
    except Exception as err:
        log.error(f"Update User Table Entry: {err}")
        raise Exception(f"Update User Table Entry: {err}") from err
    
def update_user_table_refresh_token(email: str, user_id: str,  refresh_token: str):
    try:
        # Get User Data
        user_exists = check_if_item_exist(USERS_TABLE_NAME, 'email', email, True)
        user = get_item_by_key(USERS_TABLE_NAME, 'email', email) if user_exists else {}
        # Email
        user['email'] = email
        # ID
        user['userId'] = user_id
        # Refresh Token
        user['refreshToken'] = refresh_token
        # Active
        user['active'] = True
        # Time Stamp
        user['updatedAt'] = __get_time_stamp()
        update_table_item(USERS_TABLE_NAME, user)
        return user
    except Exception as err:
        log.error(f"Update User Table Refresh Token: {err}")
        raise Exception(f"Update User Table Refresh Token: {err}") from err
    
def update_user_table_enrollments(email: str, wrapped_enrolled: bool, release_radar_enrolled: bool):
    try:
        # Get User Data
        user = get_item_by_key(USERS_TABLE_NAME, 'email', email)
        # Release Radar Id
        user['activeWrapped'] = wrapped_enrolled
        # Active
        user['activeReleaseRadar'] = release_radar_enrolled
        # Time Stamp
        user['updatedAt'] = __get_time_stamp()
        update_table_item(USERS_TABLE_NAME, user)
        return user
    except Exception as err:
        log.error(f"Update User Table Refresh Token: {err}")
        raise Exception(f"Update User Table Refresh Token: {err}") from err
    
    
def __get_time_stamp():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

def get_user_table_data(email: str):
    try:
        # Get User Data
        user = get_item_by_key(USERS_TABLE_NAME, 'email', email)
        return user
    except Exception as err:
        log.error(f"Get User Table Data: {err}")
        raise Exception(f"Get User Table Data: {err}") from err


# ============================================
# WRAPPED HISTORY TABLE - NEW
# ============================================

def save_monthly_wrap(email: str, month_key: str, top_song_ids: dict, top_artist_ids: dict, top_genres: dict):
    """
    Save a single month's wrapped data to the history table.
    
    Args:
        email: User's email (partition key)
        month_key: Format "YYYY-MM" e.g. "2024-12" (sort key)
        top_song_ids: { short_term: [], medium_term: [], long_term: [] }
        top_artist_ids: { short_term: [], medium_term: [], long_term: [] }
        top_genres: { short_term: {}, medium_term: {}, long_term: {} }
    """
    try:
        log.info(f"Saving monthly wrap for {email} - {month_key}")
        
        table = dynamodb_res.Table(WRAPPED_HISTORY_TABLE_NAME)
        
        item = {
            'email': email,
            'monthKey': month_key,
            'topSongIds': top_song_ids,
            'topArtistIds': top_artist_ids,
            'topGenres': top_genres,
            'createdAt': __get_time_stamp()
        }
        
        response = table.put_item(Item=item)
        
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            log.info(f"Successfully saved wrap for {email} - {month_key}")
            return item
        else:
            raise Exception(f"Failed to save wrap: {response}")
            
    except Exception as err:
        log.error(f"Save Monthly Wrap: {err}")
        raise Exception(f"Save Monthly Wrap: {err}") from err


def get_user_wrap_history(email: str, limit: int = None, ascending: bool = False):
    """
    Get all wrapped history for a user, sorted by month.
    
    Args:
        email: User's email
        limit: Optional limit on number of results
        ascending: If True, oldest first. If False (default), newest first.
    
    Returns:
        List of wrap objects sorted by monthKey
    """
    try:
        log.info(f"Getting wrap history for {email}")
        
        table = dynamodb_res.Table(WRAPPED_HISTORY_TABLE_NAME)
        
        query_params = {
            'KeyConditionExpression': Key('email').eq(email),
            'ScanIndexForward': ascending  # False = descending (newest first)
        }
        
        if limit:
            query_params['Limit'] = limit
        
        response = table.query(**query_params)
        wraps = response.get('Items', [])
        
        # Handle pagination if needed
        while 'LastEvaluatedKey' in response and (limit is None or len(wraps) < limit):
            query_params['ExclusiveStartKey'] = response['LastEvaluatedKey']
            response = table.query(**query_params)
            wraps.extend(response.get('Items', []))
        
        # Apply limit if specified
        if limit:
            wraps = wraps[:limit]
        
        log.info(f"Found {len(wraps)} wraps for {email}")
        return wraps
        
    except Exception as err:
        log.error(f"Get User Wrap History: {err}")
        raise Exception(f"Get User Wrap History: {err}") from err


def get_user_wrap_by_month(email: str, month_key: str):
    """
    Get a specific month's wrap data for a user.
    
    Args:
        email: User's email
        month_key: Format "YYYY-MM" e.g. "2024-12"
    
    Returns:
        Wrap object or None if not found
    """
    try:
        log.info(f"Getting wrap for {email} - {month_key}")
        
        table = dynamodb_res.Table(WRAPPED_HISTORY_TABLE_NAME)
        
        response = table.get_item(
            Key={
                'email': email,
                'monthKey': month_key
            }
        )
        
        if 'Item' in response:
            return response['Item']
        else:
            log.info(f"No wrap found for {email} - {month_key}")
            return None
            
    except Exception as err:
        log.error(f"Get User Wrap By Month: {err}")
        raise Exception(f"Get User Wrap By Month: {err}") from err


def get_user_wraps_in_range(email: str, start_month: str, end_month: str):
    """
    Get wrap data for a user within a date range.
    
    Args:
        email: User's email
        start_month: Format "YYYY-MM" (inclusive)
        end_month: Format "YYYY-MM" (inclusive)
    
    Returns:
        List of wrap objects within the range
    """
    try:
        log.info(f"Getting wraps for {email} from {start_month} to {end_month}")
        
        table = dynamodb_res.Table(WRAPPED_HISTORY_TABLE_NAME)
        
        response = table.query(
            KeyConditionExpression=Key('email').eq(email) & Key('monthKey').between(start_month, end_month),
            ScanIndexForward=False  # Newest first
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
        log.error(f"Get User Wraps In Range: {err}")
        raise Exception(f"Get User Wraps In Range: {err}") from err