from lambdas.common.dynamo_helpers import full_table_scan
from lambdas.common.constants import USERS_TABLE_NAME, LOGGER

log = LOGGER.get_logger(__file__)

def get_active_wrapped_users():
     try:
        log.info("Geting active wrapped users...")
        table_values = full_table_scan(USERS_TABLE_NAME)
        table_values[:] = [item for item in table_values if item['activeWrapped']]
        log.info(f"Found {len(table_values)} active users!")
        return table_values
     except Exception as err:
        log.error(f"Get Active Wrapped Users: {err}")
        raise Exception(f"Get Active Wrapped Users: {err}") from err
     
def get_active_release_radar_users():
     try:
        log.info("Geting active release radar users...")
        table_values = full_table_scan(USERS_TABLE_NAME)
        table_values[:] = [item for item in table_values if item['activeReleaseRadar']]
        log.info(f"Found {len(table_values)} active users!")
        return table_values
     except Exception as err:
        log.error(f"Get Active Release Radar Users: {err}")
        raise Exception(f"Get Active Release Radar Users: {err}") from err