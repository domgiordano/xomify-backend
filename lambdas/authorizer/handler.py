

import jwt
from lambdas.common.constants import PRODUCT
from lambdas.common.ssm_helpers import API_SECRET_KEY
from lambdas.common.errors import LambdaAuthorizerError
from lambdas.common.logger import get_logger

log = get_logger(__file__)

HANDLER = 'authorizer'

def generate_policy(effect, resource):
    #Return a valid AWS policy response
    #auth_response = {'principalId': principal_id}
    auth_response = {
        'principalId': PRODUCT,
        'policyDocument': {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Action': 'execute-api:*',
                    'Effect': effect,
                    'Resource': resource
                }
            ]
        }
    }
    return auth_response

def decode_auth_token(auth_token):
    #Decodes the auth token
    try:
        # remove "Bearer " from the token string.
        auth_token = auth_token.replace('Bearer ', '')
        # decode using system environ $SECRET_KEY, will crash if not set.
        return jwt.decode(auth_token, API_SECRET_KEY, algorithms=['HS256'])
    except jwt.ExpiredSignatureError:
        'Signature expired. Please log in again.'
        return
    except jwt.InvalidTokenError:
        'Invalid token. Please log in again.'
        return
    
def handler(event, context):
    try:
        method_arn = event.get('methodArn', '')
        auth_token = event.get('authorizationToken', '')
        
        if auth_token and method_arn:
            user_details = decode_auth_token(auth_token)
            if user_details:
                arn_parts = method_arn.split(':')
                api_gateway_arn_tmp = arn_parts[5].split('/')
                # Construct: arn:aws:execute-api:region:account:apiId/stage/*
                resource_arn = f"{arn_parts[0]}:{arn_parts[1]}:{arn_parts[2]}:{arn_parts[3]}:{arn_parts[4]}:{api_gateway_arn_tmp[0]}/{api_gateway_arn_tmp[1]}/*"
                
                return generate_policy('Allow', resource_arn)
            
        log.warning("Authroizer: Deny.")
        return generate_policy('Deny', method_arn)
    except Exception as err:
        message = err.args[0]
        function = 'handler'
        if len(err.args) > 1:
            function = err.args[1]
        log.error('💥 Error in Lambda Authorizer: ' + message)
        error = LambdaAuthorizerError(message, HANDLER, function)
        return generate_policy('Deny', method_arn)
    
