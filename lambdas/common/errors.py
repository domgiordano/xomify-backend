"""
XOMIFY Error Classes
====================
Standardized error handling for all Lambda functions.

Features:
- Consistent error response format
- HTTP status codes
- Easy to catch and handle
- Serializable for API responses
"""

import json
import traceback
from typing import Optional
from lambdas.common.logger import get_logger

log = get_logger(__file__)


class XomifyError(Exception):
    """
    Base exception class for all Xomify errors.
    
    Usage:
        raise XomifyError("Something went wrong", status=400)
        
    Or catch and convert to response:
        except XomifyError as e:
            return e.to_response()
    """
    
    def __init__(
        self, 
        message: str, 
        handler: str = "unknown",
        function: str = "unknown", 
        status: int = 500,
        details: Optional[dict] = None
    ):
        self.message = message
        self.handler = handler
        self.function = function
        self.status = status
        self.details = details or {}
        super().__init__(self.message)
    
    def to_dict(self) -> dict:
        """Convert error to dictionary for JSON response."""
        return {
            "error": {
                "message": self.message,
                "handler": self.handler,
                "function": self.function,
                "status": self.status,
                **self.details
            }
        }
    
    def to_response(self, is_api: bool = True) -> dict:
        """Convert error to Lambda response format."""
        body = self.to_dict()
        return {
            "statusCode": self.status,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            },
            "body": json.dumps(body) if is_api else body,
            "isBase64Encoded": False
        }
    
    def log_error(self):
        """Log the error with full context."""
        log.error(f"💥 {self.__class__.__name__} in {self.handler}.{self.function}: {self.message}")
        if self.details:
            log.error(f"   Details: {self.details}")
    
    def __str__(self) -> str:
        return json.dumps(self.to_dict())


# ============================================
# Specific Error Types
# ============================================

class AuthorizationError(XomifyError):
    """Raised when authorization fails."""
    
    def __init__(self, message: str = "Unauthorized", handler: str = "authorizer", function: str = "unknown"):
        super().__init__(
            message=message,
            handler=handler,
            function=function,
            status=401
        )


class ValidationError(XomifyError):
    """Raised when input validation fails."""
    
    def __init__(self, message: str, handler: str = "unknown", function: str = "unknown", field: str = None):
        details = {"field": field} if field else {}
        super().__init__(
            message=message,
            handler=handler,
            function=function,
            status=400,
            details=details
        )


class NotFoundError(XomifyError):
    """Raised when a resource is not found."""
    
    def __init__(self, message: str, handler: str = "unknown", function: str = "unknown", resource: str = None):
        details = {"resource": resource} if resource else {}
        super().__init__(
            message=message,
            handler=handler,
            function=function,
            status=404,
            details=details
        )


class DynamoDBError(XomifyError):
    """Raised when DynamoDB operations fail."""
    
    def __init__(self, message: str, handler: str = "dynamo_helpers", function: str = "unknown", table: str = None):
        details = {"table": table} if table else {}
        super().__init__(
            message=message,
            handler=handler,
            function=function,
            status=500,
            details=details
        )


class SpotifyAPIError(XomifyError):
    """Raised when Spotify API calls fail."""
    
    def __init__(self, message: str, handler: str = "spotify", function: str = "unknown", endpoint: str = None):
        details = {"endpoint": endpoint} if endpoint else {}
        super().__init__(
            message=message,
            handler=handler,
            function=function,
            status=502,
            details=details
        )


class WrappedError(XomifyError):
    """Raised when wrapped processing fails."""
    
    def __init__(self, message: str, handler: str = "wrapped", function: str = "unknown"):
        super().__init__(
            message=message,
            handler=handler,
            function=function,
            status=500
        )

class ReleaseRadarEmailError(XomifyError):
    """Raised when wrapped processing fails."""
    
    def __init__(self, message: str, handler: str = "wrapped", function: str = "unknown"):
        super().__init__(
            message=message,
            handler=handler,
            function=function,
            status=500
        )
class ReleaseRadarError(XomifyError):
    """Raised when release radar processing fails."""
    
    def __init__(self, message: str, handler: str = "release_radar", function: str = "unknown"):
        super().__init__(
            message=message,
            handler=handler,
            function=function,
            status=500
        )


class WrappedEmailError(XomifyError):
    """Raised when email sending fails."""
    
    def __init__(self, message: str, handler: str = "wrapped_email", function: str = "unknown"):
        super().__init__(
            message=message,
            handler=handler,
            function=function,
            status=500
        )


class UserTableError(XomifyError):
    """Raised when user table operations fail."""
    
    def __init__(self, message: str, handler: str = "user", function: str = "unknown"):
        super().__init__(
            message=message,
            handler=handler,
            function=function,
            status=500
        )


# ============================================
# Error Handler Decorator
# ============================================

def handle_errors(handler_name: str):
    """
    Decorator to handle errors consistently across handlers.
    
    Usage:
        @handle_errors("wrapped")
        def handler(event, context):
            ...
    """
    def decorator(func):
        def wrapper(event, context):
            try:
                return func(event, context)
            except XomifyError as e:
                e.log_error()
                return e.to_response()
            except Exception as e:
                # Catch unexpected errors
                log.error(f"💥 Unexpected error in {handler_name}: {str(e)}")
                log.error(traceback.format_exc())
                
                error = XomifyError(
                    message=str(e),
                    handler=handler_name,
                    function=func.__name__,
                    status=500
                )
                return error.to_response()
        return wrapper
    return decorator


# ============================================
# Backward Compatibility Aliases
# ============================================
# These match your old error class names

BaseXomifyException = XomifyError
LambdaAuthorizerError = AuthorizationError
UnauthorizedError = AuthorizationError
DynamodbError = DynamoDBError
WrappednError = WrappedError  # Note: fixing the typo from original
UpdateUserTableError = UserTableError
