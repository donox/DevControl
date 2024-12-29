class ApplicationException(Exception):
    """Base class for application-specific exceptions."""
    pass

class InvalidInputError(ApplicationException):
    """Raised when the input data is invalid."""
    pass

class OperationError(ApplicationException):
    """Raised when an operation fails."""
    pass
