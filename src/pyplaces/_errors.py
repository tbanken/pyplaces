# API errors from nominatinum
# Read errors from s3 buckets (?)

class InsufficientResponseError(ValueError):
    """Exception for empty or too few results in server response."""
    
class ResponseStatusCodeError(ValueError):
    """Exception for an unhandled server response status code."""