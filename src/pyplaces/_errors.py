class S3ReadError(FileNotFoundError):
    """Exception for empty results when querying an S3 bucket."""

class PyArrowError(ValueError):
    """Exception for catching PyArrow errors when invalid parameters are passed."""