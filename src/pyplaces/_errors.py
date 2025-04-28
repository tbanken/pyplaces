class UnsupportedOperatorError(NotImplementedError):
    """Exception for catching unsupported PyArrow operators"""

class S3ReadError(FileNotFoundError):
    """Exception for empty results when querying an S3 bucket."""