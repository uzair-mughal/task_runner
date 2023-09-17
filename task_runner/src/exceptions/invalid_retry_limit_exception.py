class InvalidRetryLimitException(Exception):
    def __init__(self):
        message = 'Retry limit must be a positive integer.'
        super().__init__(message)
