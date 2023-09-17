class InvalidRetryTimeoutException(Exception):
    def __init__(self):
        message = 'Retry timeout must be a non negative integer.'
        super().__init__(message)
