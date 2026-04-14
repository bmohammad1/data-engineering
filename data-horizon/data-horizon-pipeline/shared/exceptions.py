"""Custom exception classes for the data-horizon pipeline."""


class PipelineError(Exception):
    """Base exception for all pipeline errors."""

    def __init__(self, message: str, *, service: str = "", run_id: str = "") -> None:
        self.service = service
        self.run_id = run_id
        super().__init__(message)

    @property
    def error_detail(self) -> dict:
        return {
            "error_type": type(self).__name__,
            "service": self.service,
            "message": str(self),
            "run_id": self.run_id,
        }


class RetryableError(PipelineError):
    """Transient failure that Step Functions should retry."""


class PermanentError(PipelineError):
    """Non-recoverable failure that Step Functions should catch and alert on."""


class ConfigLoadError(PermanentError):
    """Raised when pipeline config cannot be loaded from Secrets Manager."""


class S3WriteError(RetryableError):
    """Raised when writing to S3 fails."""


class DynamoDBError(RetryableError):
    """Raised when a DynamoDB operation fails."""


class TagFileError(PermanentError):
    """Raised when the tags CSV file is missing or malformed."""
