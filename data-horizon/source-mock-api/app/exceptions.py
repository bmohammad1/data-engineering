"""Custom exception classes for the  Source Mock API."""

from fastapi import HTTPException, status


class TagNotFoundException(HTTPException):
    """Raised when a requested tag ID does not exist in the static data."""

    def __init__(self, tag_id: str) -> None:
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"TagID '{tag_id}' not found",
        )
