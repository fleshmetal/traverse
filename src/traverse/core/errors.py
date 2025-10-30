class MissingColumnError(Exception):
    """Raised when an expected column is missing from a DataFrame."""


class InvalidSchemaError(Exception):
    """Raised when the dataframe does not follow the expected schema."""


class ExportError(Exception):
    """Raised for export/IO related failures."""
