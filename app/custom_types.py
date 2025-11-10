# in backend/app/custom_types.py
import datetime
from typing import Any
from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema
from pydantic_core.core_schema import ValidationInfo

def validate_date_from_str(value: Any, info: ValidationInfo) -> datetime.date:
    """
    Takes a string in dd/mm/yyyy, a string in YYYY-MM-DD, a datetime object,
    or a date object and returns a valid date object.
    """
    # --- ADD THIS BLOCK AT THE TOP ---
    # If the value is already a full datetime object, extract the date part.
    if isinstance(value, datetime.datetime):
        return value.date()
    # --- END OF ADDITION ---

    # This part handles values that are already date objects.
    if isinstance(value, datetime.date):
        return value
        
    # This part handles string inputs.
    if isinstance(value, str):
        try:
            return datetime.datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            try:
                return datetime.datetime.strptime(value, "%d/%m/%Y").date()
            except ValueError:
                raise ValueError("Date must be in YYYY-MM-DD or dd/mm/yyyy format")
                
    raise TypeError("Invalid type for a date")

# The FormattedDate class itself does not need to change.
class FormattedDate(datetime.date):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        return core_schema.json_or_python_schema(
            json_schema=core_schema.date_schema(),
            python_schema=core_schema.with_info_before_validator_function(
                validate_date_from_str, core_schema.date_schema()
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda d: d.strftime("%d/%m/%Y")
            ),
        )