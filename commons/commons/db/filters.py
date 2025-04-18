import logging
from collections.abc import Callable
from datetime import date, datetime, time
from typing import Any, Generic, TypeVar

from pydantic import BaseModel
from pydantic.fields import FieldInfo
from sqlalchemy import Column, Select

T = TypeVar("T", bound=BaseModel)
logger = logging.getLogger(__name__)


class FilterField(FieldInfo):
    """
    A custom Pydantic field that defines a filter function for a specific field in a SQLAlchemy model.
    """

    def __init__(
        self,
        field: Column,
        operator: str = "eq",
        filter_fn: Callable[[Select, Any], Select] | None = None,
        join_model: Any = None,
        join_condition: Any = None,
        select_from: Any | None = None,
        **kwargs,
    ):
        """
        Initialize the FilterField.

        :param field: The SQLAlchemy column to filter on.
        :param operator: The comparison operator to use for filtering.
            Supported operators: "eq", "ne", "lt", "le", "gt", "ge", "like",
            "ilike", "in", "not_in", "between", "not_between", "is", "is_not".
        :param filter_fn: A custom filter function to apply the filter.
            If provided, this function will be used instead of the default filter function.
        :param kwargs: Keyword arguments to pass to the Pydantic Field constructor.
        """
        super().__init__(**kwargs)
        self.field = field
        self.operator = operator
        self.filter_fn = filter_fn or self.get_filter_function()
        self.join_model = join_model
        self.join_condition = join_condition
        self.select_from = select_from

    def get_filter_function(self) -> Callable[[Select, Any], Select]:
        """
        Get the filter function based on the specified operator.

        :return: The filter function.
        """

        def filter_fn(query: Select, value: Any) -> Select:
            if value is None:
                return query

            if isinstance(value, date):
                if self.operator == "ge":
                    value = datetime.combine(value, time.min)
                elif self.operator == "le":
                    value = datetime.combine(value, time.max)

            if self.operator == "eq":
                return query.where(self.field == value)
            if self.operator == "ne":
                return query.where(self.field != value)
            if self.operator == "lt":
                return query.where(self.field < value)
            if self.operator == "le":
                return query.where(self.field <= value)
            if self.operator == "gt":
                return query.where(self.field > value)
            if self.operator == "ge":
                return query.where(self.field >= value)
            if self.operator == "like":
                return query.where(self.field.like(value))
            if self.operator == "ilike":
                return query.where(self.field.ilike(f"%{value}%"))
            if self.operator == "in":
                return query.where(self.field.in_(value))
            if self.operator == "not_in":
                return query.where(~self.field.in_(value))
            if self.operator == "between":
                return query.where(self.field.between(value[0], value[1]))
            if self.operator == "not_between":
                return query.where(~self.field.between(value[0], value[1]))
            if self.operator == "is":
                return query.where(self.field.is_(value))
            if self.operator == "is_not":
                return query.where(self.field.isnot(value))
            raise ValueError(f"Unsupported operator: {self.operator}")

        return filter_fn

    def apply_filter(self, query: Select, value: Any) -> Select:
        """
        Apply the filter to a SQLAlchemy select query.

        :param query: The original query.
        :param value: The value to filter by.

        :return: The modified query.
        """
        return self.filter_fn(query, value)


class BaseFilter(BaseModel, Generic[T]):
    @classmethod
    def apply_filters(cls, query: Select, values: dict[str, Any]) -> Select:
        """
        Dynamically apply filters to a SQLAlchemy Select query.

        :param query: The original query.
        :param values: A dictionary of filter names and values.

        :return: The modified query.
        """

        # Iterate through each field name and value in the provided dictionary
        for field_name, value in values.items():
            # Check if the value is not None and if the field name exists in the model fields
            if value is not None and cls.model_fields.get(field_name):
                filter_field = cls.model_fields[field_name]
                # Ensure the filter field is an instance of FilterField
                if not isinstance(filter_field, FilterField):
                    logger.error("Field %s is not a FilterField", field_name)
                    continue

                if hasattr(filter_field, "select_from") and filter_field.select_from:  # type: ignore
                    query = query.select_from(filter_field.select_from)

                # If the filter field has a join model specified, apply the join
                if hasattr(filter_field, "join_model") and filter_field.join_model:
                    join_condition = (
                        filter_field.join_condition()
                        if callable(filter_field.join_condition)
                        else filter_field.join_condition
                    )
                    query = query.join(filter_field.join_model, join_condition)

                # Attempt to apply the filter to the query
                try:
                    query = filter_field.apply_filter(query, value)
                except Exception as exc:
                    # Log any errors that occur during filter application
                    logger.error("Error applying filter %s: %s", field_name, exc)
                    continue
        # Return the modified query after applying all filters
        return query
