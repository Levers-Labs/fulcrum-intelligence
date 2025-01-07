import yaml

from query_manager.core.schemas import MetricYAMLSchema
from query_manager.exceptions import ErrorCode, QueryManagerError


class YAMLMetricParser:
    @staticmethod
    def parse_yaml_string(yaml_content: str) -> MetricYAMLSchema:
        """
        Parse YAML string and validate against schema.

        This method takes a YAML string as input, attempts to parse it into a Python dictionary,
        and then validates the parsed data against the MetricYAMLSchema. If the parsing or validation
        fails, it raises a QueryManagerError with appropriate error code and detail.

        Args:
            yaml_content (str): The YAML string to be parsed and validated.

        Returns:
            MetricYAMLSchema: The validated MetricYAMLSchema object.

        Raises:
            QueryManagerError: If the YAML string is invalid or the parsed data fails validation.
        """
        try:
            # Attempt to safely load the YAML content into a Python dictionary
            data = yaml.safe_load(yaml_content)
            # Create and return a MetricYAMLSchema object from the parsed data
            return MetricYAMLSchema(**data)
        except yaml.YAMLError as e:
            # If YAML parsing fails, raise a QueryManagerError with INVALID_YAML error code
            raise QueryManagerError(
                status_code=400, code=ErrorCode.INVALID_YAML, detail=f"Invalid YAML format: {str(e)}"
            ) from e
        except ValueError as e:
            # If validation fails, raise a QueryManagerError with VALIDATION_ERROR error code
            raise QueryManagerError(
                status_code=400, code=ErrorCode.VALIDATION_ERROR, detail=f"Validation error: {str(e)}"
            ) from e
