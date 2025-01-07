import yaml

from query_manager.core.schemas import MetricYAMLSchema
from query_manager.exceptions import ErrorCode, QueryManagerError


class YAMLMetricParser:
    @staticmethod
    def parse_yaml_string(yaml_content: str) -> MetricYAMLSchema:
        """Parse YAML string and validate against schema"""
        try:
            data = yaml.safe_load(yaml_content)
            return MetricYAMLSchema(**data)
        except yaml.YAMLError as e:
            raise QueryManagerError(
                status_code=400, code=ErrorCode.INVALID_YAML, detail=f"Invalid YAML format: {str(e)}"
            ) from e
        except ValueError as e:
            raise QueryManagerError(
                status_code=400, code=ErrorCode.VALIDATION_ERROR, detail=f"Validation error: {str(e)}"
            ) from e
