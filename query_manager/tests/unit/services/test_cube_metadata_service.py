import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from query_manager.core.enums import Complexity, CubeFilterOperator, MetricAim
from query_manager.core.models import (
    Metric,
    MetricMetadata,
    SemanticMetaMetric,
    SemanticMetaTimeDimension,
)
from query_manager.services.cube_metadata_service import (
    CSVColumnConfig,
    CSVMetricData,
    CubeDefinition,
    CubeDimension,
    CubeMeasure,
    CubeMetadataService,
)


class TestCSVColumnConfig:
    """Test cases for CSVColumnConfig class."""

    def test_required_columns(self):
        """Test that required columns are correctly defined."""
        expected_required = {
            "measure": "measure",
            "cube": "cube",
            "time_dimension": "time_dimension",
            "dimension_cubes": "dimension_cubes",
        }
        assert CSVColumnConfig.REQUIRED_COLUMNS == expected_required

    def test_optional_columns(self):
        """Test that optional columns include expected fields."""
        optional = CSVColumnConfig.OPTIONAL_COLUMNS
        assert "metric_name" in optional
        assert "definition" in optional
        assert "owning_team" in optional
        assert "complexity" in optional
        assert "aim" in optional

    def test_get_all_columns(self):
        """Test getting all columns."""
        all_columns = CSVColumnConfig.get_all_columns()
        assert "measure" in all_columns
        assert "metric_name" in all_columns
        assert len(all_columns) >= 4  # At least required columns

    def test_validate_csv_columns_valid(self):
        """Test CSV validation with valid columns."""
        df = pd.DataFrame(
            {
                "measure": ["ct_signups"],
                "cube": ["signups"],
                "time_dimension": ["signup_at"],
                "dimension_cubes": ["signups"],
                "metric_name": ["New Signups"],
            }
        )

        is_valid, missing, available = CSVColumnConfig.validate_csv_columns(df)
        assert is_valid
        assert len(missing) == 0
        assert "metric_name" in available

    def test_validate_csv_columns_missing_required(self):
        """Test CSV validation with missing required columns."""
        df = pd.DataFrame(
            {
                "metric_name": ["New Signups"],
                "definition": ["Count of signups"],
            }
        )

        is_valid, missing, available = CSVColumnConfig.validate_csv_columns(df)
        assert not is_valid
        assert len(missing) == 4  # All required columns missing


class TestCSVMetricData:
    """Test cases for CSVMetricData class."""

    def test_from_csv_row_complete_data(self):
        """Test creating CSVMetricData from complete CSV row."""
        row = {
            "measure": "ct_signups",
            "cube": "signups",
            "time_dimension": "signup_at",
            "dimension_cubes": "signups, signups__base",
            "metric_name": "New Signups",
            "definition": "Count of new signups",
            "owning_team": "Marketing",
            "filters": "{signup_plan} = 'hobby'",
            "complexity": "simple",
            "aim": "increase",
        }

        csv_data = CSVMetricData.from_csv_row(row)

        assert csv_data.measure == "ct_signups"
        assert csv_data.cube == "signups"
        assert csv_data.time_dimension == "signup_at"
        assert csv_data.metric_name == "New Signups"
        assert csv_data.definition == "Count of new signups"
        assert csv_data.owning_team == "Marketing"
        assert csv_data.filters == "{signup_plan} = 'hobby'"
        assert csv_data.complexity == "simple"
        assert csv_data.aim == "increase"
        assert "signups" in csv_data.dimension_cubes
        assert "signups__base" in csv_data.dimension_cubes

    def test_from_csv_row_with_nan_values(self):
        """Test handling NaN values in CSV data."""
        row = {
            "measure": "ct_signups",
            "cube": "signups",
            "time_dimension": "signup_at",
            "dimension_cubes": "signups",
            "metric_name": pd.NA,
            "definition": "",
            "owning_team": None,
        }

        csv_data = CSVMetricData.from_csv_row(row)

        assert csv_data.measure == "ct_signups"
        assert csv_data.metric_name is None
        assert csv_data.definition is None
        assert csv_data.owning_team is None

    def test_from_csv_row_missing_required(self):
        """Test error when required fields are missing."""
        row = {
            "metric_name": "Test Metric",
            "definition": "Test definition",
        }

        with pytest.raises(ValueError, match="Required column 'measure' is missing"):
            CSVMetricData.from_csv_row(row)

    def test_get_unique_dimension_cubes(self):
        """Test getting unique dimension cubes."""
        csv_data = CSVMetricData(
            measure="test", cube="test_cube", dimension_cubes=["cube1", "cube1__base", "cube2", "cube1"]
        )

        unique_cubes = csv_data.get_unique_dimension_cubes()

        assert len(unique_cubes) == 2
        assert "cube1" in unique_cubes or "cube1__base" in unique_cubes
        assert "cube2" in unique_cubes

    def test_extract_dimension_cubes_from_csv(self):
        """Test extracting dimension cubes from CSV file."""
        df = pd.DataFrame(
            {
                "measure": ["ct_signups", "ct_hobby_signups"],
                "cube": ["signups", "signups"],
                "time_dimension": ["signup_at", "signup_at"],
                "dimension_cubes": ["signups, signups__base", "signups"],
                "metric_name": ["New Signups", "Hobby Signups"],
            }
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            csv_path = Path(f.name)

        try:
            cubes = CSVMetricData.extract_dimension_cubes_from_csv(csv_path)
            assert "signups" in cubes or "signups__base" in cubes
            assert len(cubes) >= 1
        finally:
            csv_path.unlink()

    def test_validate_and_load_csv_valid(self):
        """Test validating and loading valid CSV."""
        df = pd.DataFrame(
            {
                "measure": ["ct_signups"],
                "cube": ["signups"],
                "time_dimension": ["signup_at"],
                "dimension_cubes": ["signups"],
                "metric_name": ["New Signups"],
            }
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            csv_path = Path(f.name)

        try:
            is_valid, message, csv_data_list = CSVMetricData.validate_and_load_csv(csv_path)
            assert is_valid
            assert "loaded successfully" in message.lower()
            assert len(csv_data_list) == 1
            assert csv_data_list[0].measure == "ct_signups"
        finally:
            csv_path.unlink()

    def test_validate_and_load_csv_invalid(self):
        """Test validating and loading invalid CSV."""
        df = pd.DataFrame(
            {
                "wrong_column": ["value1"],
                "another_wrong": ["value2"],
            }
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            csv_path = Path(f.name)

        try:
            is_valid, message, csv_data_list = CSVMetricData.validate_and_load_csv(csv_path)
            assert not is_valid
            assert "validation failed" in message.lower()
            assert len(csv_data_list) == 0
        finally:
            csv_path.unlink()


class TestCubeModels:
    """Test cases for cube model classes."""

    def test_cube_measure_creation(self):
        """Test creating CubeMeasure."""
        measure = CubeMeasure(
            name="ct_signups",
            title="New Signups",
            short_title="Signups",
            description="Count of signups",
            type="count",
            format="number",
            sql="COUNT(*)",
            filters=[{"sql": "status = 'active'"}],
        )

        assert measure.name == "ct_signups"
        assert measure.title == "New Signups"
        assert measure.type == "count"
        assert len(measure.filters) == 1

    def test_cube_dimension_creation(self):
        """Test creating CubeDimension."""
        dimension = CubeDimension(
            name="signup_at", type="time", title="Signup Date", description="Date when user signed up", sql="signup_at"
        )

        assert dimension.name == "signup_at"
        assert dimension.type == "time"
        assert dimension.title == "Signup Date"

    def test_cube_definition_creation(self):
        """Test creating CubeDefinition."""
        measures = [CubeMeasure(name="ct_signups", type="count")]
        dimensions = [CubeDimension(name="signup_at", type="time")]

        cube = CubeDefinition(name="signups", public=True, measures=measures, dimensions=dimensions)

        assert cube.name == "signups"
        assert cube.public is True
        assert len(cube.measures) == 1
        assert len(cube.dimensions) == 1


class TestCubeMetadataService:
    """Test cases for CubeMetadataService class."""

    @pytest.fixture
    def cube_service(self):
        """Create CubeMetadataService instance."""
        with patch("query_manager.services.cube_metadata_service.get_settings"):
            return CubeMetadataService()

    @pytest.fixture
    def mock_cube_client(self):
        """Mock cube client."""
        client = AsyncMock()
        client.get_cube_measures.return_value = [
            {
                "name": "signups.ct_signups",
                "title": "New Signups",
                "description": "Count of signups",
                "type": "count",
            }
        ]
        client.get_cube_dimensions.return_value = [
            {
                "name": "signups.signup_at",
                "title": "Signup Date",
                "type": "time",
                "description": "Date of signup",
                "cube": "signups",
            }
        ]
        return client

    def test_validate_csv_file_valid(self, cube_service):
        """Test CSV file validation with valid file."""
        df = pd.DataFrame(
            {
                "measure": ["ct_signups"],
                "cube": ["signups"],
                "time_dimension": ["signup_at"],
                "dimension_cubes": ["signups"],
            }
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            csv_path = Path(f.name)

        try:
            is_valid, message = cube_service.validate_csv_file(csv_path)
            assert is_valid
            assert "validation passed" in message.lower()
        finally:
            csv_path.unlink()

    def test_validate_csv_file_invalid(self, cube_service):
        """Test CSV file validation with invalid file."""
        df = pd.DataFrame({"invalid_column": ["value1"]})

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            csv_path = Path(f.name)

        try:
            is_valid, message = cube_service.validate_csv_file(csv_path)
            assert not is_valid
            assert "validation failed" in message.lower()
        finally:
            csv_path.unlink()

    def test_parse_csv_file(self, cube_service):
        """Test parsing CSV file."""
        df = pd.DataFrame(
            {
                "measure": ["ct_signups", "CT_HOBBY_SIGNUPS"],  # Mixed case
                "cube": ["signups", "signups"],
                "time_dimension": ["signup_at", "signup_at"],
                "dimension_cubes": ["signups", "signups"],
                "metric_name": ["New Signups", "Hobby Signups"],
            }
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            csv_path = Path(f.name)

        try:
            csv_map = cube_service._parse_csv_file(csv_path)

            # Should use lowercase keys
            assert "ct_signups" in csv_map
            assert "ct_hobby_signups" in csv_map
            assert "CT_HOBBY_SIGNUPS" not in csv_map
            assert len(csv_map) == 2
        finally:
            csv_path.unlink()

    @pytest.mark.asyncio
    async def test_load_metrics_from_cube(self, cube_service, mock_cube_client):
        """Test loading metrics from cube."""
        metrics = await cube_service.load_metrics_from_cube(mock_cube_client, "signups")

        assert len(metrics) == 1
        mock_cube_client.get_cube_measures.assert_called_once_with("signups")

        # Verify metric structure
        metric = metrics[0]
        assert metric.metric_id == "ct_signups"
        assert metric.label == "New Signups"
        assert metric.meta_data.semantic_meta.cube == "signups"

    @pytest.mark.asyncio
    async def test_load_metrics_from_cube_with_csv(self, cube_service, mock_cube_client):
        """Test loading metrics from cube with CSV filtering."""
        df = pd.DataFrame(
            {
                "measure": ["ct_signups"],
                "cube": ["signups"],
                "time_dimension": ["signup_at"],
                "dimension_cubes": ["signups"],
                "metric_name": ["New Self-Serve Signups"],
                "definition": ["Count of new self-serve signups"],
                "owning_team": ["Marketing"],
            }
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            csv_path = Path(f.name)

        try:
            metrics = await cube_service.load_metrics_from_cube(mock_cube_client, "signups", csv_path)

            assert len(metrics) == 1
            metric = metrics[0]
            assert metric.label == "New Self-Serve Signups"
            assert metric.definition == "Count of new self-serve signups"
            assert metric.owned_by_team == ["Marketing"]
        finally:
            csv_path.unlink()

    @pytest.mark.asyncio
    async def test_load_dimensions_from_cube(self, cube_service, mock_cube_client):
        """Test loading dimensions from cube."""
        dimensions = await cube_service.load_dimensions_from_cube(mock_cube_client, "signups")

        assert len(dimensions) == 1
        mock_cube_client.get_cube_dimensions.assert_called_once_with("signups")

        # Verify dimension structure
        dimension = dimensions[0]
        assert dimension["id"] == "signup_at"
        assert dimension["label"] == "Signup Date"
        assert dimension["metadata"]["semantic_meta"]["cube"] == "signups"
        assert dimension["metadata"]["semantic_meta"]["member_type"] == "dimension"

    @pytest.mark.asyncio
    async def test_filter_dimensions_by_value_count(self, cube_service, mock_cube_client):
        """Test filtering dimensions by value count."""
        dimensions = [
            {
                "id": "signup_plan",
                "label": "Signup Plan",
                "reference": "signup_plan",
                "definition": "Type of plan",
                "metadata": {"semantic_meta": {"cube": "signups", "member": "signup_plan"}},
            }
        ]

        # Mock dimension creation and cube client response
        mock_cube_client.load_dimension_members_from_cube = AsyncMock(
            return_value=["hobby", "pro", "enterprise"]  # 3 values < 15 limit
        )

        filtered = await cube_service.filter_dimensions_by_value_count(dimensions, mock_cube_client, max_values=15)

        assert len(filtered) == 1
        assert filtered[0]["id"] == "signup_plan"
        assert "values" in filtered[0]
        assert "value_count" in filtered[0]
        assert filtered[0]["value_count"] == 3

    def test_create_semantic_meta(self, cube_service):
        """Test creating semantic metadata."""
        measure = CubeMeasure(name="ct_signups", type="count")
        time_dimension = SemanticMetaTimeDimension(cube="signups", member="created_at")
        csv_data = CSVMetricData(
            measure="ct_signups",
            cube="signups",
            dimension_cubes=["signups"],
            time_dimension="signup_at",
            filters="{signup_plan} = 'hobby'",
        )

        semantic_meta = cube_service._create_semantic_meta("signups", measure, time_dimension, csv_data)

        assert semantic_meta.cube == "signups"
        assert semantic_meta.member == "ct_signups"
        assert semantic_meta.time_dimension.member == "signup_at"  # From CSV
        assert len(semantic_meta.cube_filters) == 1

    def test_parse_csv_filters(self, cube_service):
        """Test parsing CSV filter strings."""
        filters_str = "{signup_plan} = 'hobby' AND {status} = 'active'"

        filters = cube_service._parse_csv_filters(filters_str, "signups")

        assert len(filters) == 2
        assert filters[0].dimension == "signups.signup_plan"
        assert filters[0].operator == CubeFilterOperator.EQUALS
        assert filters[0].values == ["hobby"]
        assert filters[1].dimension == "signups.status"
        assert filters[1].values == ["active"]

    def test_parse_measure_filters(self, cube_service):
        """Test parsing measure filters."""
        filters = [{"sql": "{active} = true"}]

        cube_filters = cube_service._parse_measure_filters(filters, "signups")

        assert len(cube_filters) == 1
        assert cube_filters[0].dimension == "signups.active"
        assert cube_filters[0].operator == CubeFilterOperator.EQUALS
        assert cube_filters[0].values == [True]

    def test_generate_variant_metric_id(self, cube_service):
        """Test generating variant metric IDs."""
        base_csv_data = CSVMetricData(
            measure="ct_signups", cube="signups", dimension_cubes=["signups"], metric_name="Total Signups", filters=""
        )

        variant_csv_data = CSVMetricData(
            measure="ct_signups",
            cube="signups",
            dimension_cubes=["signups"],
            metric_name="Hobby Signups",
            filters="{signup_plan} = 'hobby'",
        )

        variant_id = cube_service._generate_variant_metric_id("ct_signups", variant_csv_data, base_csv_data)

        assert variant_id.startswith("ct_signups_")
        assert variant_id != "ct_signups"

    def test_extract_filter_keywords(self, cube_service):
        """Test extracting keywords from filter strings."""
        filters_str = "enterprise inbound"

        keywords = cube_service._extract_filter_keywords(filters_str)

        assert "ent" in keywords or "inbound" in keywords

    def test_static_methods(self, cube_service):
        """Test static utility methods."""
        measure = CubeMeasure(name="revenue", title="Revenue", format="currency", type="sum")

        # Test unit of measure
        unit_of_measure = CubeMetadataService._get_unit_of_measure(measure)
        assert unit_of_measure == "Currency"

        # Test unit
        unit = CubeMetadataService._get_unit(measure)
        assert unit == "$"

        # Test complexity
        complexity = CubeMetadataService._get_complexity(measure)
        assert complexity == Complexity.ATOMIC

        # Test aim
        aim = CubeMetadataService._get_aim(measure)
        assert aim == MetricAim.MAXIMIZE

        # Test aggregation type
        aggregation = CubeMetadataService._get_aggregation_type("count")
        assert aggregation == "sum"

    def test_prepare_metric_data_for_json(self, cube_service):
        """Test preparing metric data for JSON output."""
        semantic_meta = SemanticMetaMetric(
            cube="signups",
            member="ct_signups",
            time_dimension=SemanticMetaTimeDimension(cube="signups", member="signup_at"),
            cube_filters=[],
        )

        metric = Metric(
            metric_id="ct_signups",
            label="New Signups",
            abbreviation="signups",
            definition="Count of signups",
            unit="n",
            unit_of_measure="Count",
            complexity=Complexity.ATOMIC,
            aim=MetricAim.MAXIMIZE,
            grain_aggregation="sum",
            periods=["daily"],
            aggregations=["sum"],
            owned_by_team=["Marketing"],
            terms=[],
            metric_expression=None,
            meta_data=MetricMetadata(semantic_meta=semantic_meta),
            dimensions=[],
            hypothetical_max=None,
        )

        data = cube_service._prepare_metric_data(metric, include_metric_id=True)

        assert data["metric_id"] == "ct_signups"
        assert data["label"] == "New Signups"
        assert "metadata" in data  # Renamed from meta_data
        assert "dimensions" in data
        assert isinstance(data["complexity"], str)  # Converted to string

    def test_prepare_metric_data_for_db(self, cube_service):
        """Test preparing metric data for database."""
        semantic_meta = SemanticMetaMetric(
            cube="signups",
            member="ct_signups",
            time_dimension=SemanticMetaTimeDimension(cube="signups", member="signup_at"),
            cube_filters=[],
        )

        metric = Metric(
            metric_id="ct_signups",
            label="New Signups",
            abbreviation="signups",
            definition="Count of signups",
            unit="n",
            unit_of_measure="Count",
            complexity=Complexity.ATOMIC,
            aim=MetricAim.MAXIMIZE,
            grain_aggregation="sum",
            periods=["daily"],
            aggregations=["sum"],
            owned_by_team=["Marketing"],
            terms=[],
            metric_expression=None,
            meta_data=MetricMetadata(semantic_meta=semantic_meta),
            dimensions=[],
            hypothetical_max=None,
        )

        data = cube_service._prepare_metric_data(metric, include_metric_id=False)

        assert "metric_id" not in data
        assert "meta_data" in data  # Original name for DB
        assert "metadata" not in data
        assert "dimensions" not in data
        assert isinstance(data["complexity"], Complexity)  # Enum for DB


class TestIntegrationScenarios:
    """Integration test scenarios for the service."""

    @pytest.fixture
    def cube_service(self):
        """Create CubeMetadataService instance."""
        with patch("query_manager.services.cube_metadata_service.get_settings"):
            return CubeMetadataService()

    def test_end_to_end_csv_processing(self, cube_service):
        """Test end-to-end CSV processing workflow."""
        # Create comprehensive CSV
        df = pd.DataFrame(
            {
                "measure": ["ct_signups", "ct_hobby_signups"],
                "cube": ["signups", "signups"],
                "time_dimension": ["signup_at", "signup_at"],
                "dimension_cubes": ["signups, signups__base", "signups"],
                "metric_name": ["New Signups", "Hobby Signups"],
                "definition": ["Count of signups", "Count of hobby signups"],
                "owning_team": ["Marketing", "Self-Serve"],
                "filters": ["", "{signup_plan} = 'hobby'"],
                "complexity": ["simple", "simple"],
                "aim": ["increase", "increase"],
            }
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            csv_path = Path(f.name)

        try:
            # Test validation
            is_valid, message = cube_service.validate_csv_file(csv_path)
            assert is_valid

            # Test parsing
            csv_map = cube_service._parse_csv_file(csv_path)
            assert len(csv_map) == 2
            assert "ct_signups" in csv_map
            assert "ct_hobby_signups" in csv_map

            # Test dimension cube extraction
            dimension_cubes = CSVMetricData.extract_dimension_cubes_from_csv(csv_path)
            assert len(dimension_cubes) >= 1
            assert "signups" in dimension_cubes or "signups__base" in dimension_cubes

        finally:
            csv_path.unlink()

    def test_metrics_generation_workflow(self, cube_service):
        """Test metrics generation workflow."""
        # Create measures and CSV data
        measures = [
            CubeMeasure(name="ct_signups", title="Signups", type="count"),
            CubeMeasure(name="ct_hobby_signups", title="Hobby Signups", type="count"),
        ]

        df = pd.DataFrame(
            {
                "measure": ["ct_signups"],
                "cube": ["signups"],
                "time_dimension": ["signup_at"],
                "dimension_cubes": ["signups"],
                "metric_name": ["New Self-Serve Signups"],
                "definition": ["Count of new self-serve signups"],
            }
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            csv_path = Path(f.name)

        try:
            # Generate metrics
            metrics = cube_service._generate_metrics_for_cube("signups", measures, csv_path)

            # Should only generate metric that's in CSV
            assert len(metrics) == 1
            metric = metrics[0]
            assert metric.metric_id == "ct_signups"
            assert metric.label == "New Self-Serve Signups"
            assert metric.definition == "Count of new self-serve signups"

        finally:
            csv_path.unlink()
