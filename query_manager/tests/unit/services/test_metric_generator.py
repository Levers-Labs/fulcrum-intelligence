import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from query_manager.core.enums import Complexity, MetricAim
from query_manager.core.models import Metric, SemanticMetaTimeDimension
from query_manager.services.metric_generator import (
    CSVMetricData,
    CubeDefinition,
    CubeDimension,
    CubeMeasure,
    MetricGeneratorService,
)


@pytest.fixture
def test_yaml_content():
    """Test YAML content with various cube configurations."""
    return """
cubes:
  - name: test_cube_public
    public: true
    dimensions:
      - name: created_at
        title: Created Date
        type: time
        sql: created_at
        description: Date when the record was created
      - name: status
        title: Status
        type: string
        sql: status
        description: Status of the record
    measures:
      - name: total_count
        title: Total Count
        short_title: Count
        type: count
        sql: id
        description: Total count of records
      - name: avg_amount
        title: Average Amount
        short_title: Avg Amount
        type: avg
        sql: amount
        description: Average amount calculation
        format: currency
      - name: sum_revenue
        title: Sum Revenue
        short_title: Revenue
        type: sum
        sql: revenue
        description: Total revenue sum
        filters:
          - dimension: status
            operator: equals
            values: ["active"]

  - name: test_cube_private
    public: false
    dimensions:
      - name: created_at
        title: Created Date
        type: time
        sql: created_at
    measures:
      - name: private_count
        title: Private Count
        type: count
        sql: id
        description: This should not be processed due to public=false

  - name: test_cube_no_time_dimension
    public: true
    dimensions:
      - name: region
        title: Region
        type: string
        sql: region
    measures:
      - name: count_no_time
        title: Count Without Time
        type: count
        sql: id
        description: Count metric without time dimension
"""


@pytest.fixture
def test_csv_content():
    """Test CSV content with various metric configurations."""
    return """Header Row (This is ignored)
Measure,Metric Name,Definition,Canonical Cube,Anchor Date,Filters,Owning Team(s)
total_count,Total Record Count,The total number of records in the system,test_cube_public,created_at,,Engineering
avg_amount,Average Transaction Amount,The average amount of all transactions,test_cube_public,created_at,,Finance
sum_revenue,Total Revenue,Total revenue generated from all sources,test_cube_public,created_at,status = 'active',Sales
"""


@pytest.fixture
def csv_dataframe():
    """Create a pandas DataFrame for CSV testing."""
    data = {
        "Measure": ["total_count", "avg_amount", "sum_revenue"],
        "Metric Name": ["Total Record Count", "Average Transaction Amount", "Total Revenue"],
        "Definition": ["The total number of records", "The average amount of transactions", "Total revenue generated"],
        "Canonical Cube": ["test_cube_public", "test_cube_public", "test_cube_public"],
        "Anchor Date": ["created_at", "created_at", "created_at"],
        "Filters": ["", "", "status = 'active'"],
        "Owning Team(s)": ["Engineering", "Finance", "Sales"],
    }
    return pd.DataFrame(data)


@pytest.fixture
def metric_generator_service():
    """Create a MetricGeneratorService instance."""
    return MetricGeneratorService()


@pytest.fixture
def sample_cube_measures():
    """Sample cube measures for testing."""
    return [
        CubeMeasure(
            name="total_count",
            title="Total Count",
            short_title="Count",
            type="count",
            sql="id",
            description="Total count of records",
        ),
        CubeMeasure(
            name="avg_amount",
            title="Average Amount",
            short_title="Avg Amount",
            type="avg",
            sql="amount",
            description="Average amount calculation",
            format="currency",
        ),
        CubeMeasure(
            name="sum_revenue",
            title="Sum Revenue",
            short_title="Revenue",
            type="sum",
            sql="revenue",
            description="Total revenue sum",
            filters=[{"dimension": "status", "operator": "equals", "values": ["active"]}],
        ),
    ]


@pytest.fixture
def sample_cube_dimensions():
    """Sample cube dimensions for testing."""
    return [
        CubeDimension(
            name="created_at",
            title="Created Date",
            type="time",
            sql="created_at",
            description="Date when the record was created",
        ),
        CubeDimension(name="status", title="Status", type="string", sql="status", description="Status of the record"),
    ]


class TestMetricGeneratorService:
    """Test cases for MetricGeneratorService."""

    def test_init(self):
        """Test service initialization."""
        service = MetricGeneratorService()
        assert service.settings is not None

    def test_from_yaml_content_public_cubes_only(self, metric_generator_service, test_yaml_content):
        """Test generating metrics from YAML content with only public cubes."""
        metrics = metric_generator_service.from_yaml_content(test_yaml_content)

        # Should generate 3 metrics from test_cube_public, 0 from private, 1 from no_time_dimension
        assert len(metrics) == 4

        # Check metric IDs
        metric_ids = [m.metric_id for m in metrics]
        assert "total_count" in metric_ids
        assert "avg_amount" in metric_ids
        assert "sum_revenue" in metric_ids
        assert "count_no_time" in metric_ids

        # Verify private cube metrics are not included
        assert "private_count" not in metric_ids

    def test_from_yaml_content_with_csv_filtering(self, metric_generator_service, test_yaml_content, csv_dataframe):
        """Test generating metrics from YAML content with CSV filtering."""
        with patch("pandas.read_csv", return_value=csv_dataframe):
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
                f.write("dummy csv content")
                csv_path = f.name

            metrics = metric_generator_service.from_yaml_content(test_yaml_content, csv_path)

            # Should only generate metrics that exist in CSV
            assert len(metrics) == 3
            metric_ids = [m.metric_id for m in metrics]
            assert "total_count" in metric_ids
            assert "avg_amount" in metric_ids
            assert "sum_revenue" in metric_ids
            assert "count_no_time" not in metric_ids  # Not in CSV

    def test_from_yaml_file(self, metric_generator_service, test_yaml_content):
        """Test generating metrics from YAML file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write(test_yaml_content)
            yaml_path = f.name

        metrics = metric_generator_service.from_yaml_file(yaml_path)
        assert len(metrics) == 4

        # Clean up
        Path(yaml_path).unlink()

    def test_save_metrics_to_json(self, metric_generator_service, test_yaml_content):
        """Test saving metrics to JSON file."""
        metrics = metric_generator_service.from_yaml_content(test_yaml_content)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json_path = f.name

        metric_generator_service.save_metrics_to_json(metrics, json_path)

        # Verify JSON file was created and contains correct data
        with open(json_path) as f:
            saved_data = json.load(f)

        assert len(saved_data) == 4
        assert all("metric_id" in item for item in saved_data)
        assert all("complexity" in item for item in saved_data)
        assert all("metadata" in item for item in saved_data)  # Should be renamed from meta_data

        # Clean up
        Path(json_path).unlink()

    @pytest.mark.asyncio
    async def test_save_metrics_to_db_success(self, metric_generator_service, test_yaml_content):
        """Test saving metrics to database successfully."""
        metrics = metric_generator_service.from_yaml_content(test_yaml_content)

        with patch("query_manager.services.metric_generator.set_tenant_id") as mock_set_tenant, patch(
            "query_manager.services.metric_generator.validate_tenant", new_callable=AsyncMock
        ) as mock_validate, patch(
            "query_manager.services.metric_generator.get_async_session"
        ) as mock_session_ctx, patch(
            "query_manager.services.metric_generator.reset_context"
        ) as mock_reset:

            # Mock async session
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_ctx.return_value = mock_session

            result = await metric_generator_service.save_metrics_to_db(metrics, tenant_id=1)

            assert result == 4  # Should save all 4 metrics
            mock_set_tenant.assert_called_once_with(1)
            mock_validate.assert_called_once()
            mock_session.commit.assert_called_once()
            mock_reset.assert_called_once()

    @pytest.mark.asyncio
    async def test_save_metrics_to_db_with_errors(self, metric_generator_service, test_yaml_content):
        """Test saving metrics to database with some errors."""
        metrics = metric_generator_service.from_yaml_content(test_yaml_content)

        with patch("query_manager.services.metric_generator.set_tenant_id") as mock_set_tenant, patch(
            "query_manager.services.metric_generator.validate_tenant", new_callable=AsyncMock
        ) as mock_validate, patch(
            "query_manager.services.metric_generator.get_async_session"
        ) as mock_session_ctx, patch(
            "query_manager.services.metric_generator.reset_context"
        ) as mock_reset:

            # Mock async session with some failures
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_session_ctx.return_value = mock_session

            # Make execute fail for some metrics
            mock_session.execute.side_effect = [None, Exception("DB Error"), None, None]

            result = await metric_generator_service.save_metrics_to_db(metrics, tenant_id=1)

            assert result == 3  # Should save 3 out of 4 metrics
            mock_set_tenant.assert_called_once_with(1)
            mock_validate.assert_called_once()
            mock_session.commit.assert_called_once()
            mock_reset.assert_called_once()

    def test_from_cube_measures_static_method(self, sample_cube_measures, sample_cube_dimensions):
        """Test the static from_cube_measures method."""
        metrics = MetricGeneratorService.from_cube_measures(
            cube_name="test_cube", measures=sample_cube_measures, dimensions=sample_cube_dimensions
        )

        assert len(metrics) == 3
        assert all(isinstance(m, Metric) for m in metrics)
        assert all(m.metric_id in ["total_count", "avg_amount", "sum_revenue"] for m in metrics)

    def test_from_cube_measures_with_csv(self, sample_cube_measures, sample_cube_dimensions, csv_dataframe):
        """Test the static from_cube_measures method with CSV filtering."""
        with patch("pandas.read_csv", return_value=csv_dataframe):
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
                f.write("dummy csv content")
                csv_path = f.name

            metrics = MetricGeneratorService.from_cube_measures(
                cube_name="test_cube",
                measures=sample_cube_measures,
                dimensions=sample_cube_dimensions,
                csv_file_path=csv_path,
            )

            assert len(metrics) == 3

            # Clean up
            Path(csv_path).unlink()

    def test_prepare_metric_data_for_json(self, metric_generator_service, test_yaml_content):
        """Test preparing metric data for JSON output."""
        metrics = metric_generator_service.from_yaml_content(test_yaml_content)
        metric = metrics[0]

        data = metric_generator_service._prepare_metric_data(metric, include_metric_id=True)

        assert "metric_id" in data
        assert "metadata" in data  # Should be renamed from meta_data
        assert "dimensions" in data
        assert isinstance(data["complexity"], str)  # Should be string for JSON

    def test_prepare_metric_data_for_db(self, metric_generator_service, test_yaml_content):
        """Test preparing metric data for database storage."""
        metrics = metric_generator_service.from_yaml_content(test_yaml_content)
        metric = metrics[0]

        data = metric_generator_service._prepare_metric_data(metric, include_metric_id=False)

        assert "metric_id" not in data
        assert "meta_data" in data  # Should keep original name for DB
        assert "metadata" not in data
        assert "dimensions" not in data  # Should not be included for DB

    def test_parse_csv_file_success(self, metric_generator_service, csv_dataframe):
        """Test parsing CSV file successfully."""
        with patch("pandas.read_csv", return_value=csv_dataframe):
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
                f.write("dummy csv content")
                csv_path = f.name

            csv_map = metric_generator_service._parse_csv_file(csv_path)

            assert len(csv_map) == 3
            assert "total_count" in csv_map
            assert "avg_amount" in csv_map
            assert "sum_revenue" in csv_map

            # Clean up
            Path(csv_path).unlink()

    def test_parse_csv_file_error(self, metric_generator_service):
        """Test parsing CSV file with error."""
        csv_map = metric_generator_service._parse_csv_file("nonexistent_file.csv")
        assert csv_map == {}

    def test_generate_metrics_for_cube_with_time_dimension(self, metric_generator_service):
        """Test generating metrics for cube with time dimension."""
        cube = CubeDefinition(
            name="test_cube",
            measures=[CubeMeasure(name="count", title="Count", type="count", sql="id")],
            dimensions=[CubeDimension(name="created_at", title="Created Date", type="time", sql="created_at")],
        )

        metrics = metric_generator_service._generate_metrics_for_cube(cube)

        assert len(metrics) == 1
        assert metrics[0].metric_id == "count"
        assert metrics[0].meta_data.semantic_meta.time_dimension.member == "created_at"

    def test_generate_metrics_for_cube_no_time_dimension(self, metric_generator_service):
        """Test generating metrics for cube without time dimension."""
        cube = CubeDefinition(
            name="test_cube",
            measures=[CubeMeasure(name="count", title="Count", type="count", sql="id")],
            dimensions=[CubeDimension(name="region", title="Region", type="string", sql="region")],
        )

        metrics = metric_generator_service._generate_metrics_for_cube(cube)

        assert len(metrics) == 1
        assert metrics[0].metric_id == "count"
        # Should use default time dimension
        assert metrics[0].meta_data.semantic_meta.time_dimension.member == "created_at"

    def test_create_semantic_meta_with_filters(self, metric_generator_service):
        """Test creating semantic metadata with filters."""
        cube_name = "test_cube"
        measure = CubeMeasure(
            name="revenue",
            title="Revenue",
            type="sum",
            sql="revenue",
            filters=[{"sql": "{status} = true"}],  # Correct format for measure filters
        )
        time_dimension = SemanticMetaTimeDimension(cube="test_cube", member="created_at")

        semantic_meta = metric_generator_service._create_semantic_meta(cube_name, measure, time_dimension)

        assert semantic_meta.cube == cube_name
        assert semantic_meta.member == "revenue"
        assert len(semantic_meta.cube_filters) == 1
        assert semantic_meta.cube_filters[0].dimension == "test_cube.status"

    def test_parse_filters_string(self, metric_generator_service):
        """Test parsing filter strings."""
        filters_str = "{status} = 'active' AND {category} = 'premium'"  # Use correct format
        cube_name = "test_cube"

        filters = metric_generator_service._parse_filters(filters_str, cube_name)

        assert len(filters) == 2
        assert filters[0].dimension == "test_cube.status"
        assert filters[0].operator == "equals"
        assert filters[0].values == ["active"]

        assert filters[1].dimension == "test_cube.category"
        assert filters[1].operator == "equals"
        assert filters[1].values == ["premium"]

    def test_generate_variant_metric_id(self, metric_generator_service):
        """Test generating variant metric IDs."""
        base_measure = "revenue"
        csv_data = CSVMetricData(
            measure="revenue",
            metric_name="Premium Revenue",
            filters="premium",  # Use a filter that matches keyword patterns
        )
        base_csv_data = CSVMetricData(measure="revenue", metric_name="Total Revenue", filters="")

        variant_id = metric_generator_service._generate_variant_metric_id(base_measure, csv_data, base_csv_data)

        # The method extracts keywords from metric name differences when filters don't match patterns
        assert variant_id.startswith("revenue_")

    def test_extract_filter_keywords(self, metric_generator_service):
        """Test extracting filter keywords."""
        filters_str = "enterprise inbound"  # Simple string with known patterns

        keywords = metric_generator_service._extract_filter_keywords(filters_str)

        # Should extract both 'ent' (from enterprise) and 'inbound'
        assert len(keywords) >= 1
        assert "ent" in keywords or "inbound" in keywords

    def test_get_complexity_static_method(self):
        """Test getting complexity from measure."""
        measure = CubeMeasure(name="count", title="Count", type="count")
        complexity = MetricGeneratorService._get_complexity(measure)
        assert complexity == Complexity.ATOMIC

    def test_get_aim_static_method(self):
        """Test getting aim from measure."""
        measure = CubeMeasure(name="count", title="Count", type="count")
        aim = MetricGeneratorService._get_aim(measure)
        assert aim == MetricAim.MAXIMIZE

    def test_get_aggregation_type_static_method(self):
        """Test getting aggregation type from cube type."""
        assert MetricGeneratorService._get_aggregation_type("count") == "sum"
        assert MetricGeneratorService._get_aggregation_type("sum") == "sum"
        assert MetricGeneratorService._get_aggregation_type("avg") == "avg"
        assert MetricGeneratorService._get_aggregation_type("max") == "max"
        assert MetricGeneratorService._get_aggregation_type("min") == "min"
        assert MetricGeneratorService._get_aggregation_type("unknown") == "sum"

    def test_get_unit_of_measure_static_method(self):
        """Test getting unit of measure from measure."""
        measure = CubeMeasure(name="count", title="Count", type="count")
        unit = MetricGeneratorService._get_unit_of_measure(measure)
        assert unit == "Quantity"

        measure = CubeMeasure(name="revenue", title="Revenue", type="sum", format="currency")
        unit = MetricGeneratorService._get_unit_of_measure(measure)
        assert unit == "Currency"

    def test_get_unit_static_method(self):
        """Test getting unit from measure."""
        measure = CubeMeasure(name="count", title="Count", type="count")
        unit = MetricGeneratorService._get_unit(measure)
        assert unit == "n"

        measure = CubeMeasure(name="revenue", title="Revenue", type="sum", format="currency")
        unit = MetricGeneratorService._get_unit(measure)
        assert unit == "$"


class TestCSVMetricData:
    """Test cases for CSVMetricData model."""

    def test_from_csv_row_complete_data(self):
        """Test creating CSVMetricData from complete CSV row."""
        row = {
            "Measure": "total_count",
            "Metric Name": "Total Count",
            "Definition": "Total count of records",
            "Canonical Cube": "test_cube",
            "Anchor Date": "created_at",
            "Filters": "status = 'active'",
            "Owning Team(s)": "Engineering",
        }

        csv_data = CSVMetricData.from_csv_row(row)

        assert csv_data.measure == "total_count"
        assert csv_data.metric_name == "Total Count"
        assert csv_data.definition == "Total count of records"
        assert csv_data.cube == "test_cube"
        assert csv_data.anchor_date == "created_at"
        assert csv_data.filters == "status = 'active'"
        assert csv_data.owning_team == "Engineering"

    def test_from_csv_row_with_nan_values(self):
        """Test creating CSVMetricData from CSV row with NaN values."""
        import pandas as pd

        row = {
            "Measure": "total_count",
            "Metric Name": pd.NA,
            "Definition": "",
            "Canonical Cube": "test_cube",
            "Anchor Date": "created_at",
            "Filters": pd.NA,
            "Owning Team(s)": "",
        }

        csv_data = CSVMetricData.from_csv_row(row)

        assert csv_data.measure == "total_count"
        assert csv_data.metric_name is None
        assert csv_data.definition is None
        assert csv_data.cube == "test_cube"
        assert csv_data.anchor_date == "created_at"
        assert csv_data.filters is None
        assert csv_data.owning_team is None

    def test_from_csv_row_empty_measure(self):
        """Test creating CSVMetricData from CSV row with empty measure."""
        row = {
            "Measure": "",
            "Metric Name": "Some Metric",
            "Definition": "Some definition",
            "Canonical Cube": "test_cube",
            "Anchor Date": "created_at",
            "Filters": "",
            "Owning Team(s)": "Engineering",
        }

        csv_data = CSVMetricData.from_csv_row(row)

        assert csv_data.measure == ""
        assert csv_data.metric_name == "Some Metric"


class TestCubeModels:
    """Test cases for cube model classes."""

    def test_cube_measure_creation(self):
        """Test creating CubeMeasure."""
        measure = CubeMeasure(
            name="count",
            title="Count",
            short_title="Count",
            type="count",
            sql="id",
            description="Count of records",
            format="number",
            filters=[{"dimension": "status", "operator": "equals", "values": ["active"]}],
        )

        assert measure.name == "count"
        assert measure.title == "Count"
        assert measure.type == "count"
        assert len(measure.filters) == 1

    def test_cube_dimension_creation(self):
        """Test creating CubeDimension."""
        dimension = CubeDimension(
            name="created_at",
            title="Created Date",
            type="time",
            sql="created_at",
            description="Date when record was created",
        )

        assert dimension.name == "created_at"
        assert dimension.title == "Created Date"
        assert dimension.type == "time"

    def test_cube_definition_creation(self):
        """Test creating CubeDefinition."""
        measures = [CubeMeasure(name="count", type="count")]
        dimensions = [CubeDimension(name="created_at", type="time")]

        cube = CubeDefinition(name="test_cube", public=True, measures=measures, dimensions=dimensions)

        assert cube.name == "test_cube"
        assert cube.public is True
        assert len(cube.measures) == 1
        assert len(cube.dimensions) == 1
