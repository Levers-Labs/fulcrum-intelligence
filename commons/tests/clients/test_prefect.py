import pytest
from pydantic import BaseModel

from commons.clients.base import HttpClientError
from commons.clients.prefect import (
    PrefectClient,
    PrefectDeployment,
    PrefectSchedule,
    Schedule,
)


class TestParameterSchema(BaseModel):
    """Test schema for deployment parameters."""

    tenant_id: str
    report_id: str


@pytest.fixture
def prefect_client():
    """Create a PrefectClient instance for testing."""
    return PrefectClient(api_url="https://api.prefect.cloud/api", api_key="test-api-key")


@pytest.fixture
def sample_deployment():
    """Create a sample PrefectDeployment for testing."""
    return PrefectDeployment(
        name="test-deployment",
        flow_name="test-flow",
        work_pool_name="test-pool",
        entrypoint="test.flows:test_flow",
        parameters={"tenant_id": "test-tenant", "report_id": "test-report"},
    )


def test_get_flow_by_name_success(prefect_client, mocker):
    """Test successful flow retrieval by name."""
    expected_response = {"id": "test-flow-id", "name": "test-flow"}
    mock_get = mocker.Mock(return_value=expected_response)
    mocker.patch.object(prefect_client, "get", mock_get)

    result = prefect_client.get_flow_by_name("test-flow")

    assert result == expected_response
    mock_get.assert_called_once_with("flows/name/test-flow")


def test_create_flow_success(prefect_client, mocker):
    """Test successful flow creation."""
    expected_response = {"id": "new-flow-id", "name": "test-flow"}
    mock_post = mocker.Mock(return_value=expected_response)
    mocker.patch.object(prefect_client, "post", mock_post)

    result = prefect_client.create_flow("test-flow", tags=["tag1"])

    assert result == expected_response
    mock_post.assert_called_once_with("/flows/", data={"name": "test-flow", "tags": ["tag1"]})


def test_get_or_create_flow_existing(prefect_client, mocker):
    """Test get_or_create_flow when flow exists."""
    expected_response = {"id": "existing-flow-id", "name": "test-flow"}
    mock_get = mocker.MagicMock(return_value=expected_response)
    mock_create = mocker.MagicMock()
    mocker.patch.object(prefect_client, "get_flow_by_name", mock_get)
    mocker.patch.object(prefect_client, "create_flow", mock_create)

    result = prefect_client.get_or_create_flow("test-flow")

    assert result == expected_response
    mock_get.assert_called_once_with("test-flow")
    mock_create.assert_not_called()


def test_get_or_create_flow_new(prefect_client, mocker):
    """Test get_or_create_flow when flow doesn't exist."""
    mock_get = mocker.MagicMock(side_effect=HttpClientError(status_code=404, message="Flow not found"))
    expected_response = {"id": "new-flow-id", "name": "test-flow"}
    mock_create = mocker.MagicMock(return_value=expected_response)
    mocker.patch.object(prefect_client, "get_flow_by_name", mock_get)
    mocker.patch.object(prefect_client, "create_flow", mock_create)

    result = prefect_client.get_or_create_flow("test-flow")

    assert result == expected_response
    mock_get.assert_called_once_with("test-flow")
    mock_create.assert_called_once_with("test-flow")


def test_create_deployment_success(prefect_client, sample_deployment, mocker):
    """Test successful deployment creation."""
    flow_response = {"id": "test-flow-id", "name": "test-flow"}
    deployment_response = {"id": "test-deployment-id", "name": "test-deployment"}

    mock_get_flow = mocker.MagicMock(return_value=flow_response)
    mock_post = mocker.MagicMock(return_value=deployment_response)

    mocker.patch.object(prefect_client, "get_or_create_flow", mock_get_flow)
    mocker.patch.object(prefect_client, "post", mock_post)

    result = prefect_client.create_deployment(sample_deployment)

    assert result == deployment_response
    mock_get_flow.assert_called_once_with(sample_deployment.flow_name)

    # Verify deployment data transformation
    call_args = mock_post.call_args[1]["data"]
    assert "flow_name" not in call_args
    assert call_args["flow_id"] == "test-flow-id"


def test_create_deployment_flow_not_found(prefect_client, sample_deployment, mocker):
    """Test deployment creation when flow is not found."""
    mock_get_flow = mocker.MagicMock(return_value=None)
    mocker.patch.object(prefect_client, "get_or_create_flow", mock_get_flow)

    with pytest.raises(ValueError, match=f"Flow {sample_deployment.flow_name} not found"):
        prefect_client.create_deployment(sample_deployment)


def test_delete_deployment_success(prefect_client, mocker):
    """Test successful deployment deletion."""
    expected_response = {"id": "test-deployment-id", "status": "deleted"}
    mock_delete = mocker.MagicMock(return_value=expected_response)
    mocker.patch.object(prefect_client, "delete", mock_delete)

    result = prefect_client.delete_deployment("test-deployment-id")

    assert result == expected_response
    mock_delete.assert_called_once_with("/deployments/test-deployment-id")


def test_get_or_create_flow_other_error(prefect_client, mocker):
    """Test get_or_create_flow with non-404 error."""
    mock_get = mocker.MagicMock(side_effect=HttpClientError(status_code=500, message="Internal server error"))
    mock_create = mocker.MagicMock()
    mocker.patch.object(prefect_client, "get_flow_by_name", mock_get)
    mocker.patch.object(prefect_client, "create_flow", mock_create)

    with pytest.raises(HttpClientError):
        prefect_client.get_or_create_flow("test-flow")

    mock_get.assert_called_once_with("test-flow")
    mock_create.assert_not_called()


def test_create_deployment_with_defaults_basic():
    """Test creating deployment with basic parameters."""
    deployment = PrefectDeployment.create_with_defaults(
        name="test-deployment", flow_name="test-flow", entrypoint="test.flows:test_flow"
    )

    assert deployment.name == "test-deployment"
    assert deployment.flow_name == "test-flow"
    assert deployment.entrypoint == "test.flows:test_flow"
    assert deployment.work_pool_name == "tasks-manager-managed-pool"
    assert deployment.schedules is None


def test_create_deployment_with_defaults_schedule():
    """Test creating deployment with schedule."""
    deployment = PrefectDeployment.create_with_defaults(
        name="test-deployment",
        flow_name="test-flow",
        entrypoint="test.flows:test_flow",
        schedule="0 0 * * *",
        timezone="UTC",
        is_active=True,
    )

    assert deployment.schedules is not None
    assert len(deployment.schedules) == 1
    assert deployment.schedules[0].schedule.cron == "0 0 * * *"
    assert deployment.schedules[0].schedule.timezone == "UTC"
    assert deployment.schedules[0].active is True


def test_create_deployment_with_defaults_parameter_schema():
    """Test creating deployment with parameter schema."""
    deployment = PrefectDeployment.create_with_defaults(
        name="test-deployment",
        flow_name="test-flow",
        entrypoint="test.flows:test_flow",
        parameter_schema=TestParameterSchema,
    )

    assert deployment.parameter_openapi_schema is not None
    assert "properties" in deployment.parameter_openapi_schema
    assert "tenant_id" in deployment.parameter_openapi_schema["properties"]
    assert "report_id" in deployment.parameter_openapi_schema["properties"]


def test_schedule_model_basic():
    """Test Schedule model creation with basic parameters."""
    schedule = Schedule(cron="0 0 * * *")

    assert schedule.cron == "0 0 * * *"
    assert schedule.timezone is None
    assert schedule.day_or is True


def test_schedule_model_full():
    """Test Schedule model creation with all parameters."""
    schedule = Schedule(cron="0 0 * * *", timezone="UTC", day_or=False)

    assert schedule.cron == "0 0 * * *"
    assert schedule.timezone == "UTC"
    assert schedule.day_or is False


def test_prefect_schedule_model_defaults():
    """Test PrefectSchedule model with default values."""
    schedule = Schedule(cron="0 0 * * *")
    prefect_schedule = PrefectSchedule(schedule=schedule)

    assert prefect_schedule.active is True
    assert prefect_schedule.schedule == schedule


def test_prefect_schedule_model_custom():
    """Test PrefectSchedule model with custom values."""
    schedule = Schedule(cron="0 0 * * *", timezone="UTC")
    prefect_schedule = PrefectSchedule(schedule=schedule, active=False)

    assert prefect_schedule.active is False
    assert prefect_schedule.schedule == schedule


def test_read_deployment_schedules_success(prefect_client, mocker):
    """Test successful retrieval of deployment schedules."""
    expected_response = [{"id": "schedule-1", "active": True}, {"id": "schedule-2", "active": False}]
    mock_get = mocker.Mock(return_value=expected_response)
    mocker.patch.object(prefect_client, "get", mock_get)

    result = prefect_client.read_deployment_schedules("test-deployment-id")

    assert result == expected_response
    mock_get.assert_called_once_with("/deployments/test-deployment-id/schedules")


def test_update_deployment_schedule_success(prefect_client, mocker):
    """Test successful update of deployment schedule."""
    schedule_data = {
        "active": False,
        "schedule": {"cron": "0 0 * * *", "timezone": "UTC", "day_or": True},
    }
    mock_request = mocker.Mock()
    mocker.patch.object(prefect_client, "_make_request", mock_request)

    prefect_client.update_deployment_schedule("test-deployment-id", "schedule-id", schedule_data)

    mock_request.assert_called_once_with(
        "PATCH", "/deployments/test-deployment-id/schedules/schedule-id", json=schedule_data
    )
