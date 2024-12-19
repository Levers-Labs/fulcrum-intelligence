import pytest

from commons.models.tenant import CubeConnectionConfig, Tenant, TenantConfig


def test_tenant_model():
    # Test valid tenant creation
    tenant = Tenant(
        identifier="test-tenant",
        name="Test Tenant",
        description="Test Description",
        domains=["test.com"],
        external_id="ext123",
    )
    assert tenant.identifier == "test-tenant"
    assert tenant.name == "Test Tenant"
    assert tenant.description == "Test Description"
    assert tenant.domains == ["test.com"]
    assert tenant.external_id == "ext123"


def test_cube_connection_config():
    # Test valid token auth config
    config = CubeConnectionConfig(
        cube_api_url="http://cube.example.com", cube_auth_type="TOKEN", cube_auth_token="test-token"  # noqa
    )
    assert config.cube_api_url == "http://cube.example.com"
    assert config.cube_auth_type == "TOKEN"
    assert config.cube_auth_token == "test-token"  # noqa

    # Test valid secret key auth config
    config = CubeConnectionConfig(
        cube_api_url="http://cube.example.com", cube_auth_type="SECRET_KEY", cube_auth_secret_key="test-secret"  # noqa
    )
    assert config.cube_api_url == "http://cube.example.com"
    assert config.cube_auth_type == "SECRET_KEY"
    assert config.cube_auth_secret_key == "test-secret"  # noqa


def test_tenant_config_validation():
    # Test valid token auth config
    config = TenantConfig(
        cube_connection_config=CubeConnectionConfig(
            cube_api_url="http://cube.example.com", cube_auth_type="TOKEN", cube_auth_token="test-token"  # noqa
        ),
        is_stories_enabled=True,
    )
    assert config.cube_connection_config.cube_auth_type == "TOKEN"
    assert config.cube_connection_config.cube_auth_token == "test-token"  # noqa

    # Test missing token validation
    with pytest.raises(ValueError, match="cube_auth_token is required when auth_type is TOKEN"):
        TenantConfig(
            cube_connection_config=CubeConnectionConfig(cube_api_url="http://cube.example.com", cube_auth_type="TOKEN")
        )

    # Test missing secret key validation
    with pytest.raises(ValueError, match="cube_auth_secret_key is required when auth_type is SECRET_KEY"):
        TenantConfig(
            cube_connection_config=CubeConnectionConfig(
                cube_api_url="http://cube.example.com", cube_auth_type="SECRET_KEY"
            )
        )
