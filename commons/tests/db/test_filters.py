import pytest
from sqlalchemy import select

from commons.db.filters import BaseFilter, FilterField
from commons.db.models import BaseDBModel


# Sample SQLAlchemy model
class User(BaseDBModel, table=True):
    name: str
    age: int


# Sample filter model
class UserFilter(BaseFilter[User]):
    id: int | None = FilterField(User.id)  # type: ignore
    name: str | None = FilterField(User.name, operator="ilike")  # type: ignore
    age_gt: int | None = FilterField(User.age, operator="gt")  # type: ignore
    age_lt: int | None = FilterField(User.age, operator="lt")  # type: ignore


# Pytest fixtures
@pytest.fixture
def user_table():
    return User.__table__


@pytest.fixture
def user_query(user_table):
    return select(user_table)


# Test cases
def test_get_filter_function_eq():
    field = FilterField(User.id)
    assert field.operator == "eq"
    query = field.get_filter_function()(select(User), 1)
    expected_query = 'SELECT "user".id, "user".name, "user".age FROM "user" WHERE "user".id = :id_1'
    assert str(query).replace("\n", "") == expected_query


def test_get_filter_function_ne():
    field = FilterField(User.id, operator="ne")
    assert field.operator == "ne"
    query = field.get_filter_function()(select(User), 1)
    expected_query = 'SELECT "user".id, "user".name, "user".age FROM "user" WHERE "user".id != :id_1'
    assert str(query).replace("\n", "") == expected_query


def test_get_filter_function_lt():
    field = FilterField(User.age, operator="lt")
    assert field.operator == "lt"
    query = field.get_filter_function()(select(User), 18)
    assert (
        str(query).replace("\n", "")
        == 'SELECT "user".id, "user".name, "user".age FROM "user" WHERE "user".age < :age_1'
    )


def test_get_filter_function_le():
    field = FilterField(User.age, operator="le")
    assert field.operator == "le"
    query = field.get_filter_function()(select(User), 18)
    assert (
        str(query).replace("\n", "")
        == 'SELECT "user".id, "user".name, "user".age FROM "user" WHERE "user".age <= :age_1'
    )


def test_get_filter_function_gt():
    field = FilterField(User.age, operator="gt")
    assert field.operator == "gt"
    query = field.get_filter_function()(select(User), 18)
    assert (
        str(query).replace("\n", "")
        == 'SELECT "user".id, "user".name, "user".age FROM "user" WHERE "user".age > :age_1'
    )


def test_get_filter_function_ge():
    field = FilterField(User.age, operator="ge")
    assert field.operator == "ge"
    query = field.get_filter_function()(select(User), 18)
    assert (
        str(query).replace("\n", "")
        == 'SELECT "user".id, "user".name, "user".age FROM "user" WHERE "user".age >= :age_1'
    )


def test_get_filter_function_like():
    field = FilterField(User.name, operator="like")
    assert field.operator == "like"
    query = field.get_filter_function()(select(User), "%john%")
    assert (
        str(query).replace("\n", "")
        == 'SELECT "user".id, "user".name, "user".age FROM "user" WHERE "user".name LIKE :name_1'
    )


def test_get_filter_function_ilike():
    field = FilterField(User.name, operator="ilike")
    assert field.operator == "ilike"
    query = field.get_filter_function()(select(User), "%john%")
    assert (
        str(query).replace("\n", "")
        == 'SELECT "user".id, "user".name, "user".age FROM "user" WHERE lower("user".name) LIKE lower(:name_1)'
    )


def test_get_filter_function_in():
    field = FilterField(User.id, operator="in")
    assert field.operator == "in"
    query = field.get_filter_function()(select(User), [1, 2, 3])
    assert (
        str(query).replace("\n", "")
        == 'SELECT "user".id, "user".name, "user".age FROM "user" WHERE "user".id IN (__[POSTCOMPILE_id_1])'
    )


def test_get_filter_function_not_in():
    field = FilterField(User.id, operator="not_in")
    assert field.operator == "not_in"
    query = field.get_filter_function()(select(User), [1, 2, 3])
    assert (
        str(query).replace("\n", "")
        == 'SELECT "user".id, "user".name, "user".age FROM "user" WHERE ("user".id NOT IN (__[POSTCOMPILE_id_1]))'
    )


def test_get_filter_function_between():
    field = FilterField(User.age, operator="between")
    assert field.operator == "between"
    query = field.get_filter_function()(select(User), (18, 30))
    assert (
        str(query).replace("\n", "")
        == 'SELECT "user".id, "user".name, "user".age FROM "user" WHERE "user".age BETWEEN :age_1 AND :age_2'
    )


def test_get_filter_function_not_between():
    field = FilterField(User.age, operator="not_between")
    assert field.operator == "not_between"
    query = field.get_filter_function()(select(User), (18, 30))
    assert (
        str(query).replace("\n", "")
        == 'SELECT "user".id, "user".name, "user".age FROM "user" WHERE "user".age NOT BETWEEN :age_1 AND :age_2'
    )


def test_get_filter_function_is():
    field = FilterField(User.name, operator="is")
    assert field.operator == "is"
    query = field.get_filter_function()(select(User), "test")
    expected_query = 'SELECT "user".id, "user".name, "user".age FROM "user" WHERE "user".name IS :name_1'
    assert str(query).replace("\n", "") == expected_query


def test_get_filter_function_value_is_none():
    field = FilterField(User.name, operator="is")
    assert field.operator == "is"
    query = field.get_filter_function()(select(User), None)
    expected_query = 'SELECT "user".id, "user".name, "user".age FROM "user"'
    assert str(query).replace("\n", "") == expected_query


def test_get_filter_function_is_not():
    field = FilterField(User.name, operator="is_not")
    assert field.operator == "is_not"
    query = field.get_filter_function()(select(User), "test")
    expected_query = 'SELECT "user".id, "user".name, "user".age FROM "user" WHERE "user".name IS NOT :name_1'
    assert str(query).replace("\n", "") == expected_query


def test_get_filter_function_unsupported_operator():
    field = FilterField(User.id, operator="unsupported")
    with pytest.raises(ValueError) as exc_info:
        field.get_filter_function()(select(User), 1)
    assert str(exc_info.value) == "Unsupported operator: unsupported"


def test_apply_filter():
    field = FilterField(User.id)
    query = select(User)
    filtered_query = field.apply_filter(query, 1)
    expected_query = 'SELECT "user".id, "user".name, "user".age FROM "user" WHERE "user".id = :id_1'
    assert str(filtered_query).replace("\n", "") == expected_query


def test_apply_filters(user_query):
    filters = UserFilter(id=1, name="%john%", age_gt=18, age_lt=30)
    filtered_query = UserFilter.apply_filters(user_query, filters.dict(exclude_unset=True))
    expected_query = (
        'SELECT "user".id, "user".name, "user".age FROM "user" WHERE "user".id = '
        ':id_1 AND lower("user".name) LIKE lower(:name_1) AND "user".age > :age_1 AND "user".age < :age_2'
    )
    assert str(filtered_query).replace("\n", "") == expected_query


def test_apply_filters_with_multiple_filters(user_query):
    filters = UserFilter(id=1, name="%john%", age_gt=18, age_lt=30)
    filtered_query = UserFilter.apply_filters(user_query, filters.dict(exclude_unset=True))
    expected_query = (
        'SELECT "user".id, "user".name, "user".age FROM "user" '
        'WHERE "user".id = :id_1 AND lower("user".name) LIKE lower(:name_1) '
        'AND "user".age > :age_1 AND "user".age < :age_2'
    )
    assert str(filtered_query).replace("\n", "") == expected_query
