import pathlib

import pytest
from pydantic import ValidationError

from great_expectations.experimental.datasources import SqliteDatasource


@pytest.fixture
def sqlite_datasource_name() -> str:
    return "sqlite_datasource"


@pytest.fixture
def sqlite_database_path() -> pathlib.Path:
    return (
        pathlib.Path(__file__)
        / "../../../test_sets/taxi_yellow_tripdata_samples/sqlite/yellow_tripdata.db"
    )


@pytest.fixture
def sqlite_datasource(sqlite_database_path, sqlite_datasource_name) -> SqliteDatasource:
    connection_string = f"sqlite:///{sqlite_database_path}"
    datasource = SqliteDatasource(
        name=sqlite_datasource_name,
        connection_string=connection_string,
    )
    return datasource


@pytest.mark.unit
def test_connection_string_starts_with_sqlite(
    sqlite_datasource, sqlite_database_path, sqlite_datasource_name
):
    # The actual file doesn't matter only it's existence since SqlAlchemy does a check
    # when it creates the database engine.
    assert sqlite_datasource.name == sqlite_datasource_name
    assert sqlite_datasource.connection_string == f"sqlite:///{sqlite_database_path}"


@pytest.mark.unit
def test_connection_string_that_does_not_start_with_sqlite():
    name = "sqlite_datasource"
    connection_string = "stuff+sqlite:///path/to/database/file.db"
    with pytest.raises(ValidationError):
        SqliteDatasource(
            name=name,
            connection_string=connection_string,
        )


@pytest.mark.unit
def test_non_select_query_asset(sqlite_datasource):
    with pytest.raises(ValueError):
        sqlite_datasource.add_query_asset(name="query_asset", query="* from table")
