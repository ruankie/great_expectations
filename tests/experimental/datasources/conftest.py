import logging
from datetime import datetime
from typing import Callable, Tuple

from great_expectations.execution_engine import SqlAlchemyExecutionEngine

LOGGER = logging.getLogger(__name__)

from contextlib import contextmanager

from sqlalchemy.sql.compiler import SQLCompiler

from great_expectations.core.batch import BatchData
from great_expectations.core.batch_spec import (
    BatchMarkers,
    SqlAlchemyDatasourceBatchSpec,
)

# This is the default min/max time that we are using in our mocks.
# They are made global so our tests can reference them directly.
DEFAULT_MIN_DT = datetime(2021, 1, 1, 0, 0, 0)
DEFAULT_MAX_DT = datetime(2022, 12, 31, 0, 0, 0)


class Dialect:
    def __init__(self, dialect: str):
        self.name = dialect

    def statement_compiler(dialect, **kwargs) -> SQLCompiler:
        return SQLCompiler()


class _MockConnection:
    def __init__(self, dialect: Dialect):
        self.dialect = dialect

    def execute(self, query):
        """Execute a query over a sqlalchemy engine connection.

        Currently this mock assumes the query is always of the form:
        "select min(col), max(col) from table"
        where col is a datetime column since that's all that's necessary.
        This can be generalized if needed.

        Args:
            query: The SQL query to execute.
        """
        return [(DEFAULT_MIN_DT, DEFAULT_MAX_DT)]


class MockSaEngine:
    def __init__(self, dialect: Dialect):
        self.dialect = dialect

    @contextmanager
    def connect(self):
        """A contextmanager that yields a _MockConnection"""
        yield _MockConnection(self.dialect)


def sa_engine_mock(dialect: str) -> MockSaEngine:
    return MockSaEngine(dialect=Dialect(dialect))


def sqlachemy_execution_engine_mock_cls(
    validate_batch_spec: Callable[[SqlAlchemyDatasourceBatchSpec], None], dialect: str
):
    """Creates a mock gx sql alchemy engine class

    Args:
        validate_batch_spec: A hook that can be used to validate the generated the batch spec
            passed into get_batch_data_and_markers
        dialect: A string representing the SQL Engine dialect. Examples include: postgresql, sqlite
    """

    class MockSqlAlchemyExecutionEngine(SqlAlchemyExecutionEngine):
        def __init__(self, *args, **kwargs):
            # We should likely let the user pass in an engine. In a SqlAlchemyExecutionEngine used in
            # non-mocked code the engine property is of the type:
            # from sqlalchemy.engine import Engine as SaEngine
            self.engine = MockSaEngine(dialect=Dialect(dialect))

        def get_batch_data_and_markers(  # type: ignore[override]
            self, batch_spec: SqlAlchemyDatasourceBatchSpec
        ) -> Tuple[BatchData, BatchMarkers]:
            validate_batch_spec(batch_spec)
            return BatchData(self), BatchMarkers(ge_load_time=None)

    return MockSqlAlchemyExecutionEngine


class ExecutionEngineDouble:
    def __init__(self, *args, **kwargs):
        pass

    def get_batch_data_and_markers(self, batch_spec) -> Tuple[BatchData, BatchMarkers]:
        return BatchData(self), BatchMarkers(ge_load_time=None)
