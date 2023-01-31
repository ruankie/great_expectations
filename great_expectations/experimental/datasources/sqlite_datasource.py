from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Dict, List, Optional, Type, TypeVar, Union

import pydantic
from pydantic import constr
from pydantic import dataclasses as pydantic_dc
from typing_extensions import ClassVar

from great_expectations.experimental.datasources.interfaces import DataAsset

if TYPE_CHECKING:
    import sqlalchemy

    # The SqliteConnectionString type is defined this way because mypy can't handle
    # constraint types. See: https://github.com/pydantic/pydantic/issues/156
    # which suggests the solution here:
    # https://github.com/pydantic/pydantic/issues/975#issuecomment-551147305
    SqliteConnectionString = str
else:
    SqliteConnectionString = constr(regex=r"^sqlite")

from typing_extensions import Literal

from great_expectations.experimental.datasources.sql_datasource import (
    BatchSortersDefinition,
    ColumnSplitter,
    DatetimeRange,
    QueryAsset,
    SqlAsset,
    SQLDatasource,
    SQLDatasourceError,
    TableAsset,
    _query_for_year_and_month,
)


def _add_year_and_month_splitter(
    asset: SqliteAssetType, column_name: str
) -> SqliteAssetType:
    """Associates a year month splitter with a sqlite data asset

    Args:
        column_name: A column name of the date column where year and month will be parsed out.

    Returns:
        This passed in asset so operations on the asset can be composed.
    """
    asset.column_splitter = SqliteYearMonthSplitter(
        column_name=column_name,
    )
    return asset


class SqliteTableAsset(TableAsset):
    # Subclass overrides
    type: Literal["sqlite_table"] = "sqlite_table"  # type: ignore[assignment]
    column_splitter: Optional[SqliteYearMonthSplitter] = None  # type: ignore[assignment]

    def add_year_and_month_splitter(
        self,
        column_name: str,
    ) -> SqliteTableAsset:
        """Associates a year month splitter with this sqlite table data asset

        Args:
            column_name: A column name of the date column where year and month will be parsed out.

        Returns:
            This SqliteTableAsset so we can use this method fluently.
        """
        return _add_year_and_month_splitter(self, column_name)


class SqliteQueryAsset(QueryAsset):
    # Subclass overrides
    type: Literal["sqlite_query"] = "sqlite_query"  # type: ignore[assignment]
    column_splitter: Optional[SqliteYearMonthSplitter] = None  # type: ignore[assignment]

    def add_year_and_month_splitter(
        self,
        column_name: str,
    ) -> SqliteQueryAsset:
        """Associates a year month splitter with this sqlite table data asset

        Args:
            column_name: A column name of the date column where year and month will be parsed out.

        Returns:
            This SqliteQueryAsset so we can use this method fluently.
        """
        return _add_year_and_month_splitter(self, column_name)


@pydantic_dc.dataclass(frozen=True)
class SqliteYearMonthSplitter(ColumnSplitter):
    method_name: Literal["split_on_year_and_month"] = "split_on_year_and_month"
    # noinspection Pydantic
    param_names: List[Literal["year", "month"]] = pydantic.Field(
        default_factory=lambda: ["year", "month"]
    )

    def param_defaults(self, sql_asset: SqlAsset) -> Dict[str, List]:
        """Query sqlite database to get the years and months to split over.

        Args:
            sql_asset: A Sqlite*Asset over which we want to split the data.
        """
        if not isinstance(sql_asset, tuple(_SqliteAssets)):
            raise SQLDatasourceError(
                "SQL asset passed to SqliteYearMonthSplitter is not a Sqlite*Asset. It is "
                f"{sql_asset}"
            )

        return _query_for_year_and_month(
            sql_asset, self.column_name, _get_sqlite_datetime_range
        )


def _get_sqlite_datetime_range(
    conn: sqlalchemy.engine.base.Connection, table_name: str, col_name: str
) -> DatetimeRange:
    q = f"select STRFTIME('%Y%m%d', min({col_name})), STRFTIME('%Y%m%d', max({col_name})) from {table_name}"
    min_max_dt = [datetime.strptime(dt, "%Y%m%d") for dt in list(conn.execute(q))[0]]
    return DatetimeRange(min=min_max_dt[0], max=min_max_dt[1])


_SqliteAssets: List[Type[DataAsset]] = [SqliteTableAsset, SqliteQueryAsset]
# Unfortunately the following types can't be derived from _SqliteAssets above because mypy doesn't
# support programmatically unrolling this list, eg Union[*_SqliteAssets] is not supported.
SqliteAssetTypes = Union[SqliteTableAsset, SqliteQueryAsset]
SqliteAssetType = TypeVar("SqliteAssetType", SqliteTableAsset, SqliteQueryAsset)


class SqliteDatasource(SQLDatasource):
    """Adds a sqlite datasource to the data context.

    Args:
        name: The name of this sqlite datasource
        connection_string: The SQLAlchemy connection string used to connect to the sqlite database.
            For example: "sqlite:///path/to/file.db"
        assets: An optional dictionary whose keys are TableAsset names and whose values
            are TableAsset objects.
    """

    # class var definitions
    asset_types: ClassVar[List[Type[DataAsset]]] = _SqliteAssets

    # Subclass instance var overrides
    # right side of the operator determines the type name
    # left side enforces the names on instance creation
    type: Literal["sqlite"] = "sqlite"  # type: ignore[assignment]
    connection_string: SqliteConnectionString
    assets: Dict[str, SqliteAssetTypes] = {}  # type: ignore[assignment]

    def add_table_asset(
        self,
        name: str,
        table_name: str,
        order_by: Optional[BatchSortersDefinition] = None,
    ) -> SqliteTableAsset:
        """Adds a sqlite table asset to this sqlite datasource.

        Args:
            name: The name of this table asset.
            table_name: The table where the data resides.
            order_by: A list of BatchSorters or BatchSorter strings.

        Returns:
            The SqliteTableAsset that is added to the datasource.
        """
        asset = SqliteTableAsset(
            name=name,
            table_name=table_name,
            order_by=order_by or [],  # type: ignore[arg-type]  # coerce list[str]
            # see TableAsset._parse_order_by_sorter()
        )
        asset._datasource = self
        self.assets[name] = asset
        return asset

    def add_query_asset(
        self,
        name: str,
        query: str,
        order_by: Optional[BatchSortersDefinition] = None,
    ) -> SqliteQueryAsset:
        """Adds a sqlite query asset to this sqlite datasource.

        Args:
            name: The name of this table asset.
            query: The SELECT query to selects the data to validate. It must begin with the "SELECT".
            order_by: A list of BatchSorters or BatchSorter strings.

        Returns:
            The SqliteTableAsset that is added to the datasource.
        """
        asset = SqliteQueryAsset(
            name=name,
            query=query,
            order_by=order_by or [],  # type: ignore[arg-type]  # coerce list[str]
            # see TableAsset._parse_order_by_sorter()
        )
        asset._datasource = self
        self.assets[name] = asset
        return asset
