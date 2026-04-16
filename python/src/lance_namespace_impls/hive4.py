"""
Lance Hive4 Namespace implementation using Hive 4.x Metastore.

This module provides integration with Apache Hive 4.x Metastore for managing
Lance tables. Hive4 supports a 3-level namespace hierarchy:
catalog > database > table.
"""

import logging
import os
from typing import List, Optional

from lance.namespace import LanceNamespace
from lance_namespace_urllib3_client.models import (
    CreateNamespaceRequest,
    CreateNamespaceResponse,
    DeclareTableRequest,
    DeclareTableResponse,
    DeregisterTableRequest,
    DeregisterTableResponse,
    DescribeNamespaceRequest,
    DescribeNamespaceResponse,
    DescribeTableRequest,
    DescribeTableResponse,
    DropNamespaceRequest,
    DropNamespaceResponse,
    DropTableRequest,
    DropTableResponse,
    ListNamespacesRequest,
    ListNamespacesResponse,
    ListTablesRequest,
    ListTablesResponse,
)

from lance_namespace_impls.hive3 import (
    DEFAULT_CATALOG,
    EXTERNAL_TABLE,
    FieldSchema,
    HIVE_AVAILABLE,
    HIVE_METASTORE_SASL_ENABLED_KEY,
    Hive3MetastoreClientWrapper,
    HiveDatabase,
    HiveTable,
    NoSuchObjectException,
    AlreadyExistsException,
    SerDeInfo,
    StorageDescriptor,
    TABLE_TYPE_KEY,
    LANCE_TABLE_FORMAT,
    MANAGED_BY_KEY,
    VERSION_KEY,
    _as_bool,
    _get_kerberos_service_name,
)
from lance_namespace_impls.rest_client import InvalidInputException

try:
    from thrift_files.libraries.thrift_hive_metastore_client.ttypes import (
        Catalog,
        GetTableRequest,
        GetTablesRequest,
    )

    HIVE4_REQUESTS_AVAILABLE = True
except ImportError:
    Catalog = None
    GetTableRequest = None
    GetTablesRequest = None
    HIVE4_REQUESTS_AVAILABLE = False

logger = logging.getLogger(__name__)

_CATALOG_DB_THRIFT_NAME_MARKER = "@"
_CATALOG_DB_SEPARATOR = "#"
_DB_EMPTY_MARKER = "!"


def _prepend_catalog_to_db_name(
    catalog_name: Optional[str], db_name: Optional[str]
) -> str:
    """Encode catalog + database for old Hive thrift APIs without catName."""
    catalog_name = catalog_name or DEFAULT_CATALOG
    parts = [
        _CATALOG_DB_THRIFT_NAME_MARKER,
        catalog_name,
        _CATALOG_DB_SEPARATOR,
    ]
    if db_name is None:
        return "".join(parts)
    if db_name == "":
        return "".join(parts) + _DB_EMPTY_MARKER
    return "".join(parts) + db_name


class Hive4MetastoreClientWrapper(Hive3MetastoreClientWrapper):
    """Helper class to manage Hive 4.x Metastore client connections."""


class Hive4Namespace(LanceNamespace):
    """Lance Hive4 Namespace implementation using Hive 4.x Metastore."""

    def __init__(self, **properties):
        if not HIVE_AVAILABLE:
            raise ImportError(
                "Hive dependencies not installed. Please install with: "
                "pip install 'lance-namespace[hive4]'"
            )

        self.uri = properties.get("uri", "thrift://localhost:9083")
        self.ugi = properties.get("ugi")
        self.root = properties.get("root", os.getcwd())
        self.pool_size = int(properties.get("client.pool-size", "3"))
        self.sasl_enabled = _as_bool(properties.get(HIVE_METASTORE_SASL_ENABLED_KEY))
        self.metastore_kerberos_principal = properties.get(
            "hive.metastore.kerberos.principal"
        )
        self.kerberos_service_name = _get_kerberos_service_name(properties)
        self.kerberos_client_principal = properties.get("kerberos.client-principal")

        self._properties = properties.copy()
        self._client = None

    def namespace_id(self) -> str:
        return f"Hive4Namespace {{ uri: {self.uri!r} }}"

    @property
    def client(self):
        if self._client is None:
            self._client = Hive4MetastoreClientWrapper(
                self.uri,
                self.ugi,
                sasl_enabled=self.sasl_enabled,
                kerberos_service_name=self.kerberos_service_name,
                kerberos_client_principal=self.kerberos_client_principal,
            )
        return self._client

    def _normalize_identifier(self, identifier: List[str]) -> tuple:
        if len(identifier) == 1:
            return (DEFAULT_CATALOG, "default", identifier[0])
        if len(identifier) == 2:
            return (DEFAULT_CATALOG, identifier[0], identifier[1])
        if len(identifier) == 3:
            return (identifier[0], identifier[1], identifier[2])
        raise ValueError(f"Invalid identifier: {identifier}")

    def _is_root_namespace(self, identifier: Optional[List[str]]) -> bool:
        return not identifier or len(identifier) == 0

    def _get_table_location(self, catalog: str, database: str, table: str) -> str:
        if catalog.lower() == DEFAULT_CATALOG:
            return os.path.join(self.root, f"{database}.db", table)
        return os.path.join(self.root, catalog, f"{database}.db", table)

    def _get_database_name_for_legacy_rpc(self, catalog: str, database: str) -> str:
        return _prepend_catalog_to_db_name(catalog, database)

    def _get_database(self, client, catalog: str, database: str):
        return client.get_database(self._get_database_name_for_legacy_rpc(catalog, database))

    def _get_database_names(self, client, catalog: str) -> List[str]:
        if hasattr(client, "get_databases"):
            return client.get_databases(_prepend_catalog_to_db_name(catalog, None))
        return client.get_all_databases()

    def _get_all_tables(self, client, catalog: str, database: str) -> List[str]:
        return client.get_all_tables(self._get_database_name_for_legacy_rpc(catalog, database))

    def _get_table(self, client, catalog: str, database: str, table_name: str):
        if HIVE4_REQUESTS_AVAILABLE and hasattr(client, "get_table_req"):
            result = client.get_table_req(
                GetTableRequest(dbName=database, tblName=table_name, catName=catalog)
            )
            if result is not None and getattr(result, "table", None) is not None:
                return result.table

        return client.get_table(
            self._get_database_name_for_legacy_rpc(catalog, database), table_name
        )

    def _get_table_objects(
        self, client, catalog: str, database: str, table_names: List[str]
    ) -> List[HiveTable]:
        if not table_names:
            return []

        if HIVE4_REQUESTS_AVAILABLE and hasattr(client, "get_table_objects_by_name_req"):
            result = client.get_table_objects_by_name_req(
                GetTablesRequest(dbName=database, tblNames=table_names, catName=catalog)
            )
            tables = getattr(result, "tables", None)
            if tables is not None:
                return tables

        tables = []
        for table_name in table_names:
            tables.append(self._get_table(client, catalog, database, table_name))
        return tables

    def list_namespaces(self, request: ListNamespacesRequest) -> ListNamespacesResponse:
        try:
            ns_id = request.id if request.id else []

            if self._is_root_namespace(ns_id):
                with self.client as client:
                    try:
                        catalogs = (
                            client.get_catalogs().names
                            if hasattr(client, "get_catalogs")
                            else [DEFAULT_CATALOG]
                        )
                    except Exception:
                        catalogs = [DEFAULT_CATALOG]
                    return ListNamespacesResponse(namespaces=catalogs)

            if len(ns_id) == 1:
                catalog_name = ns_id[0].lower()
                with self.client as client:
                    databases = self._get_database_names(client, catalog_name)
                return ListNamespacesResponse(
                    namespaces=[db for db in databases if db != "default"]
                )

            return ListNamespacesResponse(namespaces=[])

        except Exception as e:
            logger.error(f"Failed to list namespaces: {e}")
            raise

    def describe_namespace(
        self, request: DescribeNamespaceRequest
    ) -> DescribeNamespaceResponse:
        try:
            if self._is_root_namespace(request.id):
                properties = {
                    "location": self.root,
                    "description": "Root namespace (Hive 4.x Metastore)",
                }
                if self.ugi:
                    properties["ugi"] = self.ugi
                if self.sasl_enabled:
                    properties[HIVE_METASTORE_SASL_ENABLED_KEY] = "true"
                    if self.metastore_kerberos_principal:
                        properties["hive.metastore.kerberos.principal"] = (
                            self.metastore_kerberos_principal
                        )
                    properties["kerberos.service-name"] = self.kerberos_service_name
                if self.kerberos_client_principal:
                    properties["kerberos.client-principal"] = (
                        self.kerberos_client_principal
                    )
                return DescribeNamespaceResponse(properties=properties)

            if len(request.id) == 1:
                catalog_name = request.id[0].lower()
                properties = {
                    "description": f"Catalog: {catalog_name}",
                    "catalog.location.uri": os.path.join(self.root, catalog_name),
                }
                with self.client as client:
                    try:
                        catalog = client.get_catalog(catalog_name)
                    except Exception:
                        catalog = None
                if catalog is not None:
                    if getattr(catalog, "description", None):
                        properties["description"] = catalog.description
                    if getattr(catalog, "locationUri", None):
                        properties["catalog.location.uri"] = catalog.locationUri
                return DescribeNamespaceResponse(properties=properties)

            if len(request.id) == 2:
                catalog_name = request.id[0].lower()
                database_name = request.id[1].lower()
                with self.client as client:
                    database = self._get_database(client, catalog_name, database_name)

                properties = {}
                if database.description:
                    properties["comment"] = database.description
                if database.ownerName:
                    properties["owner"] = database.ownerName
                if database.locationUri:
                    properties["location"] = database.locationUri
                if database.parameters:
                    properties.update(database.parameters)
                if getattr(database, "catalogName", None):
                    properties["catalog"] = database.catalogName
                return DescribeNamespaceResponse(properties=properties)

            raise ValueError(f"Invalid namespace identifier: {request.id}")

        except Exception as e:
            if NoSuchObjectException and isinstance(e, NoSuchObjectException):
                raise ValueError(f"Namespace {request.id} does not exist")
            logger.error(f"Failed to describe namespace {request.id}: {e}")
            raise

    def create_namespace(
        self, request: CreateNamespaceRequest
    ) -> CreateNamespaceResponse:
        try:
            if self._is_root_namespace(request.id):
                raise ValueError("Root namespace already exists")

            mode = request.mode.lower() if request.mode else "create"

            if len(request.id) == 1:
                catalog_name = request.id[0].lower()
                if Catalog is None:
                    raise ImportError("Hive 4 catalog thrift types are not available")

                catalog = Catalog(
                    name=catalog_name,
                    description=(
                        request.properties.get("comment", "")
                        if request.properties
                        else ""
                    ),
                    locationUri=(
                        request.properties.get(
                            "location", os.path.join(self.root, catalog_name)
                        )
                        if request.properties
                        else os.path.join(self.root, catalog_name)
                    ),
                )

                with self.client as client:
                    try:
                        client.create_catalog(catalog)
                    except AlreadyExistsException:
                        if mode == "create":
                            raise ValueError(f"Namespace {request.id} already exists")
                        if mode in ("exist_ok", "existok"):
                            pass
                        elif mode == "overwrite":
                            client.drop_catalog(catalog_name)
                            client.create_catalog(catalog)
                return CreateNamespaceResponse()

            if len(request.id) == 2:
                catalog_name = request.id[0].lower()
                database_name = request.id[1].lower()

                if not HiveDatabase:
                    raise ImportError("Hive dependencies not available")

                database = HiveDatabase()
                database.name = database_name
                database.catalogName = catalog_name
                database.description = (
                    request.properties.get("comment", "") if request.properties else ""
                )
                database.ownerName = (
                    request.properties.get("owner", os.getenv("USER", ""))
                    if request.properties
                    else os.getenv("USER", "")
                )
                database.locationUri = (
                    request.properties.get(
                        "location",
                        os.path.join(
                            self.root,
                            f"{database_name}.db"
                            if catalog_name == DEFAULT_CATALOG
                            else os.path.join(catalog_name, f"{database_name}.db"),
                        ),
                    )
                    if request.properties
                    else os.path.join(
                        self.root,
                        f"{database_name}.db"
                        if catalog_name == DEFAULT_CATALOG
                        else os.path.join(catalog_name, f"{database_name}.db"),
                    )
                )
                if request.properties:
                    database.parameters = {
                        k: v
                        for k, v in request.properties.items()
                        if k not in ["comment", "owner", "location"]
                    }

                with self.client as client:
                    try:
                        client.create_database(database)
                    except AlreadyExistsException:
                        if mode == "create":
                            raise ValueError(f"Namespace {request.id} already exists")
                        if mode in ("exist_ok", "existok"):
                            pass
                        elif mode == "overwrite":
                            client.drop_database(
                                self._get_database_name_for_legacy_rpc(
                                    catalog_name, database_name
                                ),
                                deleteData=True,
                                cascade=True,
                            )
                            client.create_database(database)
                return CreateNamespaceResponse()

            raise ValueError(f"Invalid namespace identifier: {request.id}")

        except Exception as e:
            if AlreadyExistsException and isinstance(e, AlreadyExistsException):
                raise ValueError(f"Namespace {request.id} already exists")
            logger.error(f"Failed to create namespace {request.id}: {e}")
            raise

    def drop_namespace(self, request: DropNamespaceRequest) -> DropNamespaceResponse:
        if request.behavior and request.behavior.lower() == "cascade":
            raise InvalidInputException(
                "Cascade behavior is not supported for this implementation"
            )

        try:
            if self._is_root_namespace(request.id):
                raise ValueError("Cannot drop root namespace")

            if len(request.id) == 1:
                catalog_name = request.id[0].lower()
                with self.client as client:
                    databases = self._get_database_names(client, catalog_name)
                    if [db for db in databases if db != "default"]:
                        raise ValueError(f"Namespace {request.id} is not empty")
                    client.drop_catalog(catalog_name)
                return DropNamespaceResponse()

            if len(request.id) == 2:
                catalog_name = request.id[0].lower()
                database_name = request.id[1].lower()

                with self.client as client:
                    tables = self._get_all_tables(client, catalog_name, database_name)
                    if tables:
                        raise ValueError(f"Namespace {request.id} is not empty")

                    client.drop_database(
                        self._get_database_name_for_legacy_rpc(
                            catalog_name, database_name
                        ),
                        deleteData=True,
                        cascade=False,
                    )

                return DropNamespaceResponse()

            raise ValueError(f"Invalid namespace identifier: {request.id}")

        except Exception as e:
            if NoSuchObjectException and isinstance(e, NoSuchObjectException):
                raise ValueError(f"Namespace {request.id} does not exist")
            logger.error(f"Failed to drop namespace {request.id}: {e}")
            raise

    def list_tables(self, request: ListTablesRequest) -> ListTablesResponse:
        try:
            if self._is_root_namespace(request.id) or len(request.id) < 2:
                return ListTablesResponse(tables=[])

            catalog_name = request.id[0].lower()
            database_name = request.id[1].lower()

            with self.client as client:
                table_names = self._get_all_tables(client, catalog_name, database_name)
                tables = []
                for table in self._get_table_objects(
                    client, catalog_name, database_name, table_names
                ):
                    if table.parameters:
                        table_type = table.parameters.get(TABLE_TYPE_KEY, "").lower()
                        if table_type == LANCE_TABLE_FORMAT:
                            tables.append(table.tableName)

                return ListTablesResponse(tables=tables)

        except Exception as e:
            if NoSuchObjectException and isinstance(e, NoSuchObjectException):
                raise ValueError(f"Namespace {request.id} does not exist")
            logger.error(f"Failed to list tables in namespace {request.id}: {e}")
            raise

    def describe_table(self, request: DescribeTableRequest) -> DescribeTableResponse:
        if request.load_detailed_metadata:
            raise ValueError(
                "load_detailed_metadata=true is not supported for this implementation"
            )

        try:
            catalog, database, table_name = self._normalize_identifier(request.id)

            with self.client as client:
                table = self._get_table(client, catalog, database, table_name)

            if not table.parameters:
                raise ValueError(f"Table {request.id} is not a Lance table")
            table_type = table.parameters.get(TABLE_TYPE_KEY, "").lower()
            if table_type != LANCE_TABLE_FORMAT:
                raise ValueError(f"Table {request.id} is not a Lance table")

            location = table.sd.location if table.sd else None
            if not location:
                raise ValueError(f"Table {request.id} has no location")

            return DescribeTableResponse(location=location)

        except Exception as e:
            if NoSuchObjectException and isinstance(e, NoSuchObjectException):
                raise ValueError(f"Table {request.id} does not exist")
            logger.error(f"Failed to describe table {request.id}: {e}")
            raise

    def drop_table(self, request: DropTableRequest) -> DropTableResponse:
        try:
            catalog, database, table_name = self._normalize_identifier(request.id)

            with self.client as client:
                table = self._get_table(client, catalog, database, table_name)

                if not table.parameters:
                    raise ValueError(f"Table {request.id} is not a Lance table")
                table_type = table.parameters.get(TABLE_TYPE_KEY, "").lower()
                if table_type != LANCE_TABLE_FORMAT:
                    raise ValueError(f"Table {request.id} is not a Lance table")

                location = table.sd.location if table.sd else None

                client.drop_table(
                    self._get_database_name_for_legacy_rpc(catalog, database),
                    table_name,
                    deleteData=True,
                )

                return DropTableResponse(location=location)

        except Exception as e:
            if NoSuchObjectException and isinstance(e, NoSuchObjectException):
                raise ValueError(f"Table {request.id} does not exist")
            logger.error(f"Failed to drop table {request.id}: {e}")
            raise

    def deregister_table(
        self, request: DeregisterTableRequest
    ) -> DeregisterTableResponse:
        try:
            catalog, database, table_name = self._normalize_identifier(request.id)

            with self.client as client:
                table = self._get_table(client, catalog, database, table_name)

                if not table.parameters:
                    raise ValueError(f"Table {request.id} is not a Lance table")
                table_type = table.parameters.get(TABLE_TYPE_KEY, "").lower()
                if table_type != LANCE_TABLE_FORMAT:
                    raise ValueError(f"Table {request.id} is not a Lance table")

                location = table.sd.location if table.sd else None

                client.drop_table(
                    self._get_database_name_for_legacy_rpc(catalog, database),
                    table_name,
                    deleteData=False,
                )

                return DeregisterTableResponse(location=location)

        except Exception as e:
            if NoSuchObjectException and isinstance(e, NoSuchObjectException):
                raise ValueError(f"Table {request.id} does not exist")
            logger.error(f"Failed to deregister table {request.id}: {e}")
            raise

    def declare_table(self, request: DeclareTableRequest) -> DeclareTableResponse:
        try:
            catalog, database, table_name = self._normalize_identifier(request.id)
            location = request.location or self._get_table_location(
                catalog, database, table_name
            )

            if not FieldSchema:
                raise ImportError("Hive dependencies not available")

            fields = [
                FieldSchema(
                    name="__placeholder_id", type="bigint", comment="Placeholder column"
                )
            ]

            storage_descriptor = StorageDescriptor(
                cols=fields,
                location=location,
                inputFormat="org.apache.hadoop.mapred.TextInputFormat",
                outputFormat="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                serdeInfo=SerDeInfo(
                    serializationLib="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                ),
            )

            parameters = {
                TABLE_TYPE_KEY: LANCE_TABLE_FORMAT,
                MANAGED_BY_KEY: "storage",
                VERSION_KEY: "0",
                "empty_table": "true",
            }

            hive_table = HiveTable(
                tableName=table_name,
                dbName=database,
                catName=catalog,
                sd=storage_descriptor,
                parameters=parameters,
                tableType=EXTERNAL_TABLE,
            )

            with self.client as client:
                client.create_table(hive_table)

            return DeclareTableResponse(location=location)

        except AlreadyExistsException:
            raise ValueError(f"Table {request.id} already exists")
        except Exception as e:
            logger.error(f"Failed to declare table {request.id}: {e}")
            raise

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_client"] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

    def close(self):
        if self._client is not None:
            self._client.close()
            self._client = None
