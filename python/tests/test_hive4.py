"""
Tests for Lance Hive4 Namespace implementation.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import lance_namespace_impls.hive4 as hive4
from lance_namespace_impls.hive4 import Hive4Namespace, _prepend_catalog_to_db_name
from lance_namespace_urllib3_client.models import (
    CreateNamespaceRequest,
    DescribeNamespaceRequest,
    DescribeTableRequest,
    DeregisterTableRequest,
    DropTableRequest,
    ListNamespacesRequest,
    ListTablesRequest,
)


@pytest.fixture
def mock_hive_client():
    with patch("lance_namespace_impls.hive4.HIVE_AVAILABLE", True):
        with patch(
            "lance_namespace_impls.hive4.Hive4MetastoreClientWrapper"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            yield mock_client


@pytest.fixture
def hive_namespace(mock_hive_client):
    with patch("lance_namespace_impls.hive4.HIVE_AVAILABLE", True):
        namespace = Hive4Namespace(uri="thrift://localhost:9083", root="/tmp/warehouse")
        namespace._client = mock_hive_client
        return namespace


class FakeGetTableRequest:
    def __init__(self, dbName=None, tblName=None, capabilities=None, catName=None):
        self.dbName = dbName
        self.tblName = tblName
        self.capabilities = capabilities
        self.catName = catName


class FakeGetTablesRequest:
    def __init__(self, dbName=None, tblNames=None, capabilities=None, catName=None):
        self.dbName = dbName
        self.tblNames = tblNames
        self.capabilities = capabilities
        self.catName = catName


class TestHive4Namespace:
    def test_initialization(self):
        with patch("lance_namespace_impls.hive4.HIVE_AVAILABLE", True):
            with patch(
                "lance_namespace_impls.hive4.Hive4MetastoreClientWrapper"
            ) as mock_client:
                namespace = Hive4Namespace(
                    uri="thrift://localhost:9083",
                    root="/tmp/warehouse",
                    ugi="user:group1,group2",
                )

                assert namespace.uri == "thrift://localhost:9083"
                assert namespace.root == "/tmp/warehouse"
                assert namespace.ugi == "user:group1,group2"

                _ = namespace.client
                mock_client.assert_called_once_with(
                    "thrift://localhost:9083",
                    "user:group1,group2",
                    sasl_enabled=False,
                    kerberos_service_name="hive",
                    kerberos_client_principal=None,
                )

    def test_list_namespaces_catalog_level_uses_catalog_aware_pattern(
        self, hive_namespace, mock_hive_client
    ):
        mock_client_instance = MagicMock()
        mock_client_instance.get_databases.return_value = ["default", "test_db", "prod_db"]
        mock_hive_client.__enter__.return_value = mock_client_instance

        response = hive_namespace.list_namespaces(ListNamespacesRequest(id=["analytics"]))

        assert response.namespaces == ["test_db", "prod_db"]
        mock_client_instance.get_databases.assert_called_once_with("@analytics#")

    def test_describe_namespace_database_uses_catalog_aware_db_name(
        self, hive_namespace, mock_hive_client
    ):
        mock_database = MagicMock()
        mock_database.description = "Test database"
        mock_database.ownerName = "test_user"
        mock_database.locationUri = "/tmp/warehouse/analytics/test_db"
        mock_database.parameters = {"key": "value"}
        mock_database.catalogName = "analytics"

        mock_client_instance = MagicMock()
        mock_client_instance.get_database.return_value = mock_database
        mock_hive_client.__enter__.return_value = mock_client_instance

        response = hive_namespace.describe_namespace(
            DescribeNamespaceRequest(id=["analytics", "test_db"])
        )

        assert response.properties["comment"] == "Test database"
        assert response.properties["catalog"] == "analytics"
        mock_client_instance.get_database.assert_called_once_with("@analytics#test_db")

    def test_list_tables_uses_get_table_objects_by_name_req(
        self, hive_namespace, mock_hive_client
    ):
        mock_client_instance = MagicMock()
        mock_client_instance.get_all_tables.return_value = ["table1", "table2", "table3"]
        mock_client_instance.get_table_objects_by_name_req.return_value = SimpleNamespace(
            tables=[
                SimpleNamespace(tableName="table1", parameters={"table_type": "lance"}),
                SimpleNamespace(tableName="table2", parameters={"other_type": "OTHER"}),
                SimpleNamespace(tableName="table3", parameters={"table_type": "lance"}),
            ]
        )
        mock_hive_client.__enter__.return_value = mock_client_instance

        with patch.object(hive4, "HIVE4_REQUESTS_AVAILABLE", True):
            with patch.object(hive4, "GetTablesRequest", FakeGetTablesRequest):
                response = hive_namespace.list_tables(
                    ListTablesRequest(id=["analytics", "test_db"])
                )

        assert response.tables == ["table1", "table3"]
        mock_client_instance.get_all_tables.assert_called_once_with("@analytics#test_db")
        request = mock_client_instance.get_table_objects_by_name_req.call_args.args[0]
        assert request.dbName == "test_db"
        assert request.tblNames == ["table1", "table2", "table3"]
        assert request.catName == "analytics"

    def test_describe_table_uses_get_table_req(self, hive_namespace, mock_hive_client):
        mock_table = SimpleNamespace(
            sd=SimpleNamespace(location="/tmp/warehouse/analytics/test_db/test_table"),
            parameters={"table_type": "lance", "version": "1"},
        )
        mock_client_instance = MagicMock()
        mock_client_instance.get_table_req.return_value = SimpleNamespace(table=mock_table)
        mock_hive_client.__enter__.return_value = mock_client_instance

        with patch.object(hive4, "HIVE4_REQUESTS_AVAILABLE", True):
            with patch.object(hive4, "GetTableRequest", FakeGetTableRequest):
                response = hive_namespace.describe_table(
                    DescribeTableRequest(id=["analytics", "test_db", "test_table"])
                )

        assert response.location == "/tmp/warehouse/analytics/test_db/test_table"
        request = mock_client_instance.get_table_req.call_args.args[0]
        assert request.dbName == "test_db"
        assert request.tblName == "test_table"
        assert request.catName == "analytics"

    def test_drop_table_uses_catalog_aware_legacy_drop(
        self, hive_namespace, mock_hive_client
    ):
        mock_table = SimpleNamespace(
            sd=SimpleNamespace(location="/tmp/test_table"),
            parameters={"table_type": "lance"},
        )
        mock_client_instance = MagicMock()
        mock_client_instance.get_table_req.return_value = SimpleNamespace(table=mock_table)
        mock_hive_client.__enter__.return_value = mock_client_instance

        with patch.object(hive4, "HIVE4_REQUESTS_AVAILABLE", True):
            with patch.object(hive4, "GetTableRequest", FakeGetTableRequest):
                response = hive_namespace.drop_table(
                    DropTableRequest(id=["analytics", "test_db", "test_table"])
                )

        assert response.location == "/tmp/test_table"
        mock_client_instance.drop_table.assert_called_once_with(
            "@analytics#test_db", "test_table", deleteData=True
        )

    def test_deregister_table_uses_catalog_aware_legacy_drop(
        self, hive_namespace, mock_hive_client
    ):
        mock_table = SimpleNamespace(
            sd=SimpleNamespace(location="/tmp/test_table"),
            parameters={"table_type": "lance"},
        )
        mock_client_instance = MagicMock()
        mock_client_instance.get_table_req.return_value = SimpleNamespace(table=mock_table)
        mock_hive_client.__enter__.return_value = mock_client_instance

        with patch.object(hive4, "HIVE4_REQUESTS_AVAILABLE", True):
            with patch.object(hive4, "GetTableRequest", FakeGetTableRequest):
                response = hive_namespace.deregister_table(
                    DeregisterTableRequest(id=["analytics", "test_db", "test_table"])
                )

        assert response.location == "/tmp/test_table"
        mock_client_instance.drop_table.assert_called_once_with(
            "@analytics#test_db", "test_table", deleteData=False
        )

    def test_create_namespace_database_sets_catalog_name(
        self, hive_namespace, mock_hive_client
    ):
        mock_client_instance = MagicMock()
        mock_hive_client.__enter__.return_value = mock_client_instance

        with patch("lance_namespace_impls.hive4.HiveDatabase") as mock_hive_db_class:
            mock_hive_db = MagicMock()
            mock_hive_db_class.return_value = mock_hive_db

            hive_namespace.create_namespace(
                CreateNamespaceRequest(
                    id=["analytics", "test_db"],
                    properties={"comment": "Test database", "owner": "test_user"},
                )
            )

            assert mock_hive_db.name == "test_db"
            assert mock_hive_db.catalogName == "analytics"
            mock_client_instance.create_database.assert_called_once_with(mock_hive_db)


def test_prepend_catalog_to_db_name():
    assert _prepend_catalog_to_db_name("analytics", "db1") == "@analytics#db1"
    assert _prepend_catalog_to_db_name("analytics", None) == "@analytics#"
    assert _prepend_catalog_to_db_name("analytics", "") == "@analytics#!"
