use std::collections::HashMap;

use async_trait::async_trait;
use lance_namespace::{
    LanceNamespace, NamespaceError, Result,
    models::{
        DescribeNamespaceRequest, DescribeNamespaceResponse, DescribeTableRequest,
        DescribeTableResponse, ListNamespacesRequest, ListNamespacesResponse, ListTablesRequest,
        ListTablesResponse, NamespaceExistsRequest, TableExistsRequest,
    },
};

use crate::{HiveNamespace, HiveNamespaceError, HiveNamespaceMetadata, HmsReadTransport};

/// Read-only bridge to Lance's Rust namespace API.
///
/// Only read methods are implemented. The default `LanceNamespace` behavior
/// rejects every mutation method, so a gateway cannot issue HMS writes through
/// this implementation.
#[derive(Debug)]
pub struct LanceHiveNamespace<T>(HiveNamespace<T>);

impl<T: HmsReadTransport> LanceHiveNamespace<T> {
    pub fn new(namespace: HiveNamespace<T>) -> Self {
        Self(namespace)
    }
    pub fn inner(&self) -> &HiveNamespace<T> {
        &self.0
    }
}

#[async_trait]
impl<T: HmsReadTransport> LanceNamespace for LanceHiveNamespace<T> {
    fn namespace_id(&self) -> String {
        self.0.namespace_id()
    }

    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
        let namespaces = self
            .0
            .list_namespaces(&request.id.unwrap_or_default())
            .await
            .map_err(namespace_error)?;
        Ok(ListNamespacesResponse::new(namespaces))
    }

    async fn describe_namespace(
        &self,
        request: DescribeNamespaceRequest,
    ) -> Result<DescribeNamespaceResponse> {
        let metadata = self
            .0
            .describe_namespace(&request.id.unwrap_or_default())
            .await
            .map_err(namespace_error)?;
        Ok(DescribeNamespaceResponse {
            properties: Some(properties(metadata)),
        })
    }

    async fn namespace_exists(&self, request: NamespaceExistsRequest) -> Result<()> {
        self.0
            .describe_namespace(&request.id.unwrap_or_default())
            .await
            .map_err(namespace_error)?;
        Ok(())
    }

    async fn list_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse> {
        let id = required_id(request.id, "namespace")?;
        let tables = self.0.list_tables(&id).await.map_err(namespace_error)?;
        Ok(ListTablesResponse::new(tables))
    }

    async fn describe_table(&self, request: DescribeTableRequest) -> Result<DescribeTableResponse> {
        if request.load_detailed_metadata == Some(true) {
            return Err(NamespaceError::Unsupported {
                message: "Hive namespace does not load Lance metadata; open the returned location with Lance instead".to_owned(),
            }.into());
        }
        let id = required_id(request.id, "table")?;
        let location = self.0.describe_table(&id).await.map_err(namespace_error)?;
        Ok(DescribeTableResponse {
            location: Some(location.clone()),
            table_uri: request
                .with_table_uri
                .filter(|value| *value)
                .map(|_| location),
            ..Default::default()
        })
    }

    async fn table_exists(&self, request: TableExistsRequest) -> Result<()> {
        let id = required_id(request.id, "table")?;
        self.0.table_exists(&id).await.map_err(namespace_error)
    }
}

fn required_id(id: Option<Vec<String>>, kind: &'static str) -> Result<Vec<String>> {
    id.filter(|value| !value.is_empty()).ok_or_else(|| {
        NamespaceError::InvalidInput {
            message: format!("{kind} identifier is required"),
        }
        .into()
    })
}

fn properties(metadata: HiveNamespaceMetadata) -> HashMap<String, String> {
    let mut values: HashMap<_, _> = metadata.parameters.into_iter().collect();
    if let Some(location) = metadata.location {
        values.insert("location".to_owned(), location);
    }
    if let Some(comment) = metadata.comment {
        values.insert("comment".to_owned(), comment);
    }
    if let Some(owner) = metadata.owner {
        values.insert("owner".to_owned(), owner);
    }
    values
}

fn namespace_error(error: HiveNamespaceError) -> lance_namespace::Error {
    let message = error.to_string();
    let error = match error {
        HiveNamespaceError::InvalidIdentifier { .. } => NamespaceError::InvalidInput { message },
        HiveNamespaceError::NamespaceNotFound { .. } => {
            NamespaceError::NamespaceNotFound { message }
        }
        HiveNamespaceError::NotLanceTable { .. } | HiveNamespaceError::MissingLocation { .. } => {
            NamespaceError::TableNotFound { message }
        }
        HiveNamespaceError::Metastore(_) => NamespaceError::Internal { message },
    };
    error.into()
}
