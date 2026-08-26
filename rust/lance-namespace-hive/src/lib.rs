//! Read-only Hive Metastore namespace support for Hive 2, Hive 3, and Hive 4.
//!
//! This crate intentionally exposes no Hive DDL or data mutation operation.
//! It reads the external-table registration from HMS and returns the Lance
//! location for a caller that has enabled the internal Lance `hdfs-native`
//! feature. It never imports a JVM, libhdfs, or QBFS client.

use async_trait::async_trait;
use thiserror::Error;

#[cfg(feature = "hms-sasl")]
mod hive2;
#[cfg(feature = "hms-sasl")]
mod hive3;
#[cfg(feature = "hms-sasl")]
mod hive4;
#[cfg(feature = "lance-namespace-api")]
mod lance_adapter;
#[cfg(feature = "lance-namespace-api")]
pub use lance_adapter::LanceHiveNamespace;
#[cfg(feature = "hms-sasl")]
mod hms_sasl;
#[cfg(feature = "hms-sasl")]
pub use hms_sasl::HmsSaslTransport;

pub const TABLE_TYPE_KEY: &str = "table_type";
pub const LANCE_TABLE_FORMAT: &str = "lance";
pub const DEFAULT_CATALOG: &str = "hive";
pub const DEFAULT_DATABASE: &str = "default";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HiveVersion {
    Hive2,
    Hive3,
    Hive4,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HiveConfig {
    pub version: HiveVersion,
    pub uri: String,
    pub root: String,
    pub ugi: Option<String>,
    pub sasl_enabled: bool,
    pub kerberos_service_name: String,
    pub kerberos_client_principal: Option<String>,
}

impl HiveConfig {
    pub fn new(version: HiveVersion, uri: impl Into<String>, root: impl Into<String>) -> Self {
        Self {
            version,
            uri: uri.into(),
            root: root.into(),
            ugi: None,
            sasl_enabled: false,
            kerberos_service_name: "hive".to_owned(),
            kerberos_client_principal: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HiveNamespaceMetadata {
    pub location: Option<String>,
    pub comment: Option<String>,
    pub owner: Option<String>,
    pub parameters: Vec<(String, String)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HiveTable {
    pub name: String,
    pub location: Option<String>,
    pub parameters: Vec<(String, String)>,
}

impl HiveTable {
    pub fn is_lance(&self) -> bool {
        self.parameters.iter().any(|(key, value)| {
            key.eq_ignore_ascii_case(TABLE_TYPE_KEY)
                && value.eq_ignore_ascii_case(LANCE_TABLE_FORMAT)
        })
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum HiveNamespaceError {
    #[error("invalid {kind} identifier {id:?}; expected {expected}")]
    InvalidIdentifier {
        kind: &'static str,
        id: Vec<String>,
        expected: &'static str,
    },
    #[error("namespace {id:?} is not found")]
    NamespaceNotFound { id: Vec<String> },
    #[error("table {id:?} is not a Lance table")]
    NotLanceTable { id: Vec<String> },
    #[error("table {id:?} has no location")]
    MissingLocation { id: Vec<String> },
    #[error("Hive Metastore error: {0}")]
    Metastore(String),
}

/// Minimal, read-only HMS RPC surface.
///
/// There are intentionally no create, alter, register, drop, or deregister
/// methods. An implementation of this trait cannot issue HMS write RPCs.
#[async_trait]
pub trait HmsReadTransport: Send + Sync + std::fmt::Debug {
    async fn list_catalogs(&self) -> Result<Vec<String>, HiveNamespaceError>;
    async fn get_catalog(&self, catalog: &str)
    -> Result<HiveNamespaceMetadata, HiveNamespaceError>;
    async fn list_databases(
        &self,
        catalog: Option<&str>,
    ) -> Result<Vec<String>, HiveNamespaceError>;
    async fn get_database(
        &self,
        catalog: Option<&str>,
        database: &str,
    ) -> Result<HiveNamespaceMetadata, HiveNamespaceError>;
    async fn list_tables(
        &self,
        catalog: Option<&str>,
        database: &str,
    ) -> Result<Vec<String>, HiveNamespaceError>;
    async fn get_table(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<HiveTable, HiveNamespaceError>;
}

#[derive(Debug)]
pub struct HiveNamespace<T> {
    config: HiveConfig,
    transport: T,
}

impl<T: HmsReadTransport> HiveNamespace<T> {
    pub fn new(config: HiveConfig, transport: T) -> Self {
        Self { config, transport }
    }

    pub fn config(&self) -> &HiveConfig {
        &self.config
    }

    pub fn namespace_id(&self) -> String {
        format!(
            "{:?}Namespace {{ uri: {:?} }}",
            self.config.version, self.config.uri
        )
    }

    /// Python-compatible hierarchy: Hive2 lists databases at root; Hive3/4
    /// list catalogs at root and databases under a catalog.
    pub async fn list_namespaces(&self, id: &[String]) -> Result<Vec<String>, HiveNamespaceError> {
        match (self.config.version, id.len()) {
            (HiveVersion::Hive2, 0) => without_default(self.transport.list_databases(None).await),
            (HiveVersion::Hive2, _) => Ok(Vec::new()),
            (HiveVersion::Hive3 | HiveVersion::Hive4, 0) => self.transport.list_catalogs().await,
            (HiveVersion::Hive3 | HiveVersion::Hive4, 1) => {
                let catalog = normalized(&id[0]);
                without_default(self.transport.list_databases(Some(&catalog)).await)
            }
            (HiveVersion::Hive3 | HiveVersion::Hive4, _) => Ok(Vec::new()),
        }
    }

    pub async fn describe_namespace(
        &self,
        id: &[String],
    ) -> Result<HiveNamespaceMetadata, HiveNamespaceError> {
        if id.is_empty() {
            return Ok(HiveNamespaceMetadata {
                location: Some(self.config.root.clone()),
                comment: Some(format!(
                    "Root namespace (Hive {:?}.x Metastore)",
                    self.config.version
                )),
                owner: None,
                parameters: self.connection_properties(),
            });
        }
        match self.config.version {
            HiveVersion::Hive2 if id.len() == 1 => self.transport.get_database(None, &id[0]).await,
            HiveVersion::Hive3 | HiveVersion::Hive4 if id.len() == 1 => {
                self.transport.get_catalog(&normalized(&id[0])).await
            }
            HiveVersion::Hive3 | HiveVersion::Hive4 if id.len() == 2 => {
                let catalog = normalized(&id[0]);
                self.transport
                    .get_database(Some(&catalog), &normalized(&id[1]))
                    .await
            }
            _ => Err(self.invalid_namespace(id)),
        }
    }

    /// Return only HMS external tables explicitly registered as Lance tables.
    pub async fn list_tables(
        &self,
        namespace: &[String],
    ) -> Result<Vec<String>, HiveNamespaceError> {
        // Root and catalog-only lists are empty in the Python implementations.
        if namespace.is_empty()
            || matches!(self.config.version, HiveVersion::Hive3 | HiveVersion::Hive4)
                && namespace.len() == 1
        {
            return Ok(Vec::new());
        }
        let (catalog, database) = self.parse_namespace(namespace)?;
        let names = self
            .transport
            .list_tables(catalog.as_deref(), &database)
            .await?;
        let mut output = Vec::new();
        for name in names {
            let table = self
                .transport
                .get_table(catalog.as_deref(), &database, &name)
                .await?;
            if table.is_lance() {
                output.push(name);
            }
        }
        output.sort_unstable();
        Ok(output)
    }

    /// Matches Python `describe_table(load_detailed_metadata=False)`: return
    /// only the HMS location; Lance metadata stays in the Lance dataset.
    pub async fn describe_table(&self, id: &[String]) -> Result<String, HiveNamespaceError> {
        let (catalog, database, table_name) = self.parse_table(id)?;
        let table = self
            .transport
            .get_table(catalog.as_deref(), &database, &table_name)
            .await?;
        if !table.is_lance() {
            return Err(HiveNamespaceError::NotLanceTable { id: id.to_vec() });
        }
        table
            .location
            .ok_or_else(|| HiveNamespaceError::MissingLocation { id: id.to_vec() })
    }

    pub async fn table_exists(&self, id: &[String]) -> Result<(), HiveNamespaceError> {
        self.describe_table(id).await.map(|_| ())
    }

    pub fn parse_namespace(
        &self,
        id: &[String],
    ) -> Result<(Option<String>, String), HiveNamespaceError> {
        match self.config.version {
            HiveVersion::Hive2 if id.len() == 1 => Ok((None, normalized(&id[0]))),
            HiveVersion::Hive3 | HiveVersion::Hive4 if id.len() == 2 => {
                Ok((Some(normalized(&id[0])), normalized(&id[1])))
            }
            _ => Err(self.invalid_namespace(id)),
        }
    }

    /// Python accepts shortened IDs: `table` -> `default.table` for Hive2 and
    /// `table` / `db.table` -> `hive.default.table` / `hive.db.table` for 3/4.
    pub fn parse_table(
        &self,
        id: &[String],
    ) -> Result<(Option<String>, String, String), HiveNamespaceError> {
        match (self.config.version, id.len()) {
            (HiveVersion::Hive2, 1) => Ok((None, DEFAULT_DATABASE.to_owned(), normalized(&id[0]))),
            (HiveVersion::Hive2, 2) => Ok((None, normalized(&id[0]), normalized(&id[1]))),
            (HiveVersion::Hive3 | HiveVersion::Hive4, 1) => Ok((
                Some(DEFAULT_CATALOG.to_owned()),
                DEFAULT_DATABASE.to_owned(),
                normalized(&id[0]),
            )),
            (HiveVersion::Hive3 | HiveVersion::Hive4, 2) => Ok((
                Some(DEFAULT_CATALOG.to_owned()),
                normalized(&id[0]),
                normalized(&id[1]),
            )),
            (HiveVersion::Hive3 | HiveVersion::Hive4, 3) => Ok((
                Some(normalized(&id[0])),
                normalized(&id[1]),
                normalized(&id[2]),
            )),
            _ => Err(HiveNamespaceError::InvalidIdentifier {
                kind: "table",
                id: id.to_vec(),
                expected: "a table name, namespace/table, or catalog/namespace/table",
            }),
        }
    }

    /// Hive4 legacy RPC fallback encodes the catalog and database as
    /// `@catalog#database`. A transport can use this only when request-style
    /// catalog RPCs are unavailable.
    pub fn legacy_database_name(&self, catalog: Option<&str>, database: &str) -> String {
        if self.config.version == HiveVersion::Hive4 {
            format!("@{}#{}", catalog.unwrap_or(DEFAULT_CATALOG), database)
        } else {
            database.to_owned()
        }
    }

    fn connection_properties(&self) -> Vec<(String, String)> {
        let mut values = Vec::new();
        if let Some(ugi) = &self.config.ugi {
            values.push(("ugi".to_owned(), ugi.clone()));
        }
        if self.config.sasl_enabled {
            values.push(("hive.metastore.sasl.enabled".to_owned(), "true".to_owned()));
            values.push((
                "kerberos.service-name".to_owned(),
                self.config.kerberos_service_name.clone(),
            ));
        }
        if let Some(principal) = &self.config.kerberos_client_principal {
            values.push(("kerberos.client-principal".to_owned(), principal.clone()));
        }
        values
    }

    fn invalid_namespace(&self, id: &[String]) -> HiveNamespaceError {
        HiveNamespaceError::InvalidIdentifier {
            kind: "namespace",
            id: id.to_vec(),
            expected: if self.config.version == HiveVersion::Hive2 {
                "one database name"
            } else {
                "a catalog name or catalog/database"
            },
        }
    }
}

fn without_default(
    values: Result<Vec<String>, HiveNamespaceError>,
) -> Result<Vec<String>, HiveNamespaceError> {
    values.map(|mut values| {
        values.retain(|value| !value.eq_ignore_ascii_case(DEFAULT_DATABASE));
        values.sort_unstable();
        values
    })
}

fn normalized(value: &str) -> String {
    value.to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[derive(Debug, Default)]
    struct StaticHms {
        catalogs: Vec<String>,
        databases: BTreeMap<Option<String>, Vec<String>>,
        tables: BTreeMap<(Option<String>, String), Vec<HiveTable>>,
    }

    #[async_trait]
    impl HmsReadTransport for StaticHms {
        async fn list_catalogs(&self) -> Result<Vec<String>, HiveNamespaceError> {
            Ok(self.catalogs.clone())
        }
        async fn get_catalog(
            &self,
            catalog: &str,
        ) -> Result<HiveNamespaceMetadata, HiveNamespaceError> {
            self.catalogs
                .iter()
                .any(|item| item == catalog)
                .then_some(HiveNamespaceMetadata {
                    location: None,
                    comment: None,
                    owner: None,
                    parameters: Vec::new(),
                })
                .ok_or_else(|| HiveNamespaceError::NamespaceNotFound {
                    id: vec![catalog.to_owned()],
                })
        }
        async fn list_databases(
            &self,
            catalog: Option<&str>,
        ) -> Result<Vec<String>, HiveNamespaceError> {
            Ok(self
                .databases
                .get(&catalog.map(str::to_owned))
                .cloned()
                .unwrap_or_default())
        }
        async fn get_database(
            &self,
            catalog: Option<&str>,
            database: &str,
        ) -> Result<HiveNamespaceMetadata, HiveNamespaceError> {
            self.list_databases(catalog)
                .await?
                .iter()
                .any(|item| item == database)
                .then_some(HiveNamespaceMetadata {
                    location: None,
                    comment: None,
                    owner: None,
                    parameters: Vec::new(),
                })
                .ok_or_else(|| HiveNamespaceError::NamespaceNotFound {
                    id: vec![database.to_owned()],
                })
        }
        async fn list_tables(
            &self,
            catalog: Option<&str>,
            database: &str,
        ) -> Result<Vec<String>, HiveNamespaceError> {
            Ok(self
                .tables
                .get(&(catalog.map(str::to_owned), database.to_owned()))
                .into_iter()
                .flatten()
                .map(|table| table.name.clone())
                .collect())
        }
        async fn get_table(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
        ) -> Result<HiveTable, HiveNamespaceError> {
            self.tables
                .get(&(catalog.map(str::to_owned), database.to_owned()))
                .into_iter()
                .flatten()
                .find(|item| item.name == table)
                .cloned()
                .ok_or_else(|| HiveNamespaceError::Metastore(format!("missing table {table}")))
        }
    }

    fn table(name: &str, lance: bool) -> HiveTable {
        HiveTable {
            name: name.to_owned(),
            location: Some(format!("hdfs://warehouse/{name}")),
            parameters: if lance {
                vec![("TABLE_TYPE".to_owned(), "LANCE".to_owned())]
            } else {
                Vec::new()
            },
        }
    }

    #[tokio::test]
    async fn hive2_matches_python_default_table_and_database_hierarchy() {
        let hms = StaticHms {
            databases: BTreeMap::from([(None, vec!["default".into(), "analytics".into()])]),
            tables: BTreeMap::from([(
                (None, "analytics".into()),
                vec![table("events", true), table("other", false)],
            )]),
            ..Default::default()
        };
        let namespace = HiveNamespace::new(
            HiveConfig::new(HiveVersion::Hive2, "thrift://hms", "/lake"),
            hms,
        );
        assert_eq!(
            namespace.list_namespaces(&[]).await.unwrap(),
            vec!["analytics"]
        );
        assert_eq!(
            namespace.list_tables(&[]).await.unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(
            namespace.parse_table(&["events".into()]).unwrap(),
            (None, "default".into(), "events".into())
        );
        assert_eq!(
            namespace.list_tables(&["analytics".into()]).await.unwrap(),
            vec!["events"]
        );
    }

    #[tokio::test]
    async fn hive3_matches_python_catalog_hierarchy_and_short_ids() {
        let hms = StaticHms {
            catalogs: vec!["hive".into(), "sales".into()],
            databases: BTreeMap::from([(
                Some("sales".into()),
                vec!["default".into(), "q3".into()],
            )]),
            ..Default::default()
        };
        let namespace = HiveNamespace::new(
            HiveConfig::new(HiveVersion::Hive3, "thrift://hms", "/lake"),
            hms,
        );
        assert_eq!(
            namespace.list_namespaces(&[]).await.unwrap(),
            vec!["hive", "sales"]
        );
        assert_eq!(
            namespace.list_namespaces(&["sales".into()]).await.unwrap(),
            vec!["q3"]
        );
        assert_eq!(
            namespace.list_tables(&["sales".into()]).await.unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(
            namespace
                .parse_table(&["q3".into(), "events".into()])
                .unwrap(),
            (Some("hive".into()), "q3".into(), "events".into())
        );
    }

    #[tokio::test]
    async fn hive4_uses_catalog_aware_ids_and_legacy_fallback_encoding() {
        let namespace = HiveNamespace::new(
            HiveConfig::new(HiveVersion::Hive4, "thrift://hms", "/lake"),
            StaticHms::default(),
        );
        assert_eq!(
            namespace
                .parse_table(&["sales".into(), "q3".into(), "events".into()])
                .unwrap(),
            (Some("sales".into()), "q3".into(), "events".into())
        );
        assert_eq!(
            namespace.legacy_database_name(Some("sales"), "q3"),
            "@sales#q3"
        );
    }

    #[test]
    fn public_hms_surface_is_read_only() {
        // This compile-time assertion documents that the public transport
        // interface exposes only get/list operations.
        fn accepts_read_transport<T: HmsReadTransport>() {}
        accepts_read_transport::<StaticHms>();
    }
}
