//! Hive 3 read compatibility.
//!
//! Python's Hive3 implementation probes `get_catalogs` and falls back to the
//! conventional `hive` catalog.  Its database and table lookups remain the
//! legacy HMS calls, so a catalog identifier is intentionally not encoded.

use crate::{DEFAULT_CATALOG, HiveNamespaceMetadata};

use super::hms_sasl::{TableRead, legacy_table_args, request_table_args};

pub(crate) fn fallback_catalogs() -> Vec<String> {
    vec![DEFAULT_CATALOG.to_owned()]
}

pub(crate) fn catalog_metadata(catalog: &str) -> HiveNamespaceMetadata {
    HiveNamespaceMetadata {
        location: None,
        comment: Some(format!("Catalog: {catalog}")),
        owner: None,
        parameters: vec![],
    }
}

pub(crate) fn database_name(_catalog: Option<&str>, database: &str) -> String {
    database.to_owned()
}

pub(crate) fn table_reads(database: &str, table: &str) -> [TableRead; 2] {
    [
        TableRead::Legacy(legacy_table_args(database, table)),
        TableRead::Request(request_table_args(database, table, None)),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_python_legacy_catalog_behavior() {
        assert_eq!(fallback_catalogs(), [DEFAULT_CATALOG]);
        assert_eq!(database_name(Some("analytics"), "db"), "db");
        assert_eq!(
            catalog_metadata("analytics").comment.as_deref(),
            Some("Catalog: analytics")
        );
    }
}
