//! Hive 4 read compatibility.
//!
//! Hive 4 adds catalog-aware request objects.  It also preserves legacy HMS
//! calls by encoding the catalog/database as `@catalog#database`; this exactly
//! mirrors the Python namespace implementation's fallback behavior.

use crate::{DEFAULT_CATALOG, HiveNamespaceMetadata};

use super::hms_sasl::{TableRead, legacy_table_args, request_table_args};

pub(crate) fn catalog_pattern(catalog: Option<&str>) -> String {
    format!("@{}#", catalog.unwrap_or(DEFAULT_CATALOG))
}

pub(crate) fn legacy_database(catalog: Option<&str>, database: &str) -> String {
    format!("@{}#{database}", catalog.unwrap_or(DEFAULT_CATALOG))
}

pub(crate) fn catalog_request_args(catalog: &str) -> Vec<u8> {
    // get_catalog_args { 1: GetCatalogRequest catName }, where the nested
    // request has `1: string name` according to Hive 4's official IDL.
    let mut request = super::hms_sasl::string_field(1, catalog);
    request.push(super::hms_sasl::STOP);
    super::hms_sasl::struct_field(1, request)
}

pub(crate) fn table_reads(catalog: Option<&str>, database: &str, table: &str) -> [TableRead; 2] {
    let catalog = catalog.unwrap_or(DEFAULT_CATALOG);
    let legacy_database = legacy_database(Some(catalog), database);
    [
        // Prefer the current Hive4 request object, including catName field 4.
        TableRead::Request(request_table_args(database, table, Some(catalog))),
        // Then align with Python's legacy RPC fallback.
        TableRead::Legacy(legacy_table_args(&legacy_database, table)),
    ]
}

pub(crate) fn default_catalog_metadata() -> HiveNamespaceMetadata {
    HiveNamespaceMetadata {
        location: None,
        comment: Some("Default Hive Metastore catalog (legacy client)".to_owned()),
        owner: None,
        parameters: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_catalog_for_modern_and_legacy_read_apis() {
        assert_eq!(catalog_pattern(Some("analytics")), "@analytics#");
        assert_eq!(legacy_database(Some("analytics"), "db"), "@analytics#db");
        let [TableRead::Request(request), TableRead::Legacy(legacy)] =
            table_reads(Some("analytics"), "db", "tbl")
        else {
            panic!("Hive4 must prefer request API then use legacy fallback");
        };
        assert!(request.windows(2).any(|field| field == [0, 4]));
        assert!(
            legacy
                .windows(b"@analytics#db".len())
                .any(|value| value == b"@analytics#db")
        );
    }
}
