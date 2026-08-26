//! Hive 2 read compatibility.
//!
//! Hive 2 has no catalog RPC surface.  Namespace identifiers therefore map
//! directly to legacy database/table calls.  The request-style table fallback
//! is retained for HMS distributions that backport `get_table_req`.

use crate::{DEFAULT_CATALOG, HiveNamespaceMetadata};

use super::hms_sasl::{TableRead, legacy_table_args, request_table_args};

pub(crate) fn catalogs() -> Vec<String> {
    vec![DEFAULT_CATALOG.to_owned()]
}

pub(crate) fn catalog_metadata() -> HiveNamespaceMetadata {
    HiveNamespaceMetadata {
        location: None,
        comment: None,
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
    fn uses_database_only_legacy_identifiers() {
        assert_eq!(database_name(Some("ignored"), "db"), "db");
        let [TableRead::Legacy(legacy), TableRead::Request(request)] = table_reads("db", "tbl")
        else {
            panic!("Hive2 must keep legacy then request-style fallback");
        };
        assert!(legacy.windows(2).any(|field| field == [0, 1]));
        assert!(request.windows(2).any(|field| field == [0, 1]));
        assert!(!request.windows(2).any(|field| field == [0, 4]));
    }
}
