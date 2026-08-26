# Rust Hive namespace

`lance-namespace-hive` is a read-only Rust counterpart to the Python Hive2,
Hive3, and Hive4 implementations. It implements the local Lance Rust
`LanceNamespace` trait behind the `lance-namespace-api` feature and keeps the
version-specific contract independent of generated Hive Thrift bindings.

The versioned request behavior is intentionally split instead of being hidden
behind a single generic switch:

| Module | Python-aligned behavior |
| --- | --- |
| `hive2.rs` | database-only legacy identifiers; request-style table fallback for backported servers |
| `hive3.rs` | probes catalog listing with a `hive` fallback; database/table calls remain legacy |
| `hive4.rs` | catalog-aware `GetTableRequest.catName` and `GetCatalogRequest`, then `@catalog#database` legacy fallback |
| `hms_sasl.rs` | shared GSSAPI/SASL negotiation, Thrift framing, bounded parser, and no-write transport |

The default `hms-sasl` feature provides a native `HmsSaslTransport`. It uses
the platform GSSAPI library and the process's existing Kerberos credential
cache; it neither reads a keytab nor implements Kerberos cryptography.
`bddx-t2` confirms that the deployment needs `hive.metastore.sasl.enabled=true`
and a `hive/_HOST` service principal. Table data is never accessed through
JVM/libhdfs/QBFS: callers open returned locations using the internal Lance
Rust `hdfs-native` feature.

No create, alter, register, drop, or deregister method exists in the public
HMS transport. Mutating `LanceNamespace` methods use the trait default and
return `Unsupported` without sending an RPC.

Run the contract tests with:

```bash
cargo test --features lance-namespace-api
```

Run each compatible read mode against a secured HMS endpoint:

```bash
for version in 2 3 4; do
  cargo run --bin hms-read -- \
    --uri thrift://host:9083 \
    --database lance_default \
    --hive-version "$version"
done
```

To perform the same read-only smoke test against an HMS endpoint, with an
already-valid ticket cache:

```bash
cargo run --bin hms-read -- \
  --uri thrift://host:9083 \
  --database lance_default \
  --hive-version 2
```

The diagnostic contains only `get_all_tables`, `get_table`, and the
request-style `get_table_req` compatibility fallback. It has no HMS write
RPC path.
