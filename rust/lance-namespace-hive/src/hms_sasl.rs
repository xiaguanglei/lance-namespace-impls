//! Native, read-only Hive Metastore Thrift transport.
//!
//! This module deliberately implements the smallest HMS read surface needed by
//! the namespace.  It uses the platform GSSAPI implementation (MIT, Heimdal,
//! or Apple's framework) and the *existing* credential cache.  No keytab is
//! read, no credential is stored, and no HMS mutation RPC exists in this file.

use std::{
    io::{Read, Write},
    net::TcpStream,
    time::Duration,
};

use async_trait::async_trait;
use libgssapi::{
    context::{ClientCtx, CtxFlags, SecurityContext},
    credential::{Cred, CredUsage},
    name::Name,
    oid::{GSS_MECH_KRB5, GSS_NT_HOSTBASED_SERVICE, OidSet},
};

use crate::{
    HiveConfig, HiveNamespaceError, HiveNamespaceMetadata, HiveTable, HiveVersion,
    HmsReadTransport, hive2, hive3, hive4,
};

const START: u8 = 1;
const OK: u8 = 2;
const COMPLETE: u8 = 5;
const MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;

pub(crate) const STOP: u8 = 0;
const BOOL: u8 = 2;
const I32: u8 = 8;
const STRING: u8 = 11;
const STRUCT: u8 = 12;
const MAP: u8 = 13;
const SET: u8 = 14;
const LIST: u8 = 15;
const MESSAGE_EXCEPTION: u8 = 3;

/// Version-specific table lookup plan.  Every variant is a read-only HMS
/// method and has a parser matched to that method's result type.
pub(crate) enum TableRead {
    Legacy(Vec<u8>),
    Request(Vec<u8>),
}

/// A native HMS endpoint.  New calls create a new SASL-authenticated TCP
/// connection.  That makes the transport `Sync` and prevents credential or
/// sequence state from being shared across callers.
#[derive(Clone, Debug)]
pub struct HmsSaslTransport {
    host: String,
    port: u16,
    service: String,
    version: HiveVersion,
    timeout: Duration,
}

impl HmsSaslTransport {
    pub fn from_config(config: &HiveConfig) -> Result<Self, HiveNamespaceError> {
        let (host, port) = parse_thrift_uri(&config.uri)?;
        Ok(Self {
            host,
            port,
            service: config.kerberos_service_name.clone(),
            version: config.version,
            timeout: Duration::from_secs(20),
        })
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    async fn call(
        &self,
        method: &'static str,
        args: Vec<u8>,
    ) -> Result<Vec<u8>, HiveNamespaceError> {
        let endpoint = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = HmsConnection::connect(&endpoint)?;
            connection.call(method, args)
        })
        .await
        .map_err(|error| HiveNamespaceError::Metastore(format!("HMS worker failed: {error}")))?
    }
}

#[async_trait]
impl HmsReadTransport for HmsSaslTransport {
    async fn list_catalogs(&self) -> Result<Vec<String>, HiveNamespaceError> {
        match self.version {
            HiveVersion::Hive2 => Ok(hive2::catalogs()),
            HiveVersion::Hive3 => {
                match parse_catalog_names(&self.call("get_catalogs", vec![STOP]).await?) {
                    Ok(names) => Ok(names),
                    Err(error) if unknown_method(&error) => Ok(hive3::fallback_catalogs()),
                    Err(error) => Err(error),
                }
            }
            HiveVersion::Hive4 => {
                match parse_catalog_names(&self.call("get_catalogs", vec![STOP]).await?) {
                    Ok(names) => Ok(names),
                    Err(error) if unknown_method(&error) => Ok(hive3::fallback_catalogs()),
                    Err(error) => Err(error),
                }
            }
        }
    }

    async fn get_catalog(
        &self,
        catalog: &str,
    ) -> Result<HiveNamespaceMetadata, HiveNamespaceError> {
        match self.version {
            HiveVersion::Hive2 => Ok(hive2::catalog_metadata()),
            // Python Hive3 deliberately synthesizes catalog metadata; it does
            // not call a catalog RPC for this version.
            HiveVersion::Hive3 => Ok(hive3::catalog_metadata(catalog)),
            HiveVersion::Hive4 => match parse_catalog_result(
                &self
                    .call("get_catalog", hive4::catalog_request_args(catalog))
                    .await?,
            ) {
                Ok(metadata) => Ok(metadata),
                Err(error) if unknown_method(&error) && catalog.eq_ignore_ascii_case("hive") => {
                    Ok(hive4::default_catalog_metadata())
                }
                Err(error) => Err(error),
            },
        }
    }

    async fn list_databases(
        &self,
        catalog: Option<&str>,
    ) -> Result<Vec<String>, HiveNamespaceError> {
        let response = match self.version {
            HiveVersion::Hive2 | HiveVersion::Hive3 => {
                self.call("get_all_databases", vec![STOP]).await?
            }
            HiveVersion::Hive4 => {
                self.call(
                    "get_databases",
                    string_field(1, &hive4::catalog_pattern(catalog)),
                )
                .await?
            }
        };
        parse_string_list_result(&response)
    }

    async fn get_database(
        &self,
        catalog: Option<&str>,
        database: &str,
    ) -> Result<HiveNamespaceMetadata, HiveNamespaceError> {
        let database = match self.version {
            HiveVersion::Hive2 => hive2::database_name(catalog, database),
            HiveVersion::Hive3 => hive3::database_name(catalog, database),
            HiveVersion::Hive4 => hive4::legacy_database(catalog, database),
        };
        parse_database_result(
            &self
                .call("get_database", string_field(1, &database))
                .await?,
        )
    }

    async fn list_tables(
        &self,
        catalog: Option<&str>,
        database: &str,
    ) -> Result<Vec<String>, HiveNamespaceError> {
        let database = match self.version {
            HiveVersion::Hive2 => hive2::database_name(catalog, database),
            HiveVersion::Hive3 => hive3::database_name(catalog, database),
            HiveVersion::Hive4 => hive4::legacy_database(catalog, database),
        };
        parse_string_list_result(
            &self
                .call("get_all_tables", string_field(1, &database))
                .await?,
        )
    }

    async fn get_table(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<HiveTable, HiveNamespaceError> {
        let reads = match self.version {
            HiveVersion::Hive2 => hive2::table_reads(database, table),
            HiveVersion::Hive3 => hive3::table_reads(database, table),
            HiveVersion::Hive4 => hive4::table_reads(catalog, database, table),
        };
        let mut unsupported = None;
        for read in reads {
            let result = match read {
                TableRead::Legacy(args) => self
                    .call("get_table", args)
                    .await
                    .map(|response| parse_table_result(&response)),
                TableRead::Request(args) => self
                    .call("get_table_req", args)
                    .await
                    .map(|response| parse_table_req_result(&response)),
            };
            match result {
                Ok(table) => return table,
                Err(error) if unknown_method(&error) => unsupported = Some(error),
                Err(error) => return Err(error),
            }
        }
        Err(unsupported.unwrap_or_else(|| {
            HiveNamespaceError::Metastore("no supported read-only HMS table RPC".to_owned())
        }))
    }
}

struct HmsConnection {
    stream: TcpStream,
}

impl HmsConnection {
    fn connect(endpoint: &HmsSaslTransport) -> Result<Self, HiveNamespaceError> {
        let mut stream =
            TcpStream::connect((endpoint.host.as_str(), endpoint.port)).map_err(io_error)?;
        stream
            .set_read_timeout(Some(endpoint.timeout))
            .map_err(io_error)?;
        stream
            .set_write_timeout(Some(endpoint.timeout))
            .map_err(io_error)?;
        negotiate_gssapi(&mut stream, &endpoint.service, &endpoint.host)?;
        Ok(Self { stream })
    }

    fn call(&mut self, method: &str, args: Vec<u8>) -> Result<Vec<u8>, HiveNamespaceError> {
        let request = message(method, args);
        write_frame(&mut self.stream, &request)?;
        let response = read_frame(&mut self.stream)?;
        let (_, message_type, _) = MessageReader::new(&response).header()?;
        if message_type == MESSAGE_EXCEPTION {
            return Err(parse_application_exception(&response));
        }
        Ok(response)
    }
}

fn negotiate_gssapi(
    stream: &mut TcpStream,
    service: &str,
    host: &str,
) -> Result<(), HiveNamespaceError> {
    let target = Name::new(
        format!("{service}@{host}").as_bytes(),
        Some(GSS_NT_HOSTBASED_SERVICE),
    )
    .map_err(gss_error)?;
    let mechs = OidSet::singleton(GSS_MECH_KRB5).map_err(gss_error)?;
    let credentials =
        Cred::acquire(None, None, CredUsage::Initiate, Some(&mechs)).map_err(gss_error)?;
    let mut context = ClientCtx::new(
        Some(credentials),
        target,
        CtxFlags::GSS_C_MUTUAL_FLAG,
        Some(GSS_MECH_KRB5),
    );

    write_sasl(stream, START, b"GSSAPI")?;
    let token = context
        .step(None, None)
        .map_err(gss_error)?
        .ok_or_else(|| {
            HiveNamespaceError::Metastore("GSSAPI produced no initial token".to_owned())
        })?;
    write_sasl(stream, OK, &token)?;

    loop {
        let (status, server_token) = read_sasl(stream)?;
        if status != OK {
            return Err(sasl_status(status, &server_token));
        }
        match context.step(Some(&server_token), None).map_err(gss_error)? {
            Some(token) => write_sasl(stream, OK, &token)?,
            None => {
                write_sasl(stream, OK, &[])?;
                break;
            }
        }
    }

    let (status, server_wrap) = read_sasl(stream)?;
    if status != OK {
        return Err(sasl_status(status, &server_wrap));
    }
    // Request the authentication-only layer (QOP auth).  The subsequent HMS
    // messages therefore remain normal framed Thrift payloads, matching the
    // server's default `auth` protection level.
    let _server_layers = context.unwrap(&server_wrap).map_err(gss_error)?;
    let selected_layer = context.wrap(false, &[1, 0, 0, 0]).map_err(gss_error)?;
    write_sasl(stream, COMPLETE, &selected_layer)?;
    let (status, payload) = read_sasl(stream)?;
    if status != COMPLETE {
        return Err(sasl_status(status, &payload));
    }
    Ok(())
}

fn write_sasl(
    stream: &mut TcpStream,
    status: u8,
    payload: &[u8],
) -> Result<(), HiveNamespaceError> {
    stream.write_all(&[status]).map_err(io_error)?;
    stream
        .write_all(&(payload.len() as u32).to_be_bytes())
        .map_err(io_error)?;
    stream.write_all(payload).map_err(io_error)?;
    stream.flush().map_err(io_error)
}

fn read_sasl(stream: &mut TcpStream) -> Result<(u8, Vec<u8>), HiveNamespaceError> {
    let mut header = [0; 5];
    stream.read_exact(&mut header).map_err(io_error)?;
    let len = u32::from_be_bytes(header[1..5].try_into().expect("slice is four bytes")) as usize;
    let payload = read_exact(stream, len)?;
    Ok((header[0], payload))
}

fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> Result<(), HiveNamespaceError> {
    stream
        .write_all(&(payload.len() as u32).to_be_bytes())
        .map_err(io_error)?;
    stream.write_all(payload).map_err(io_error)?;
    stream.flush().map_err(io_error)
}

fn read_frame(stream: &mut TcpStream) -> Result<Vec<u8>, HiveNamespaceError> {
    let mut header = [0; 4];
    stream.read_exact(&mut header).map_err(io_error)?;
    read_exact(stream, u32::from_be_bytes(header) as usize)
}

fn read_exact(stream: &mut TcpStream, len: usize) -> Result<Vec<u8>, HiveNamespaceError> {
    if len > MAX_FRAME_BYTES {
        return Err(HiveNamespaceError::Metastore(format!(
            "HMS frame exceeds {MAX_FRAME_BYTES} bytes"
        )));
    }
    let mut payload = vec![0; len];
    stream.read_exact(&mut payload).map_err(io_error)?;
    Ok(payload)
}

fn message(method: &str, mut args: Vec<u8>) -> Vec<u8> {
    if !args.ends_with(&[STOP]) {
        args.push(STOP);
    }
    let mut body = Vec::new();
    body.extend((-2_147_418_111_i32).to_be_bytes()); // version 1 + CALL
    write_string(&mut body, method);
    body.extend(0_i32.to_be_bytes());
    body.extend(args);
    body
}

pub(crate) fn string_field(id: i16, value: &str) -> Vec<u8> {
    let mut output = vec![STRING];
    output.extend(id.to_be_bytes());
    write_string(&mut output, value);
    output
}

pub(crate) fn struct_field(id: i16, value: Vec<u8>) -> Vec<u8> {
    let mut output = vec![STRUCT];
    output.extend(id.to_be_bytes());
    output.extend(value);
    output
}

pub(crate) fn legacy_table_args(database: &str, table: &str) -> Vec<u8> {
    let mut args = string_field(1, database);
    args.extend(string_field(2, table));
    args.push(STOP);
    args
}

pub(crate) fn request_table_args(database: &str, table: &str, catalog: Option<&str>) -> Vec<u8> {
    // `get_table_req_args { 1: GetTableRequest req }`.  Hive4 adds optional
    // `4: catName`; old Hive 2/3 servers skip that unknown optional field.
    let mut request = string_field(1, database);
    request.extend(string_field(2, table));
    if let Some(catalog) = catalog {
        request.extend(string_field(4, catalog));
    }
    request.push(STOP);
    let mut args = struct_field(1, request);
    args.push(STOP);
    args
}

fn write_string(output: &mut Vec<u8>, value: &str) {
    output.extend((value.len() as i32).to_be_bytes());
    output.extend(value.as_bytes());
}

fn parse_string_list_result(response: &[u8]) -> Result<Vec<String>, HiveNamespaceError> {
    let mut reader = MessageReader::new(response);
    reader.header()?;
    let (ty, id) = reader.field()?;
    if ty == STOP {
        return Ok(vec![]);
    }
    if id != 0 || ty != LIST {
        return Err(reader.unexpected("list<string> success result"));
    }
    reader.string_list()
}

fn parse_catalog_names(response: &[u8]) -> Result<Vec<String>, HiveNamespaceError> {
    let mut reader = MessageReader::new(response);
    reader.header()?;
    let (ty, id) = reader.field()?;
    if id != 0 || ty != STRUCT {
        return Err(reader.unexpected("GetCatalogsResponse success result"));
    }
    let (ty, id) = reader.field()?;
    if id != 1 || ty != LIST {
        return Err(reader.unexpected("GetCatalogsResponse.names"));
    }
    reader.string_list()
}

fn parse_catalog_result(response: &[u8]) -> Result<HiveNamespaceMetadata, HiveNamespaceError> {
    // Hive4: result { 0: GetCatalogResponse }, then
    // GetCatalogResponse { 1: Catalog }.
    let mut reader = result_struct(response, "GetCatalogResponse")?;
    let (ty, id) = reader.field()?;
    if id != 1 || ty != STRUCT {
        return Err(reader.unexpected("GetCatalogResponse.catalog"));
    }
    let mut name = None;
    let mut description = None;
    let mut location = None;
    loop {
        let (ty, id) = reader.field()?;
        if ty == STOP {
            break;
        }
        match (id, ty) {
            (1, STRING) => name = Some(reader.string()?),
            (2, STRING) => description = Some(reader.string()?),
            (3, STRING) => location = Some(reader.string()?),
            _ => reader.skip(ty)?,
        }
    }
    let _ = name;
    Ok(HiveNamespaceMetadata {
        location,
        comment: description,
        owner: None,
        parameters: vec![],
    })
}

fn parse_database_result(response: &[u8]) -> Result<HiveNamespaceMetadata, HiveNamespaceError> {
    let mut reader = result_struct(response, "Database")?;
    let mut description = None;
    let mut location = None;
    let mut parameters = vec![];
    let mut owner = None;
    loop {
        let (ty, id) = reader.field()?;
        if ty == STOP {
            break;
        }
        match (id, ty) {
            (2, STRING) => description = Some(reader.string()?),
            (3, STRING) => location = Some(reader.string()?),
            (4, MAP) => parameters = reader.string_map()?,
            (6, STRING) => owner = Some(reader.string()?),
            _ => reader.skip(ty)?,
        }
    }
    Ok(HiveNamespaceMetadata {
        location,
        comment: description,
        owner,
        parameters,
    })
}

fn parse_table_result(response: &[u8]) -> Result<HiveTable, HiveNamespaceError> {
    let mut reader = result_struct(response, "Table")?;
    parse_table_fields(&mut reader)
}

fn parse_table_req_result(response: &[u8]) -> Result<HiveTable, HiveNamespaceError> {
    let mut reader = result_struct(response, "GetTableResult")?;
    let (ty, id) = reader.field()?;
    if id != 1 || ty != STRUCT {
        return Err(reader.unexpected("GetTableResult.table"));
    }
    parse_table_fields(&mut reader)
}

fn parse_table_fields(reader: &mut MessageReader<'_>) -> Result<HiveTable, HiveNamespaceError> {
    let mut name = None;
    let mut location = None;
    let mut parameters = vec![];
    loop {
        let (ty, id) = reader.field()?;
        if ty == STOP {
            break;
        }
        match (id, ty) {
            (1, STRING) => name = Some(reader.string()?),
            (7, STRUCT) => location = parse_storage_descriptor(reader)?,
            (9, MAP) => parameters = reader.string_map()?,
            _ => reader.skip(ty)?,
        }
    }
    let name = name.ok_or_else(|| {
        HiveNamespaceError::Metastore("HMS get_table response has no table name".to_owned())
    })?;
    Ok(HiveTable {
        name,
        location,
        parameters,
    })
}

fn parse_storage_descriptor(
    reader: &mut MessageReader<'_>,
) -> Result<Option<String>, HiveNamespaceError> {
    let mut location = None;
    loop {
        let (ty, id) = reader.field()?;
        if ty == STOP {
            break;
        }
        if id == 2 && ty == STRING {
            location = Some(reader.string()?);
        } else {
            reader.skip(ty)?;
        }
    }
    Ok(location)
}

fn result_struct<'a>(
    response: &'a [u8],
    expected: &'static str,
) -> Result<MessageReader<'a>, HiveNamespaceError> {
    let mut reader = MessageReader::new(response);
    reader.header()?;
    let (ty, id) = reader.field()?;
    if id != 0 || ty != STRUCT {
        return Err(reader.unexpected(expected));
    }
    Ok(reader)
}

fn parse_application_exception(response: &[u8]) -> HiveNamespaceError {
    let mut reader = MessageReader::new(response);
    if reader.header().is_err() {
        return HiveNamespaceError::Metastore("malformed HMS exception".to_owned());
    }
    let mut message = "HMS application exception".to_owned();
    while let Ok((ty, id)) = reader.field() {
        if ty == STOP {
            break;
        }
        if id == 1 && ty == STRING {
            if let Ok(value) = reader.string() {
                message = value;
            }
        } else if reader.skip(ty).is_err() {
            break;
        }
    }
    HiveNamespaceError::Metastore(message)
}

fn unknown_method(error: &HiveNamespaceError) -> bool {
    matches!(error, HiveNamespaceError::Metastore(message) if message.contains("Unknown function") || message.contains("Invalid method name") || message.contains("unknown method"))
}

fn parse_thrift_uri(uri: &str) -> Result<(String, u16), HiveNamespaceError> {
    let authority = uri.strip_prefix("thrift://").ok_or_else(|| {
        HiveNamespaceError::Metastore(format!(
            "unsupported HMS URI {uri:?}; expected thrift://host:port"
        ))
    })?;
    let (host, port) = authority
        .rsplit_once(':')
        .ok_or_else(|| HiveNamespaceError::Metastore(format!("HMS URI {uri:?} has no port")))?;
    let port = port
        .parse()
        .map_err(|_| HiveNamespaceError::Metastore(format!("invalid HMS port in {uri:?}")))?;
    Ok((host.trim_matches(['[', ']']).to_owned(), port))
}

fn io_error(error: std::io::Error) -> HiveNamespaceError {
    HiveNamespaceError::Metastore(error.to_string())
}
fn gss_error(error: libgssapi::error::Error) -> HiveNamespaceError {
    HiveNamespaceError::Metastore(format!("GSSAPI: {error}"))
}
fn sasl_status(status: u8, payload: &[u8]) -> HiveNamespaceError {
    HiveNamespaceError::Metastore(format!(
        "HMS SASL status {status}: {}",
        String::from_utf8_lossy(payload)
    ))
}

struct MessageReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> MessageReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn header(&mut self) -> Result<(String, u8, i32), HiveNamespaceError> {
        let first = self.i32()?;
        if first < 0 {
            let message_type = (first & 0xff) as u8;
            let name = self.string()?;
            Ok((name, message_type, self.i32()?))
        } else {
            let name = self.string_with_len(first)?;
            let message_type = self.byte()?;
            Ok((name, message_type, self.i32()?))
        }
    }

    fn field(&mut self) -> Result<(u8, i16), HiveNamespaceError> {
        let ty = self.byte()?;
        if ty == STOP {
            return Ok((ty, 0));
        }
        Ok((ty, self.i16()?))
    }

    fn string_list(&mut self) -> Result<Vec<String>, HiveNamespaceError> {
        let ty = self.byte()?;
        let count = self.i32()?;
        if ty != STRING || count < 0 {
            return Err(self.unexpected("list<string>"));
        }
        (0..count).map(|_| self.string()).collect()
    }

    fn string_map(&mut self) -> Result<Vec<(String, String)>, HiveNamespaceError> {
        let key = self.byte()?;
        let value = self.byte()?;
        let count = self.i32()?;
        if key != STRING || value != STRING || count < 0 {
            return Err(self.unexpected("map<string,string>"));
        }
        (0..count)
            .map(|_| Ok((self.string()?, self.string()?)))
            .collect()
    }

    fn skip(&mut self, ty: u8) -> Result<(), HiveNamespaceError> {
        match ty {
            STOP => Ok(()),
            BOOL => {
                self.take(1)?;
                Ok(())
            }
            3 => {
                self.take(1)?;
                Ok(())
            }
            4 => {
                self.take(8)?;
                Ok(())
            }
            6 => {
                self.take(2)?;
                Ok(())
            }
            I32 => {
                self.take(4)?;
                Ok(())
            }
            10 => {
                self.take(8)?;
                Ok(())
            }
            STRING => {
                let len = self.i32()?;
                self.take_len(len)?;
                Ok(())
            }
            STRUCT => loop {
                let (inner, _) = self.field()?;
                if inner == STOP {
                    break Ok(());
                }
                self.skip(inner)?;
            },
            MAP => {
                let key = self.byte()?;
                let value = self.byte()?;
                let count = self.i32()?;
                if count < 0 {
                    return Err(self.unexpected("non-negative map length"));
                }
                for _ in 0..count {
                    self.skip(key)?;
                    self.skip(value)?;
                }
                Ok(())
            }
            LIST | SET => {
                let inner = self.byte()?;
                let count = self.i32()?;
                if count < 0 {
                    return Err(self.unexpected("non-negative collection length"));
                }
                for _ in 0..count {
                    self.skip(inner)?;
                }
                Ok(())
            }
            _ => Err(self.unexpected("known thrift type")),
        }
    }

    fn string(&mut self) -> Result<String, HiveNamespaceError> {
        let len = self.i32()?;
        self.string_with_len(len)
    }
    fn string_with_len(&mut self, len: i32) -> Result<String, HiveNamespaceError> {
        let value = self.take_len(len)?;
        String::from_utf8(value.to_vec()).map_err(|error| {
            HiveNamespaceError::Metastore(format!("HMS returned non-UTF8 string: {error}"))
        })
    }
    fn i16(&mut self) -> Result<i16, HiveNamespaceError> {
        Ok(i16::from_be_bytes(
            self.take(2)?.try_into().expect("two bytes"),
        ))
    }
    fn i32(&mut self) -> Result<i32, HiveNamespaceError> {
        Ok(i32::from_be_bytes(
            self.take(4)?.try_into().expect("four bytes"),
        ))
    }
    fn byte(&mut self) -> Result<u8, HiveNamespaceError> {
        Ok(self.take(1)?[0])
    }
    fn take_len(&mut self, len: i32) -> Result<&'a [u8], HiveNamespaceError> {
        if len < 0 {
            return Err(self.unexpected("non-negative length"));
        }
        self.take(len as usize)
    }
    fn take(&mut self, len: usize) -> Result<&'a [u8], HiveNamespaceError> {
        let end = self
            .offset
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| self.unexpected("complete thrift frame"))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }
    fn unexpected(&self, expected: &str) -> HiveNamespaceError {
        HiveNamespaceError::Metastore(format!(
            "malformed HMS Thrift response; expected {expected}"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_hive4_legacy_uri() {
        assert_eq!(
            parse_thrift_uri("thrift://metastore.example:9083").unwrap(),
            ("metastore.example".to_owned(), 9083)
        );
    }

    #[test]
    fn parses_string_list_result() {
        let mut response = Vec::new();
        response.extend((-2_147_418_110_i32).to_be_bytes()); // version 1 + REPLY
        write_string(&mut response, "get_all_tables");
        response.extend(0_i32.to_be_bytes());
        response.extend([LIST, 0, 0]);
        response.push(STRING);
        response.extend(2_i32.to_be_bytes());
        write_string(&mut response, "a");
        write_string(&mut response, "b");
        response.push(STOP);
        assert_eq!(parse_string_list_result(&response).unwrap(), ["a", "b"]);
    }

    #[test]
    fn parses_hive4_get_catalog_response() {
        let mut response = Vec::new();
        response.extend((-2_147_418_110_i32).to_be_bytes()); // version 1 + REPLY
        write_string(&mut response, "get_catalog");
        response.extend(0_i32.to_be_bytes());
        response.extend([STRUCT, 0, 0]); // result.success: GetCatalogResponse
        response.extend([STRUCT, 0, 1]); // GetCatalogResponse.catalog
        response.extend(string_field(1, "analytics"));
        response.extend(string_field(2, "Analytics tables"));
        response.extend(string_field(3, "hdfs://warehouse/analytics"));
        response.push(STOP); // Catalog
        response.push(STOP); // GetCatalogResponse
        response.push(STOP); // result
        let catalog = parse_catalog_result(&response).unwrap();
        assert_eq!(catalog.comment.as_deref(), Some("Analytics tables"));
        assert_eq!(
            catalog.location.as_deref(),
            Some("hdfs://warehouse/analytics")
        );
    }
}
