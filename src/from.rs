//! Convert arrow-rs objects to an `Robj`
//!
//! ```ignore
//! fn array_from_r(field: Robj) -> Result<ArrayData> {
//!     ArrayData::from_arrow_robj(&field)?
//! }
//! ```
//!
//! `Robj`s from `{nanoarrow}` and `{arrow}` are both supported.
//!
//! | arrow-rs struct          |             R object                            |
//! | -------------------------| ----------------------------------------------- |
//! | `Field`                  |`nanoarrow_schema` or `arrow::Field`             |
//! | `Schema`                 |`nanoarrow_schema` or `arrow::Schema`            |
//! | `DataType`               |`nanoarrow_schema` or `arrow::DataType`          |
//! | `ArrayData`              |`nanoarrow_array` or `arrow::Array`              |
//! | `RecordBatch`            |`nanoarrow_array_stream` or `arrow::RecordBatch` |
//! | `ArrowArrayStreamReader` |`nanoarrow_array_stream`                         |
//!
//! ### Notes
//!
//! In the case of creating a `RecordBatch` from a `nanoarrow_array_stream` only
//! the first chunk is returned. If you expect more than one chunk, use `ArrowArrayStreamReader`.
//!

use arrow::{
    array::{make_array, ArrayData},
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    ffi::{self, FFI_ArrowArray, FFI_ArrowSchema},
    ffi_stream::{self, ArrowArrayStreamReader, FFI_ArrowArrayStream},
    record_batch::RecordBatch,
};

use extendr_api::prelude::*;
use std::result::Result;

/// Creates arrow-rs Structs from an Robj
///

pub trait FromArrowRobj: Sized {
    fn from_arrow_robj(robj: &Robj) -> Result<Self, ErrArrowRobj>;
}

pub type ErrArrowRobj = ArrowError;

/// Calls `nanoarrow::nanoarrow_pointer_addr_chr()`
///
/// Gets the address of a nanoarrow object as a string `Robj`
/// Requires `{nanoarrow}` to be installed.
pub fn nanoarrow_addr(robj: &Robj) -> Result<Robj, Error> {
    R!("nanoarrow::nanoarrow_pointer_addr_chr")
        .expect("`nanoarrow` must be installed")
        .as_function()
        .expect("`nanoarrow_pointer_addr_ch()` must be available")
        .call(pairlist!(robj))
}

/// Calls `nanoarrow::nanoarrow_pointer_export()`
///
/// Exports a nanoarrow pointer from R to C
/// Requires `{nanoarrow}` to be installed.
pub fn nanoarrow_export(source: &Robj, dest: String) -> Result<Robj, Error> {
    R!("nanoarrow::nanoarrow_pointer_export")
        .expect("`nanoarrow` must be installed")
        .as_function()
        .expect("`nanoarrow_pointer_export()` must be available")
        .call(pairlist!(source, dest))
}

impl FromArrowRobj for Field {
    fn from_arrow_robj(robj: &Robj) -> Result<Self, ErrArrowRobj> {
        // handle nanoarrow
        if robj.inherits("nanoarrow_schema") {
            let c_schema = FFI_ArrowSchema::empty();
            let c_schema_ptr = &c_schema as *const FFI_ArrowSchema as usize;

            let _ = nanoarrow_export(robj, c_schema_ptr.to_string());

            let field = Field::try_from(&c_schema)?;

            return Ok(field);
        }

        let is_field = robj.inherits("Field");

        if !(is_field) {
            return Err(ErrArrowRobj::ParseError(
                "did not find a `Field` or `nanoarrow_schema`".into(),
            ));
        }

        let export_to_c = robj
            .dollar("export_to_c")
            .expect("export_to_c() method to be available")
            .as_function()
            .unwrap();

        let c_schema = FFI_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema as usize;

        let _ = export_to_c.call(pairlist!(c_schema_ptr.to_string()));
        let field = Field::try_from(&c_schema)?;

        Ok(field)
    }
}

impl FromArrowRobj for DataType {
    fn from_arrow_robj(robj: &Robj) -> Result<Self, ErrArrowRobj> {
        if robj.inherits("nanoarrow_schema") {
            let c_schema = FFI_ArrowSchema::empty();
            let c_schema_ptr = &c_schema as *const FFI_ArrowSchema as usize;

            let _ = nanoarrow_export(robj, c_schema_ptr.to_string());

            let field = DataType::try_from(&c_schema)?;

            return Ok(field);
        }

        let is_datatype = robj.inherits("DataType");

        if !(is_datatype) {
            return Err(ErrArrowRobj::ParseError(
                "did not find a `DataType` or `nanoarrow_schema`".into(),
            ));
        }

        let export_to_c = robj
            .dollar("export_to_c")
            .expect("export_to_c() method to be available")
            .as_function()
            .unwrap();

        let c_schema = FFI_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema as usize;

        let _ = export_to_c.call(pairlist!(c_schema_ptr.to_string()));
        let data_type = DataType::try_from(&c_schema)?;

        Ok(data_type)
    }
}

impl FromArrowRobj for Schema {
    fn from_arrow_robj(robj: &Robj) -> Result<Self, ErrArrowRobj> {
        if robj.inherits("nanoarrow_schema") {
            let c_schema = FFI_ArrowSchema::empty();
            let c_schema_ptr = &c_schema as *const FFI_ArrowSchema as usize;

            let _ = nanoarrow_export(robj, c_schema_ptr.to_string());

            let field = Schema::try_from(&c_schema)?;

            return Ok(field);
        }

        let is_schema = robj.inherits("Schema");

        if !(is_schema) {
            return Err(ErrArrowRobj::ParseError(
                "did not find a `Schema` or `nanoarrow_schema`".into(),
            ));
        }

        let export_to_c = robj
            .dollar("export_to_c")
            .expect("export_to_c() method to be available")
            .as_function()
            .unwrap();

        let c_schema = FFI_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema as usize;

        let _ = export_to_c.call(pairlist!(c_schema_ptr.to_string()));
        let schema = Schema::try_from(&c_schema)?;

        Ok(schema)
    }
}

// https://github.com/apache/arrow-rs/blob/200e8c80084442d9579e00967e407cd83191565d/arrow/src/pyarrow.rs#L248
impl FromArrowRobj for ArrayData {
    fn from_arrow_robj(robj: &Robj) -> Result<Self, ErrArrowRobj> {
        if robj.inherits("nanoarrow_array") {
            let array = FFI_ArrowArray::empty();
            let schema = FFI_ArrowSchema::empty();

            let c_array_ptr = &array as *const FFI_ArrowArray as usize;
            let c_schema_ptr = &schema as *const FFI_ArrowSchema as usize;

            let robj_schema = R!("nanoarrow::infer_nanoarrow_schema")
                .unwrap()
                .as_function()
                .unwrap()
                .call(pairlist!(robj))
                .expect("unable to infer nanoarrow schema");

            let _ = nanoarrow_export(robj, c_array_ptr.to_string());
            let _ = nanoarrow_export(&robj_schema, c_schema_ptr.to_string());

            return unsafe { ffi::from_ffi(array, &schema) };
        }

        let is_array = robj.inherits("Array");

        if !is_array {
            return Err(ErrArrowRobj::ParseError("did not find a `Array`".into()));
        }

        // prepare a pointer to receive the Array struct
        let array = FFI_ArrowArray::empty();
        let schema = FFI_ArrowSchema::empty();

        let c_array_ptr = &array as *const FFI_ArrowArray as usize;
        let c_schema_ptr = &schema as *const FFI_ArrowSchema as usize;

        let export_to_c = robj
            .dollar("export_to_c")
            .expect("export_to_c() method to be available")
            .as_function()
            .unwrap();

        let _ = export_to_c.call(pairlist!(c_array_ptr.to_string(), c_schema_ptr.to_string()));

        unsafe { ffi::from_ffi(array, &schema) }
    }
}

/// If there are more than one RecordBatches in the stream, do not use this
/// Use ArrowStreamReader instead
impl FromArrowRobj for RecordBatch {
    fn from_arrow_robj(robj: &Robj) -> Result<Self, ErrArrowRobj> {
        if robj.inherits("nanoarrow_array_stream") {
            // we need to allocate an empty schema and fetch it from the record batch
            let stream = ffi_stream::FFI_ArrowArrayStream::empty();
            let c_stream_ptr = &stream as *const FFI_ArrowArrayStream as usize;

            let _ = nanoarrow_export(robj, c_stream_ptr.to_string());

            let res = ArrowArrayStreamReader::try_new(stream)?;
            let r2 = res.into_iter().map(|xi| xi.unwrap()).nth(0).unwrap();

            return Ok(r2);
        }

        let is_rb = robj.inherits("RecordBatch");

        if !is_rb {
            return Err(ErrArrowRobj::ParseError(
                "did not find a `RecordBatch` or `nanoarrow_array_stream`".into(),
            ));
        }

        // we need to allocate an empty schema and fetch it from the record batch
        let array = FFI_ArrowArray::empty();
        let schema = FFI_ArrowSchema::empty();

        let c_array_ptr = &array as *const FFI_ArrowArray as usize;
        let c_schema_ptr = &schema as *const FFI_ArrowSchema as usize;

        let export_to_c = robj
            .dollar("export_to_c")
            .expect("export_to_c() method to be available")
            .as_function()
            .unwrap();

        let _ = export_to_c.call(pairlist!(c_array_ptr.to_string(), c_schema_ptr.to_string()));

        let res = unsafe { ffi::from_ffi(array, &schema)? };
        let schema = Schema::try_from(&schema)?;

        let res_arrays = res
            .child_data()
            .into_iter()
            .map(|xi| make_array(xi.clone()))
            .collect::<Vec<_>>();

        let res = RecordBatch::try_new(schema.into(), res_arrays)?;

        Ok(res)
    }
}

impl FromArrowRobj for ArrowArrayStreamReader {
    fn from_arrow_robj(robj: &Robj) -> Result<Self, ErrArrowRobj> {
        // TODO arrow::RecordBatchStreamWriter
        if !robj.inherits("nanoarrow_array_stream") {
            return Err(ErrArrowRobj::ParseError(
                "did not find `nanoarrow_array_stream`".into(),
            ));
        }
        // we need to allocate an empty schema and fetch it from the record batch
        let stream = ffi_stream::FFI_ArrowArrayStream::empty();
        let c_stream_ptr = &stream as *const FFI_ArrowArrayStream as usize;

        let _ = nanoarrow_export(robj, c_stream_ptr.to_string());

        ArrowArrayStreamReader::try_new(stream)
    }
}
