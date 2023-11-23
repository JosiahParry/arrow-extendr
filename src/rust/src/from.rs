use arrow::{datatypes::{Field, DataType, Schema}, ffi::{FFI_ArrowArray, FFI_ArrowSchema, self}, array::{ArrayData, make_array}, record_batch::RecordBatch};
use extendr_api::prelude::*;

use std::result::Result;
pub trait FromArrowRobj: Sized {
    fn from_arrow_robj(robj: &Robj) -> Result<Self, ErrArrowRobj>;
}

//https://github.com/apache/arrow-rs/blob/200e8c80084442d9579e00967e407cd83191565d/arrow/src/pyarrow.rs#L176

use arrow::error::ArrowError;
pub type ErrArrowRobj = ArrowError;


/// Gets the address of a nanoarrow object as a string `Robj`
pub fn nanoarrow_addr(robj: &Robj) -> Result<Robj, Error> {
    R!("nanoarrow::nanoarrow_pointer_addr_chr")
        .expect("`nanoarrow` must be installed")
        .as_function()
        .expect("`nanoarrow_pointer_addr_ch()` must be available")
        .call(pairlist!(robj))
}


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
            return Err(ErrArrowRobj::ParseError("did not find a `Field` or `nanoarrow_schema`".into()))
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
            return Err(ErrArrowRobj::ParseError("did not find a `DataType` or `nanoarrow_schema`".into()))
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
            return Err(ErrArrowRobj::ParseError("did not find a `Schema` or `nanoarrow_schema`".into()))
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

        let is_array = robj.inherits("Array");

        if !is_array {
            return Err(ErrArrowRobj::ParseError("did not find a `Array`".into()))
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

        // make the conversion through PyArrow's private API
        // this changes the pointer's memory and is thus unsafe.
        // In particular, `_export_to_c` can go out of bounds
        let _ = export_to_c.call(
            pairlist!(
                c_array_ptr.to_string(),
                c_schema_ptr.to_string()
            )
        );

        ffi::from_ffi(array, &schema)
    }
}



impl FromArrowRobj for RecordBatch {
    fn from_arrow_robj(robj: &Robj) -> Result<Self, ErrArrowRobj> {
        let is_rb = robj.inherits("RecordBatch");

        if !is_rb {
            return Err(ErrArrowRobj::ParseError("did not find a `RecordBatch`".into()))
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

        let _ = export_to_c.call(
            pairlist!(
                c_array_ptr.to_string(),
                c_schema_ptr.to_string()
            )
        );

        let res = ffi::from_ffi(array, &schema)?;
        let schema = Schema::try_from(&schema)?;
        
        let res_arrays = res.child_data().into_iter()
            .map(|xi| { make_array(xi.clone()) })
            .collect::<Vec<_>>();

        let res = RecordBatch::try_new(schema.into(), res_arrays)?;

        Ok(res)

    }
}