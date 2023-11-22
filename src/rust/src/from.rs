use arrow::{datatypes::{Field, DataType}, ffi::FFI_ArrowSchema};
use extendr_api::prelude::*;

use std::result::Result;
pub trait FromArrowRobj: Sized {
    fn from_arrow_robj(robj: &Robj) -> Result<Self, ErrArrowRobj>;
}

//https://github.com/apache/arrow-rs/blob/200e8c80084442d9579e00967e407cd83191565d/arrow/src/pyarrow.rs#L176

use arrow::error::ArrowError;
pub type ErrArrowRobj = ArrowError;


impl FromArrowRobj for Field {
    fn from_arrow_robj(robj: &Robj) -> Result<Self, ErrArrowRobj> {

        let is_field = robj.inherits("Field");

        if !(is_field) {
            return Err(ErrArrowRobj::ParseError("did not find a `Field`".into()))
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

        let is_datatype = robj.inherits("DataType");

        if !(is_datatype) {
            return Err(ErrArrowRobj::ParseError("did not find a `DataType`".into()))
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

