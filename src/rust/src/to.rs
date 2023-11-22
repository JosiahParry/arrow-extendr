use extendr_api::prelude::*;
use arrow::{
    array::{PrimitiveArray, Array},
    datatypes::ArrowPrimitiveType,
    ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema}
};

pub trait ToArrowRobj {
    fn to_arrow_robj(&self) -> Result<Robj>;
}

impl<T: ArrowPrimitiveType> ToArrowRobj for PrimitiveArray<T> {
    fn to_arrow_robj(&self) -> Result<Robj> {
        let data = self.into_data();

        // take array data and prepare for FFI 
        let (ffi_array, ffi_schema) = to_ffi(&data)
            .expect("success converting arrow data");

        // function from {arrow} R package to import an arrow array
        let import_from_c = R!("arrow::Array$import_from_c")
            .unwrap()
            .as_function()
            .unwrap();

        // extract array pointer. we need it as a string to be used by arrow R package
        let ffi_array_ptr = &ffi_array as *const FFI_ArrowArray as usize;
        let arry_addr_chr = ffi_array_ptr.to_string();

        // same deal but with the schema 
        let ffi_schema_ptr = &ffi_schema as *const FFI_ArrowSchema as usize;
        let schema_addr_chr = ffi_schema_ptr.to_string();

        // run it! 
        import_from_c.call(pairlist!(arry_addr_chr, schema_addr_chr))
    }
}

impl ToArrowRobj for Field {
    fn to_arrow_robj(&self) -> Result<Robj> {
        let ffi_schema = FFI_ArrowSchema::try_from(self).expect("Field is FFI compatible");
        let ffi_schema_ptr = &ffi_schema as *const FFI_ArrowSchema as usize;
        let schema_addr_chr = ffi_schema_ptr.to_string();

        // function from {arrow} R package to import an arrow field
        let import_from_c = R!("arrow::Field$import_from_c")
            .unwrap()
            .as_function()
            .unwrap();

        import_from_c.call(pairlist!(schema_addr_chr))
    }
}

use arrow::datatypes::Field;


// TODO 
// arrow::Field$import_from_c()
// arrow::RecordBatch$import_from_c()
// arrow::Schema$import_from_c()
// arrow:::DataType$import_from_c()