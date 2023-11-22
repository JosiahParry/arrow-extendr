use arrow::array::Array;
use arrow::array::PrimitiveArray;
use arrow::datatypes::ArrowPrimitiveType;
use extendr_api::{prelude::*};

use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, to_ffi};

use arrow::array::Int32Array;

// Find nanoarrow
#[extendr]
/// @export
fn find_narrow() {
    let pntr_addr_fn = find_namespaced_function("nanoarrow::nanoarrow_pointer_addr_chr")
        .expect("{nanoarrow} to be found");

    rprintln!("{:?}", pntr_addr_fn);

    let pntr_addr_chr = R!("nanoarrow::nanoarrow_pointer_addr_chr")
        .unwrap()
        .as_function()
        .unwrap();

    rprintln!("{:?}", pntr_addr_chr);

}


#[extendr]
/// @export
fn export_array() -> Robj {
    let array = Int32Array::from(vec![Some(1), None, Some(3)]);
    let data = array.into_data();
    let (ffi_array, ffi_schema) = to_ffi(&data)
        .expect("success converting arrow data");

    let ffi_array_ptr = &ffi_array as *const FFI_ArrowArray as usize;
    let arry_addr_chr = ffi_array_ptr.to_string();

    let ffi_scehma_ptr = &ffi_schema as *const FFI_ArrowSchema as usize;
    let schema_addr_chr = ffi_scehma_ptr.to_string();

    let import_from_c = R!("arrow::Array$import_from_c")
        .unwrap()
        .as_function()
        .unwrap();

    let res = import_from_c.call(pairlist!(arry_addr_chr, schema_addr_chr));

    res.unwrap()
}


#[extendr]
/// @export
fn toarrow_trait() -> Result<Robj> {
    let array = Int32Array::from(vec![Some(1), None, Some(3)]);
    array.to_arrow_robj()
}

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
        let ffi_scehma_ptr = &ffi_schema as *const FFI_ArrowSchema as usize;
        let schema_addr_chr = ffi_scehma_ptr.to_string();

        // run it! 
        import_from_c.call(pairlist!(arry_addr_chr, schema_addr_chr))
    }
}

// Macro to generate exports.
// This ensures exported functions are registered with R.
// See corresponding C code in `entrypoint.c`.
extendr_module! {
    mod arrowextendr;
    fn find_narrow;
    fn export_array;
    fn toarrow_trait;
}
