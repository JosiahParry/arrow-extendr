use arrow::array::Float64Array;
use extendr_api::{prelude::*};

use arrow::array::Int32Array;

pub mod to;
use to::ToArrowRobj;
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
fn test_i32() -> Result<Robj> {
    let array = Int32Array::from(vec![Some(1), None, Some(3)]);
    array.to_arrow_robj()
}

#[extendr]
/// @export
fn test_f64() -> Result<Robj> {
    let array = Float64Array::from(vec![Some(1.0), None, Some(3.0)]);
    array.to_arrow_robj()
}

use arrow::datatypes::Field;
use arrow::datatypes::DataType;

#[extendr]
/// @export
// https://github.com/apache/arrow-rs/blob/200e8c80084442d9579e00967e407cd83191565d/arrow/src/pyarrow.rs#L201
fn test_field() -> Result<Robj> {
    let f = Field::new("field_name", DataType::Binary, true);
    f.to_arrow_robj()
}
// Macro to generate exports.
// This ensures exported functions are registered with R.
// See corresponding C code in `entrypoint.c`.
extendr_module! {
    mod arrowextendr;
    fn find_narrow;
    fn test_i32;
    fn test_f64;
    fn test_field;
}
