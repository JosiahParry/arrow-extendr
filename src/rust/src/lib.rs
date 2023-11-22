use arrow::array::{Float64Array, ArrayData};


use extendr_api::{prelude::*};

use arrow::array::Int32Array;

pub mod to;
use to::ToArrowRobj;

pub mod from;
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
use arrow::datatypes::{DataType, TimeUnit};

#[extendr]
/// @export
// https://github.com/apache/arrow-rs/blob/200e8c80084442d9579e00967e407cd83191565d/arrow/src/pyarrow.rs#L201
fn test_field() -> Result<Robj> {
    let f = Field::new("field_name", DataType::Binary, true);
    f.to_arrow_robj()
}


use arrow::{datatypes::Schema, record_batch::RecordBatch};
use std::sync::Arc;

#[extendr]
/// @export
fn test_record_batch() -> Result<Robj>{
    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false)
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(id_array)]
    ).unwrap();

    batch.to_arrow_robj()
}


#[extendr]
/// @export
fn test_schema() -> Result<Robj> {
    let field_a = Field::new("a", DataType::Date64, false);
    let field_b = Field::new("a", DataType::Int64, true);
    let field_c = Field::new("b", DataType::Boolean, false);

    let schema = Schema::new(vec![field_a, field_b, field_c]);
    schema.to_arrow_robj()
}

#[extendr]
/// @export
fn test_datatype() -> Result<Robj> {
    let dt = DataType::Timestamp(TimeUnit::Second, None);
    dt.to_arrow_robj()
}


// From testing
use from::FromArrowRobj;

#[extendr]
/// @export
fn test_from_field(field: Robj) {
    let f = Field::from_arrow_robj(&field);
    rprintln!("{:#?}", f);
}

#[extendr]
/// @export
fn test_from_datatype(field: Robj) {
    let f = DataType::from_arrow_robj(&field);
    rprintln!("{:#?}", f);
}

#[extendr]
/// @export
fn test_from_schema(field: Robj) {
    let f = Schema::from_arrow_robj(&field);
    rprintln!("{:#?}", f);
}

#[extendr]
/// @export
fn test_from_array(field: Robj) {
    let f = ArrayData::from_arrow_robj(&field);
    rprintln!("{:#?}", f);
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
    fn test_record_batch;
    fn test_schema;
    fn test_datatype;

    // FromArrowRobj impls
    fn test_from_field;
    fn test_from_datatype;
    fn test_from_schema;
    fn test_from_array;
}
