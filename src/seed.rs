use std::sync::Arc;

use arrow_array::{
    ArrayRef, RecordBatch, RecordBatchIterator, RecordBatchReader,
    builder::{FixedSizeListBuilder, Float32Builder, Int32Builder, StringBuilder},
};
use arrow_cast::cast;
use arrow_schema::{DataType, Field, Schema};
use rand::prelude::*;

pub fn make_seed_batches(num_rows: usize)
-> Result<Box<dyn RecordBatchReader + Send>, Box<dyn std::error::Error + Send + Sync>> {
    let NUM_SEED_ROWS: usize = num_rows;
    const VECTOR_DIMENSION: i32 = 64;

    // Prefer FixedSizeList for embeddings (dimension is fixed).
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, false)),
                VECTOR_DIMENSION,
            ),
            false,
        ),
    ]));

    let mut rng = rand::rng();
    // ---- Build columns with builders ----
    let mut id_builder = Int32Builder::new();
    let mut name_builder = StringBuilder::new();

    // Fixed-size list builder: each row appends exactly VECTOR_DIMENSION floats
    let values_builder = Float32Builder::new();
    let mut vec_builder = FixedSizeListBuilder::new(values_builder, VECTOR_DIMENSION);

    for i in 0..NUM_SEED_ROWS {
        id_builder.append_value(i as i32);
        name_builder.append_value(format!("item_{i}"));

        // Append one vector row
        for _ in 0..VECTOR_DIMENSION {
            vec_builder
                .values()
                .append_value(rng.random_range(0.0..1.0) as f32);
        }
        vec_builder.append(true); // this completes one list entry
    }

    let id_arr: ArrayRef = Arc::new(id_builder.finish());
    let name_arr: ArrayRef = Arc::new(name_builder.finish());

    let vec_arr_raw: ArrayRef = Arc::new(vec_builder.finish());
    let target_type = DataType::FixedSizeList(
        Arc::new(Field::new("item", DataType::Float32, false)),
        VECTOR_DIMENSION,
    );
    let vec_arr: ArrayRef = cast(&vec_arr_raw, &target_type)?;

    let batch = RecordBatch::try_new(schema.clone(), vec![id_arr, name_arr, vec_arr])?;

    let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
    Ok(Box::new(reader))
}
