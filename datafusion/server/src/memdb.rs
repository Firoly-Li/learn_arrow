/**
 * 使用Parquet实现数据的罗盘和读取
 */
#[warn(dead_code)]
struct MemDB;

#[cfg(test)]
mod tests {
    use std::{fs::File, sync::Arc};

    use anyhow::Result;
    use arrow::{
        array::{Int32Array, RecordBatch, UInt64Array},
        datatypes::*,
        error::ArrowError,
    };
    use datafusion::{
        execution::{context::SessionContext, options::ParquetReadOptions},
        logical_expr::{col, lit},
    };
    use parquet::{
        arrow::{
            arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
            ArrowWriter, AsyncArrowWriter,
        },
        basic::Compression,
        file::properties::WriterProperties,
    };
    /**
     *
     */
    #[tokio::test]
    async fn test() {
        // 读取 Parquet 文件
        let file = File::open("test/example.tssp").unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let reader: ParquetRecordBatchReader = builder.build().unwrap();
        let r = reader
            .into_iter()
            .collect::<Vec<Result<RecordBatch, ArrowError>>>();
        println!("r = {:?}", r.len());
        for iter in r.into_iter() {
            let batch = iter.unwrap();
            println!("batch = {:?}", batch);
        }
    }

    /**
     *
     */
    #[tokio::test]
    async fn select_columns_from_parquet_test() {
        let ctx = SessionContext::new();
        let mut options = ParquetReadOptions::default();
        options.file_extension = "tssp";
        if let Ok(df) = ctx.read_parquet("test/example.tssp", options).await {
            let resp = df
                .select_columns(&["name"])
                .unwrap()
                .collect()
                .await
                .unwrap();
            let pretty_results = arrow::util::pretty::pretty_format_batches(&resp)
                .unwrap()
                .to_string();
            for i in pretty_results.split("\n") {
                println!("{:?}", i);
            }
        } else {
            println!("error");
        }
    }

    #[tokio::test]
    async fn get_field_names_from_data_frame_test() {
        let ctx = SessionContext::new();
        let mut options = ParquetReadOptions::default();
        options.file_extension = "tssp";
        if let Ok(df) = ctx.read_parquet("test/example.tssp", options).await {
            let desc = df
                .schema()
                .field_names()
                .into_iter()
                .map(|mut x| x.split_off(8))
                .collect::<Vec<String>>();
            println!("desc = {:?}", desc);
        }
    }

    #[tokio::test]
    async fn select_with_exprs_test() {
        let ctx = SessionContext::new();
        let mut options = ParquetReadOptions::default();
        options.file_extension = "tssp";
        if let Ok(df) = ctx.read_parquet("test/example.tssp", options).await {
            let expr = col("name").eq(lit(1_i32));
            let df = df.filter(expr);
            let resp = df.unwrap().collect().await.unwrap();
            let pretty_results = arrow::util::pretty::pretty_format_batches(&resp)
                .unwrap()
                .to_string();
            for i in pretty_results.split("\n") {
                println!("{:?}", i);
            }
            // println!("{:?}", pretty_results);
        } else {
            println!("error");
        }
    }

    /**
     * 同步写入测试
     */
    #[test]
    fn sync_write_test() {
        self::sync_write(1000, "test/example.tssp");
    }

    #[test]
    fn sync_Write_test1() {
        self::sync_write1(1000, "test/example1.tssp");
    }

    fn sync_write1(n: i32, path: &str) {
        let schema = create_schema();
        // 写入 Parquet 文件
        let file = File::create(path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema.into(), Some(props)).unwrap();
        for i in 0..n {
            writer.write(&create_batch(i)).unwrap();
        }
        writer.close().unwrap();
    }

    fn sync_write(n: i32, path: &str) {
        let batch1 = create_batch1();
        // 写入 Parquet 文件
        let file = File::create(path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, batch1.schema().clone(), Some(props)).unwrap();
        for i in 0..n {
            writer.write(&create_batch(i)).unwrap();
        }
        writer.close().unwrap();
    }

    fn create_schema() -> Schema {
        Schema::new(vec![
            Field::new("name", DataType::Int32, false),
            Field::new("age", DataType::Int32, false),
            Field::new("address", DataType::Int32, false),
        ])
    }

    #[tokio::test]
    async fn async_write_test() {
        let batch1 = create_batch1();
        let file = tokio::fs::File::create("test/1.tssp").await.unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer =
            AsyncArrowWriter::try_new(file, batch1.schema().clone(), Some(props)).unwrap();
        for i in 0..100 {
            writer.write(&create_batch(i)).await.unwrap();
        }
        writer.close().await.unwrap();
    }

    /**
     * 构建数据
     */
    fn create_batch(n: i32) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Int32, false),
            Field::new("age", DataType::Int32, false),
            Field::new("address", DataType::Int32, false),
            Field::new("time", DataType::UInt64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1 + n * 10, 2 + n * 10, 3 + n * 10])),
                Arc::new(Int32Array::from(vec![4 + n * 10, 5 + n * 10, 6 + n * 10])),
                Arc::new(Int32Array::from(vec![7 + n * 10, 8 + n * 10, 9 + n * 10])),
                Arc::new(UInt64Array::from(vec![None, None, Some(9)])),
            ],
        )
        .unwrap();
        batch
    }

    fn create_batch1() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Int32, false),
            Field::new("age", DataType::Int32, false),
            Field::new("address", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![11, 21, 31])),
                Arc::new(Int32Array::from(vec![41, 51, 61])),
                Arc::new(Int32Array::from(vec![71, 81, 91])),
            ],
        )
        .unwrap();
        batch
    }
}
