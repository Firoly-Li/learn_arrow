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
    };
    use parquet::{
        arrow::{
            arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
            ArrowWriter,
        },
        file::properties::WriterProperties,
    };

    fn create_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Int32, false),
            Field::new("age", DataType::Int32, false),
            Field::new("address", DataType::Int32, false),
            Field::new("time", DataType::UInt64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
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
            Field::new("time", DataType::UInt64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![11, 21, 31])),
                Arc::new(Int32Array::from(vec![41, 51, 61])),
                Arc::new(Int32Array::from(vec![71, 81, 91])),
                Arc::new(UInt64Array::from(vec![Some(81), None, Some(91)])),
            ],
        )
        .unwrap();
        batch
    }

    #[tokio::test]
    async fn test() {
        let batch = create_batch();
        let batch1 = create_batch1();
        // 写入 Parquet 文件
        let file = File::create("example.parquet").unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, batch.schema().clone(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.write(&batch1).unwrap();
        writer.close().unwrap();

        // 读取 Parquet 文件
        let file = File::open("example.parquet").unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mut reader: ParquetRecordBatchReader = builder.build().unwrap();
        // for iter in reader.into_iter() {
        //     let batch = iter.unwrap();
        //     println!("batch = {:?}", batch);
        // }
        let batch = reader.next().unwrap().unwrap();
        println!("batch = {:?}", batch);
    }
}
