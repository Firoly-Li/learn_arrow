pub mod memdb;

#[cfg(test)]
mod tests {

    use arrow::{array::RecordBatch, ipc::writer::FileWriter};
    use datafusion::execution::{context::SessionContext, options::ParquetReadOptions};
    use parquet::{
        arrow::AsyncArrowWriter, basic::Compression, file::properties::WriterProperties,
    };
    use tokio::fs::File;

    fn create_paths() -> Vec<String> {
        let mut resp = Vec::new();
        let path = String::from(
            "/Users/firoly/Documents/code/rust/learn_rust/learn_arrow/datafusion/server/test/",
        );
        for n in 0..5 {
            let file_path = path.clone() + &n.to_string() + ".tssp";
            resp.push(file_path)
        }
        resp
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test() {
        let ctx = SessionContext::new();
        let mut opts = ParquetReadOptions::default();
        opts.file_extension = "tssp";
        let pathes = create_paths();
        let mut n = 0;
        let _ = ctx
            .register_parquet("table0", "test/0.tssp", opts.clone())
            .await;
        let _ = ctx
            .register_parquet("table1", "test/1.tssp", opts.clone())
            .await;

        let tables = vec!["table0", "table1"];

        // 构建一个查询，将所有表的数据联合起来
        let query = format!("SELECT * FROM {}", tables.join(" UNION ALL SELECT * FROM "));
        println!("Query: {}", query);
        let query0 = "SELECT * FROM table0";
        // 执行查询并获取结果
        let df = ctx.sql(&query).await;
        match df {
            Ok(d) => {
                let results = d.collect().await.unwrap();
                write(results).await;
                // println!("Results: {:#?}", results);
            }
            Err(e) => println!("Error: {:#?}", e),
        }
    }

    async fn write(results: Vec<RecordBatch>) {
        let file = tokio::fs::File::create("test/100.tssp").await.unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer =
            AsyncArrowWriter::try_new(file, results[0].schema().clone(), Some(props)).unwrap();
        for batch in results {
            writer.write(&batch).await.unwrap();
        }
        writer.close().await.unwrap();
    }
}
