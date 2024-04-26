use arrow::{
    array::{Int32Array, RecordBatch, UInt64Array},
    datatypes::{DataType, Field, Schema},
};
use datafusion::{datasource::MemTable, execution::{context::SessionContext, options::ParquetReadOptions}, logical_expr::{col, lit, table_scan, Literal, LogicalPlanBuilder}};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let table = create_memory_table();
    let table = Arc::new(table);
    let context = SessionContext::new();
    sql_select_test(table.clone(), &context).await;
    // api_select_test(table.clone(), &context).await;

    // select_test(&context).await;
}

/**
 * DataFrame API 测试
 */
async fn api_select_test(table: Arc<MemTable>, context: &SessionContext) {
    if let Ok(df) = context.read_table(table) {
        let data = df
            .select_columns(&["name", "age"])
            .unwrap()
            .collect()
            .await
            .unwrap();
        println!("data = {:?}", data);
    }
}

/**
 * 查询Parquet文件中的指定列的所有数据
 */
async fn select_test(ctx: &SessionContext) {
    let table_paths = "test/example.tssp";
    let mut options = ParquetReadOptions::default();
    options.file_extension = "tssp";
    if let Ok(df) = ctx.read_parquet(table_paths, options).await {
        // let expr = col("name").eq(lit(Int32(Some(1))));
        let resp = df.select_columns(&["name"]).unwrap().collect().await.unwrap();
    println!("resp = {:?}", resp);
    }
}

/**
 * DataFrame SQL查询测试
 */
async fn sql_select_test(table: Arc<MemTable>, context: &SessionContext) {
    context.register_table("test", table).unwrap();
    let sql = "select * from test where name = 1";
    let dataframe = context.sql(sql).await.unwrap();
    let data = dataframe.collect().await.unwrap();
    println!("data = {:?}", data);
}

/**
 * 创建数据
 */
fn create_memory_table() -> MemTable {
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

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(Int32Array::from(vec![40, 50, 60])),
            Arc::new(Int32Array::from(vec![70, 80, 90])),
            Arc::new(UInt64Array::from(vec![None, None, Some(90)])),
        ],
    )
    .unwrap();

    let batch3 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![11, 21, 31])),
            Arc::new(Int32Array::from(vec![41, 51, 61])),
            Arc::new(Int32Array::from(vec![71, 81, 91])),
            Arc::new(UInt64Array::from(vec![None, None, Some(91)])),
        ],
    )
    .unwrap();

    let provider = MemTable::try_new(schema, vec![vec![batch, batch2, batch3]]).unwrap();
    provider
}
