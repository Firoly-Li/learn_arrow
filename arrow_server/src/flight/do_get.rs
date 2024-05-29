use anyhow::Result;
use arrow::array::RecordBatch;
use arrow_flight::{error::FlightError, Ticket};
use datafusion::dataframe::DataFrame;

pub(crate) fn parse_file_path(ticket: Ticket) -> Result<String> {
    return match String::from_utf8(ticket.ticket.to_vec()) {
        Ok(p) => {
            let path =
                "/Users/firoly/Documents/code/rust/learn_rust/learn_arrow/datafusion/server/test/"
                    .to_string()
                    + p.as_str()
                    + ".tssp";
            print!("path: {:?}",path);
            Ok(path)
        }
        Err(_e) => Err(anyhow::anyhow!("parse file path error")),
    };
}

pub(crate) async fn df_to_batchs(df: DataFrame) -> Vec<Result<RecordBatch, FlightError>>{
    // 执行查询并收集结果
    let resp = df.collect().await.unwrap();
    let batchs: Vec<Result<RecordBatch, FlightError>> =
        resp.iter().map(|x| Ok(x.clone())).collect();
    batchs
}