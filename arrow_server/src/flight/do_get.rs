use anyhow::Result;
use arrow_flight::Ticket;

pub(crate) fn parse_file_path(ticket: Ticket) -> Result<String> {
    let path =
        "/Users/firoly/Documents/code/rust/learn_rust/learn_arrow/datafusion/server/test/0.tssp";
    Ok(path.to_string())

    // return match String::from_utf8(ticket.ticket.to_vec()) {
    //     Ok(p) => {
    //         let path =
    //             "/Users/firoly/Documents/code/rust/learn_rust/learn_arrow/datafusion/server/test/"
    //                 .to_string()
    //                 + p.as_str()
    //                 + ".tssp";
    //         print!("path: {:?}",path);
    //         Ok(path)
    //     }
    //     Err(_e) => Err(anyhow::anyhow!("parse file path error")),
    // };
}
