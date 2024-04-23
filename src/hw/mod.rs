use arrow::array::RecordBatch;
use arrow_flight::FlightData;

pub mod helloworld;

pub mod arrow_msg;

fn flight_data_to_arrow_batch(flight_data: FlightData) -> Option<RecordBatch> {
    match flight_data.flight_descriptor {
        Some(desc) => {
            let r_type = desc.r#type();
            println!("r_type: {:?}", r_type);
        }
        None => println!("r_type: None"),
    }
    None
}

#[cfg(test)]
mod tests {

    #[test]
    fn test() {}
}
