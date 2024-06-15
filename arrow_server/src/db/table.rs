use arrow::{array::RecordBatch, datatypes::Schema};

pub struct Table {
    id: String,
    name: String,
    desc: Schema,
    create_time: u64,
    mem_data: Vec<RecordBatch>,
    persistence_data: Vec<String>,
}

impl Table {}

#[cfg(test)]
mod tests {

    #[test]
    fn test() {}
}
