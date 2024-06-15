use arrow_flight::FlightData;
use tokio::{fs::File, io::AsyncWriteExt, sync::mpsc::Receiver};

pub struct Wal {
    file: File,
    receiver: Receiver<FlightData>,
}

impl Wal {
    pub async fn new(file: File, receiver: Receiver<FlightData>) -> Self {
        Self { file, receiver }
    }
    async fn start(mut self) {
        loop {
            if let Some(msg) = self.receiver.recv().await {
                let len = &msg.to_string().as_bytes().len();
                println!("{}", len);
                let _ = self.file.write_all(&msg.to_string().as_bytes()).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;
    use std::{collections::{BTreeMap, HashMap}, sync::Arc};

    use arrow::{
        array::{Int32Array, RecordBatch, UInt64Array},
        datatypes::*,
    };
    use arrow_flight::{
        utils::{batches_to_flight_data, flight_data_to_arrow_batch, flight_data_to_batches},
        FlightData,
    };
    use bytes::{BufMut, Bytes, BytesMut};
    use tokio::{
        fs::{self, OpenOptions},
        io::{AsyncReadExt, AsyncWriteExt},
        sync::mpsc,
    };

    use super::Wal;

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
                Arc::new(Int32Array::from(vec![
                    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                ])),
                Arc::new(Int32Array::from(vec![
                    4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
                ])),
                Arc::new(Int32Array::from(vec![
                    7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
                ])),
                Arc::new(UInt64Array::from(vec![
                    None,
                    None,
                    Some(9),
                    None,
                    None,
                    Some(10),
                    None,
                    None,
                    Some(11),
                    None,
                    None,
                    Some(12),
                    None,
                    None,
                    Some(13),
                ])),
            ],
        )
        .unwrap();
        batch
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test() {
        let _ = fs::File::create("test.wal").await;
        // 1、 开启wal
        let file = OpenOptions::new()
            .append(true)
            .read(true)
            .open("test.wal")
            .await
            .unwrap();
        let (s, r) = mpsc::channel(10);
        let wal = Wal::new(file, r).await;
        tokio::spawn(async move { wal.start().await });
        // 2、创建batch
        let mut v = Vec::new();
        let batch = create_batch();
        let schema = batch.schema();
        v.push(batch);
        let fds = batches_to_flight_data(&schema, v).unwrap();
        for fd in fds {
            let _ = s.send(fd).await;
        }

        println!("send over");
    }

    #[derive(Debug, Clone)]
    struct Offset {
        pub point: usize,
        pub len: usize,
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn write_flight_data_test() {
        let _ = fs::File::create("test.wal").await;
        let mut offset_map = BTreeMap::new();
        let mut headers = Vec::new();

        {
            // 1、 创建文件
            let mut file = OpenOptions::new()
                .append(true)
                .read(true)
                .open("test.wal")
                .await
                .unwrap();
            let mut v = Vec::new();
            // 2、创建batch
            let batch = create_batch();

            let schema = batch.schema();
            v.push(batch);
            let fds = batches_to_flight_data(&schema, v).unwrap();
            // let bath = flight_data_to_batches(&fds).unwrap();
            println!("fds: {:?}", fds.len());
            let mut point = 0;
            let mut n = 0;
            for fd in fds {
                println!("fd: {:?}", fd);
                let mut buf = BytesMut::new();
                fd.encode(&mut buf).unwrap();
                let len = buf.len();
                println!("lenght: {}", len);
                let offset = Offset { point, len };
                println!("offset: {:?}", offset);
                point += len;
                // let offset = Offset { point, len };
                offset_map.insert(n, offset);
                n += 1;
                let _ = file.write_all(&buf).await;
            }
            headers.push(0);
        }
        // read_file
        let mut file = OpenOptions::new()
            .read(true)
            .open("test.wal")
            .await
            .unwrap();
        let mut buf: Vec<u8> = Vec::new();
        let _ = file.read_to_end(&mut buf).await;
        println!("buf: {:?}", buf.len());
        let stream = BytesMut::from(buf.as_slice());
        read_batch_from_stream(stream.freeze(), offset_map, headers);
    }

    fn read_batch_from_stream(
        stream: Bytes,
        offset_map: BTreeMap<usize, Offset>,
        headers: Vec<usize>,
    ) {
        let stream_len = stream.len();
        println!("stream len: {}", stream_len);
        let mut fds = Vec::new();
        for (n,offset) in offset_map {
            println!("offset: {:?}", offset);
            let b = stream.slice(offset.point..offset.point + offset.len);
            let fd = FlightData::decode(b.as_ref()).unwrap();
            fds.push(fd);
        }

        let bath = flight_data_to_batches(&fds).unwrap();
        println!("batch: {:?}", bath);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn read_wal_test() {
        let mut file = OpenOptions::new()
            .append(true)
            .read(true)
            .open("test.wal")
            .await
            .unwrap();
        let mut buf: Vec<u8> = Vec::new();
        let _ = file.read_to_end(&mut buf).await;
        let fd = FlightData::decode(buf.as_slice()).unwrap();
        println!("fd: {:?}", fd);
        let mut vs = Vec::new();
        vs.push(fd);
        let bath = flight_data_to_batches(&vs).unwrap();
        println!("batch: {:?}", bath);
    }

    #[test]
    fn batch_flight_data_exchange_test() {
        let mut v = Vec::new();
        let batch = create_batch();
        let schema = batch.schema();
        v.push(batch);
        let fds = batches_to_flight_data(&schema, v).unwrap();
        println!("flight_data: {:?}", fds);
        let bath = flight_data_to_batches(&fds).unwrap();
        println!("batch: {:?}", bath);
    }
}
