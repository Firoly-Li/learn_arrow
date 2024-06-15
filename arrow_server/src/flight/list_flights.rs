use arrow_flight::{FlightDescriptor, FlightInfo};

pub fn create_list_flights() -> Vec<FlightInfo> {
    let mut infos = Vec::new();
    for n in 0..10 {
        let info = create_flight_info(n);
        infos.push(info);
    }
    infos
}

pub fn create_flight_info(n: usize) -> FlightInfo {
    let mut info = FlightInfo::new();
    info.flight_descriptor = Some(FlightDescriptor::new_cmd(n.to_string()));
    info
}
