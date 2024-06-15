use arrow_flight::FlightInfo;

#[derive(Debug)]
pub struct FlightState {
    list_flights: Vec<FlightInfo>,
}

impl FlightState {
    pub fn new() -> Self {
        Self {
            list_flights: Vec::new(),
        }
    }

    pub fn add_flight(&mut self, flight: FlightInfo) {
        self.list_flights.push(flight);
    }
}
