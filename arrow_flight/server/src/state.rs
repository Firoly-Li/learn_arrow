use arrow::{array::RecordBatch, datatypes::Schema};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, Ticket,
};
use tonic::{metadata::MetadataMap, Status};

/// mutable state for the TestFlightServer, captures requests and provides responses
#[derive(Debug, Default)]
pub struct State {
    /// The last handshake request that was received
    pub handshake_request: Option<HandshakeRequest>,
    /// The next response to return from `handshake()`
    pub handshake_response: Option<Result<HandshakeResponse, Status>>,
    /// The last `get_flight_info` request received
    pub get_flight_info_request: Option<FlightDescriptor>,
    /// The next response to return from `get_flight_info`
    pub get_flight_info_response: Option<Result<FlightInfo, Status>>,
    /// The last `poll_flight_info` request received
    pub poll_flight_info_request: Option<FlightDescriptor>,
    /// The next response to return from `poll_flight_info`
    pub poll_flight_info_response: Option<Result<PollInfo, Status>>,
    /// The last do_get request received
    pub do_get_request: Option<Ticket>,
    /// The next response returned from `do_get`
    pub do_get_response: Option<Vec<Result<RecordBatch, Status>>>,
    /// The last do_put request received
    pub do_put_request: Option<Vec<FlightData>>,
    /// The next response returned from `do_put`
    pub do_put_response: Option<Vec<Result<PutResult, Status>>>,
    /// The last do_exchange request received
    pub do_exchange_request: Option<Vec<FlightData>>,
    /// The next response returned from `do_exchange`
    pub do_exchange_response: Option<Vec<Result<FlightData, Status>>>,
    /// The last list_flights request received
    pub list_flights_request: Option<Criteria>,
    /// The next response returned from `list_flights`
    pub list_flights_response: Option<Vec<Result<FlightInfo, Status>>>,
    /// The last get_schema request received
    pub get_schema_request: Option<FlightDescriptor>,
    /// The next response returned from `get_schema`
    pub get_schema_response: Option<Result<Schema, Status>>,
    /// The last list_actions request received
    pub list_actions_request: Option<Empty>,
    /// The next response returned from `list_actions`
    pub list_actions_response: Option<Vec<Result<ActionType, Status>>>,
    /// The last do_action request received
    pub do_action_request: Option<Action>,
    /// The next response returned from `do_action`
    pub do_action_response: Option<Vec<Result<arrow_flight::Result, Status>>>,
    /// The last request headers received
    pub last_request_metadata: Option<MetadataMap>,
}
