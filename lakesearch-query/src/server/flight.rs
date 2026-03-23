//! Arrow Flight service for LakeSearch.
//!
//! Exposes search queries over the Arrow Flight RPC protocol. Clients send
//! JSON-encoded search requests as Flight tickets and receive results as
//! streams of Arrow RecordBatches.

use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::stream::{StreamExt, TryStreamExt};
use futures::Stream;
use lakesearch_core::metadata::ColumnStatus;
use object_store::path::Path;
use serde::Deserialize;
use tonic::{Request, Response, Status, Streaming};
use tracing::info;

use super::api_types::{OperatorStr, ScoreMode};
use super::state::AppState;
use crate::object_cache::ObjectCache;

/// Flight ticket / command JSON format.
///
/// Contains a `table` field plus the same search fields as the REST API's
/// `SearchRequest`, flattened for simpler JSON payloads.
#[derive(Debug, Deserialize)]
struct FlightSearchRequest {
    table: String,
    column: String,
    #[serde(rename = "match")]
    match_text: String,
    #[serde(default = "super::api_types::default_operator")]
    operator: OperatorStr,
    #[serde(default)]
    select: Vec<String>,
    #[serde(default)]
    score: ScoreMode,
    limit: Option<usize>,
}

/// Arrow Flight service backed by LakeSearch.
#[derive(Clone)]
pub struct LakeSearchFlightService {
    state: AppState,
}

impl LakeSearchFlightService {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    /// Validates table exists and column is indexed. Returns the object cache
    /// and base path on success.
    async fn validate_request(
        &self,
        req: &FlightSearchRequest,
    ) -> Result<(Arc<ObjectCache>, Path), Status> {
        let (object_cache, base, meta) = self
            .state
            .cache
            .get_table_state(&req.table)
            .await
            .ok_or_else(|| Status::not_found(format!("table '{}' not found", req.table)))?;

        meta.indexed_columns
            .iter()
            .find(|c| c.name == req.column && c.status != ColumnStatus::Dropped)
            .ok_or_else(|| {
                Status::invalid_argument(format!("column '{}' not found or dropped", req.column))
            })?;

        Ok((object_cache, base))
    }
}

/// Parses a JSON byte slice into a `FlightSearchRequest`.
#[allow(clippy::result_large_err)]
fn parse_search_request(bytes: &[u8]) -> Result<FlightSearchRequest, Status> {
    serde_json::from_slice(bytes)
        .map_err(|e| Status::invalid_argument(format!("invalid JSON: {e}")))
}

#[tonic::async_trait]
impl FlightService for LakeSearchFlightService {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + 'static>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not supported"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights not supported"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let req = parse_search_request(&descriptor.cmd)?;
        let (object_cache, base) = self.validate_request(&req).await?;

        let with_score = req.score != ScoreMode::None;
        let schema = crate::query::resolve_schema_from_table(
            &object_cache,
            &base,
            &req.column,
            &req.select,
            with_score,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        let ticket = Ticket::new(descriptor.cmd.clone());

        let flight_info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|e| Status::internal(format!("schema encoding failed: {e}")))?
            .with_descriptor(descriptor)
            .with_endpoint(arrow_flight::FlightEndpoint::new().with_ticket(ticket));

        Ok(Response::new(flight_info))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema not supported"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not supported"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let req = parse_search_request(&ticket.ticket)?;

        info!(table = %req.table, column = %req.column, query = %req.match_text, "flight do_get");

        let (object_cache, base) = self.validate_request(&req).await?;

        let operator: crate::Operator = req.operator.into();
        let score_mode: crate::ScoreMode = req.score.into();

        let timeout = self.state.config.query_timeout();

        let batch_stream = tokio::time::timeout(
            timeout,
            crate::query::run_query(
                object_cache,
                base,
                req.column,
                &req.match_text,
                operator,
                score_mode,
                req.limit,
                req.select,
                self.state.config.io_concurrency,
                self.state.config.max_io_tasks,
                Arc::clone(&self.state.runtime),
            ),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("query setup timed out"))?
        .map_err(|e| Status::internal(e.to_string()))?;

        let schema = crate::RecordBatchStream::schema(&*batch_stream);

        let mapped_stream =
            batch_stream.map_err(|e| arrow_flight::error::FlightError::ExternalError(e.into()));

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(mapped_stream)
            .map_err(|e| Status::internal(e.to_string()));

        // Enforce a wall-clock deadline across the entire stream, not just
        // per-batch. Once `timeout` elapses from stream creation, every
        // subsequent poll yields DEADLINE_EXCEEDED.
        let deadline = tokio::time::Instant::now() + timeout;
        #[allow(clippy::result_large_err)] // Status is required by the Flight trait
        let timed_stream = flight_data_stream.map(move |item| {
            if tokio::time::Instant::now() >= deadline {
                Err(Status::deadline_exceeded("query timed out"))
            } else {
                item
            }
        });

        Ok(Response::new(Box::pin(timed_stream) as Self::DoGetStream))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not supported"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not supported"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions not supported"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not supported"))
    }
}
