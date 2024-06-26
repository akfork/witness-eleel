use std::time::Duration;

use execution_layer::{
    engines::Engine,
    json_structures::{JsonExecutionPayload, JsonPayloadStatusV1Status},
    Error as ExecutionLayerError, EthSpec, ExecutionBlockHash,
    ExecutionPayload, NewPayloadRequest, NewPayloadRequestDeneb,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub const ENGINE_NEW_PAYLOAD_WITH_WITNESS_V2: &str = "engine_newPayloadV2";
pub const ENGINE_NEW_PAYLOAD_WITH_WITNESS_V3: &str = "engine_newPayloadV3";
pub const ENGINE_NEW_PAYLOAD_TIMEOUT: Duration = Duration::from_secs(8);

pub const ENGINE_STATELESS_EXECUTION_V2: &str = "engine_executeStatelessPayloadV2";
pub const ENGINE_STATELESS_EXECUTION_V3: &str = "engine_executeStatelessPayloadV2";
pub const ENGINE_STATELESS_EXECUTION_TIMEOUT: Duration = Duration::from_secs(8);

/*
// statelessWitnessV1 is the witness data necessary to execute an ExecutableData
// without any local data being present.
var statelessWitnessV1 = {
    headers: ["0xhrlp1", "0xhrlp2", ...],
    codes:   ["0xcode1", "0xcode2", ...],
    state:   ["0xnode1", "0xnode2", ...]
}

// statelessPayloadStatusV1 is the result of a stateless payload execution.
var statelessPayloadStatusV1 = {
    status:          "same as payloadStatusV1.status",
    stateRoot:       "0x0000000000000000000000000000000000000000000000000000000000000000",
    receiptsRoot:    "0x0000000000000000000000000000000000000000000000000000000000000000",
    validationError: "same as payloadStatusV1.validationError",
}
*/

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonStatelessWitnessV1 {
    // TODO: maybe this should be Vec<Value>?
    pub headers: serde_json::Value,
    pub codes: serde_json::Value,
    pub state: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonStatelessPayloadStatusV1 {
    pub status: JsonPayloadStatusV1Status,
    pub state_root: ExecutionBlockHash,
    pub receipts_root: ExecutionBlockHash,
    pub validation_error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonPayloadStatusWithWitnessV1 {
    pub status: JsonPayloadStatusV1Status,
    pub latest_valid_hash: Option<ExecutionBlockHash>,
    pub validation_error: Option<String>,
    pub witness: JsonStatelessWitnessV1,
}

// #[derive(Clone, Copy, Debug)]
// pub struct EngineCapabilitiesStateless {
//     pub stateful_capabilities: EngineCapabilities,
//     pub new_payload_witness_v1: bool,
//     pub new_payload_witness_v2: bool,
//     pub new_payload_witness_v3: bool,
// }

/// An execution engine with additional methods for supporting stateless witness generation
/// and validation.
pub struct StatelessEngine {
    pub stateless_engine: Engine,
}

impl StatelessEngine {
    pub async fn new_payload_with_witness<E: EthSpec>(
        &self,
        new_payload_request: NewPayloadRequest<'_, E>,
    ) -> Result<JsonPayloadStatusWithWitnessV1, ExecutionLayerError> {
        // Assume that stateless capabilities exist for now
        // let engine_capabilities = self.stateless_engine.get_engine_capabilities(None).await?;
        match new_payload_request {
            NewPayloadRequest::Bellatrix(_) | NewPayloadRequest::Capella(_) => {
                self.new_payload_with_witness_v2(new_payload_request.into_execution_payload())
                    .await
            }
            NewPayloadRequest::Deneb(new_payload_request_deneb) => {
                self.new_payload_with_witness_v3(new_payload_request_deneb)
                    .await
            }
            NewPayloadRequest::Electra(_) => {
                todo!()
            }
        }
    }

    pub async fn stateless_execution<E: EthSpec>(
        &self,
        new_payload_request: NewPayloadRequest<'_, E>,
        witness: JsonStatelessWitnessV1,
    ) -> Result<JsonStatelessPayloadStatusV1, ExecutionLayerError> {
        // Assume that stateless capabilities exist for now
        // let engine_capabilities = self.stateless_engine.get_engine_capabilities(None).await?;
        match new_payload_request {
            NewPayloadRequest::Bellatrix(_) | NewPayloadRequest::Capella(_) => {
                self.stateless_execution_v2(new_payload_request.into_execution_payload(), witness)
                    .await
            }
            NewPayloadRequest::Deneb(new_payload_request_deneb) => {
                self.stateless_execution_v3(new_payload_request_deneb, witness)
                    .await
            }
            NewPayloadRequest::Electra(_) => {
                todo!()
            }
        }
    }

    pub async fn new_payload_with_witness_v2<E: EthSpec>(
        &self,
        execution_payload: ExecutionPayload<E>,
    ) -> Result<JsonPayloadStatusWithWitnessV1, ExecutionLayerError> {
        let params = json!([JsonExecutionPayload::from(execution_payload)]);

        let response: JsonPayloadStatusWithWitnessV1 = self
            .stateless_engine
            .api
            .rpc_request(
                ENGINE_NEW_PAYLOAD_WITH_WITNESS_V2,
                params,
                ENGINE_NEW_PAYLOAD_TIMEOUT,
            )
            .await?;

        Ok(response)
    }

    pub async fn new_payload_with_witness_v3<E: EthSpec>(
        &self,
        new_payload_request_deneb: NewPayloadRequestDeneb<'_, E>,
    ) -> Result<JsonPayloadStatusWithWitnessV1, ExecutionLayerError> {
        let params = json!([
            JsonExecutionPayload::V3(new_payload_request_deneb.execution_payload.clone().into()),
            new_payload_request_deneb.versioned_hashes,
            new_payload_request_deneb.parent_beacon_block_root,
        ]);

        let response: JsonPayloadStatusWithWitnessV1 = self
            .stateless_engine
            .api
            .rpc_request(
                ENGINE_NEW_PAYLOAD_WITH_WITNESS_V3,
                params,
                ENGINE_NEW_PAYLOAD_TIMEOUT,
            )
            .await?;

        Ok(response)
    }

    pub async fn stateless_execution_v2<E: EthSpec>(
        &self,
        execution_payload: ExecutionPayload<E>,
        witness: JsonStatelessWitnessV1,
    ) -> Result<JsonStatelessPayloadStatusV1, ExecutionLayerError> {
        let params = json!([JsonExecutionPayload::from(execution_payload), witness]);

        let response: JsonStatelessPayloadStatusV1 = self
            .stateless_engine
            .api
            .rpc_request(
                ENGINE_STATELESS_EXECUTION_V2,
                params,
                ENGINE_STATELESS_EXECUTION_TIMEOUT,
            )
            .await?;

        Ok(response)
    }

    pub async fn stateless_execution_v3<E: EthSpec>(
        &self,
        new_payload_request_deneb: NewPayloadRequestDeneb<'_, E>,
        witness: JsonStatelessWitnessV1,
    ) -> Result<JsonStatelessPayloadStatusV1, ExecutionLayerError> {
        let params = json!([
            JsonExecutionPayload::V3(new_payload_request_deneb.execution_payload.clone().into()),
            new_payload_request_deneb.versioned_hashes,
            new_payload_request_deneb.parent_beacon_block_root,
            witness
        ]);

        let response: JsonStatelessPayloadStatusV1 = self
            .stateless_engine
            .api
            .rpc_request(
                ENGINE_STATELESS_EXECUTION_V3,
                params,
                ENGINE_STATELESS_EXECUTION_TIMEOUT,
            )
            .await?;

        Ok(response)
    }
}
