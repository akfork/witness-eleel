//! Handler for new payload.
use crate::stateless_validation::{JsonPayloadStatusWithWitnessV1, JsonStatelessPayloadStatusV1};
use crate::types::{ErrorCode, JsonError};
use crate::{
    multiplexer::{Multiplexer, NewPayloadCacheEntry},
    types::{
        ErrorResponse, JsonExecutionPayload, JsonPayloadStatusV1, JsonPayloadStatusV1Status,
        JsonValue, NewPayloadRequest, NewPayloadRequestBellatrix, NewPayloadRequestCapella,
        NewPayloadRequestDeneb, QuantityU64, Request, Response,
    },
};
use eth2::types::{
    EthSpec, ExecutionBlockHash, ExecutionPayload, ForkName, Hash256, Slot, VersionedHash,
};
use execution_layer::http::{ENGINE_NEW_PAYLOAD_V1, ENGINE_NEW_PAYLOAD_V2, ENGINE_NEW_PAYLOAD_V3};
use std::time::{Duration, Instant};
use tokio::fs;

impl<E: EthSpec> Multiplexer<E> {
    pub async fn handle_controller_new_payload(
        &self,
        request: Request,
    ) -> Result<Response, ErrorResponse> {
        let method = request.method.clone();
        tracing::info!(method = method, "processing payload from controller");
        let (id, json_execution_payload, versioned_hashes, parent_beacon_block_root) =
            self.decode_new_payload(request)?;

        let execution_payload = ExecutionPayload::from(json_execution_payload);
        let block_hash = execution_payload.block_hash();
        let block_number = execution_payload.block_number();
        let new_payload_request = Self::new_payload_request_from_parts(
            &execution_payload,
            versioned_hashes,
            parent_beacon_block_root,
        );
        let status = if let Some(status) = self.get_cached_payload_status(&block_hash, true).await {
            status
        } else {
            if !self.stateless_engines.is_empty() {
                match self
                    .engine
                    .new_payload_with_witness(new_payload_request.clone())
                    .await
                {
                    Ok(resp) => {
                        self.handle_stateless_validation(new_payload_request, resp)
                            .await?
                    }
                    Err(e) => {
                        // Return an error to the controlling CL.
                        // TODO: consider flag to return SYNCING here (after block hash verif).
                        tracing::warn!(error = ?e, "error during newPayloadWithWitness");
                        return Err(ErrorResponse::invalid_request(
                            id,
                            "payload verification failed: see eleel logs".to_string(),
                        ));
                    }
                }
            } else {
                // Send payload to the real EL.
                match self
                    .engine
                    .stateless_engine
                    .api
                    .new_payload(new_payload_request)
                    .await
                {
                    Ok(status) => {
                        let json_status = JsonPayloadStatusV1::from(status);

                        // Update newPayload cache.
                        self.new_payload_cache.lock().await.put(
                            block_hash,
                            NewPayloadCacheEntry {
                                status: json_status.clone(),
                                block_number,
                            },
                        );

                        // Update payload builder.
                        self.register_canonical_payload(&execution_payload, json_status.status)
                            .await;

                        json_status
                    }
                    Err(e) => {
                        // Return an error to the controlling CL.
                        // TODO: consider flag to return SYNCING here (after block hash verif).
                        tracing::warn!(error = ?e, "error during newPayload");
                        return Err(ErrorResponse::invalid_request(
                            id,
                            "payload verification failed: see eleel logs".to_string(),
                        ));
                    }
                }
            }
        };

        Response::new(id, status)
    }

    /// Takes the response from the primary EE and runs it through stateless validation
    /// for all stateless EEs.
    ///
    /// If the results disagree, then returns an error.
    pub async fn handle_stateless_validation(
        &self,
        new_payload_request: NewPayloadRequest<'_, E>,
        status_with_witness: JsonPayloadStatusWithWitnessV1,
    ) -> Result<JsonPayloadStatusV1, ErrorResponse> {
        tracing::info!("handling stateless validation");
        // The primary validation was successful here so the state and receipts root should match
        // the new payload one.
        let primary_state_root = new_payload_request.execution_payload_ref().state_root();
        let primary_receipts_root = new_payload_request.execution_payload_ref().receipts_root();
        let primary_status = status_with_witness.status;
        let Some(witness) = status_with_witness.witness else {
            tracing::debug!("no witness returned for newpayloadwithwitness response");
            return Ok(JsonPayloadStatusV1 {
                latest_valid_hash: status_with_witness.latest_valid_hash,
                status: status_with_witness.status,
                validation_error: status_with_witness.validation_error,
            });
        };

        tracing::info!(
            "Fetching witness block: {:?}, data: {:?}",
            new_payload_request.block_number(),
            serde_json::to_string(&witness).unwrap()
        );
        // write into a file with filename <block_number>.json, content witness json string
        let filename = format!("{}.json", new_payload_request.block_number());
        fs::write(
            filename,
            serde_json::to_string(&witness).unwrap().as_bytes(),
        )
        .await
        .unwrap();

        let mut result: Option<JsonStatelessPayloadStatusV1> = None;
        for (i, stateless_engine) in self.stateless_engines.iter().enumerate() {
            match stateless_engine
                .stateless_execution(new_payload_request.clone(), &witness)
                .await
            {
                Err(e) => {
                    // Return early logging the error
                    tracing::warn!(error=?e, "error during stateless validation from {}", i);
                    return Err(ErrorResponse {
                        error: JsonError {
                            code: ErrorCode::InvalidRequest,
                            message: format!(
                                "stateless validation error {:?} from engine {}",
                                e, i
                            ),
                        },
                        jsonrpc: "2.0".into(),
                        id: JsonValue::Number(1.into()),
                    });
                }
                Ok(resp) => {
                    tracing::debug!("Received a response for stateless validation {:?}", resp);
                    if let Some(ref status) = result {
                        // TODO: check something wrt validation error as well
                        if resp.receipts_root != status.receipts_root
                            || resp.state_root != status.state_root
                            || resp.status != status.status
                        {
                            return Err(ErrorResponse {
                                error: JsonError {
                                    code: ErrorCode::InvalidRequest,
                                    message: format!(
                                        "stateless validation error engines disagree at {}",
                                        i
                                    ),
                                },
                                jsonrpc: "2.0".into(),
                                id: JsonValue::Number(1.into()),
                            });
                        }
                    } else {
                        result = Some(resp);
                    }
                }
            }
        }

        // Now check the agreed upon response with the primary EE response
        if let Some(status) = result {
            if status.state_root.0 != primary_state_root
                || status.receipts_root.0 != primary_receipts_root
                || status.status != primary_status
            {
                return Err(ErrorResponse {
                    error: JsonError {
                        code: ErrorCode::InvalidRequest,
                        message: "stateless validation disagree with primary engine".to_string(),
                    },
                    jsonrpc: "2.0".into(),
                    id: JsonValue::Number(1.into()),
                });
            }
        }

        // Everything agreed, send back primary response
        Ok(JsonPayloadStatusV1 {
            latest_valid_hash: status_with_witness.latest_valid_hash,
            status: status_with_witness.status,
            validation_error: status_with_witness.validation_error,
        })
    }

    pub async fn handle_new_payload(&self, request: Request) -> Result<Response, ErrorResponse> {
        tracing::info!("processing new payload from client");
        let (id, json_execution_payload, versioned_hashes, parent_block_root) =
            self.decode_new_payload(request)?;

        let execution_payload = ExecutionPayload::from(json_execution_payload);
        let block_hash = execution_payload.block_hash();
        let block_number = execution_payload.block_number();
        let new_payload_request = Self::new_payload_request_from_parts(
            &execution_payload,
            versioned_hashes,
            parent_block_root,
        );

        // Check block hash prior to keying cache. This prevents responding with an incorrect
        // cached response for a request with a mismatch/invalid block hash.
        if let Err(e) = new_payload_request.verify_payload_block_hash() {
            tracing::warn!(
                block_hash = ?block_hash,
                block_number = ?block_number,
                error = ?e,
                "incorrect block hash"
            );
            return Err(ErrorResponse::invalid_request(
                id,
                format!("incorrect block hash {block_hash:?}"),
            ));
        }

        // If this is a *recent* payload, wait a short time for a definite response from the EL.
        // Chances are it's busy processing the payload sent by the controlling BN.
        let is_recent = self.is_recent_payload(block_number).await;
        if is_recent {
            let start = Instant::now();
            while start.elapsed().as_millis() < self.config.new_payload_wait_millis {
                if let Some(status) = self.get_cached_payload_status(&block_hash, true).await {
                    return Response::new(id, status);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        // Try again to get any status from the cache, or fall back on a SYNCING response.
        let status = if let Some(status) = self.get_cached_payload_status(&block_hash, false).await
        {
            if !Self::is_definite(&status) {
                tracing::info!("sending indefinite status on newPayload");
            }
            status
        } else {
            // Before sending a synthetic SYNCING response we MUST check the block (checked above)
            // and the versioned hashes (post-Deneb).
            if let Err(e) = new_payload_request.verify_versioned_hashes() {
                tracing::warn!(
                    block_hash = ?block_hash,
                    block_number = ?block_number,
                    error = ?e,
                    "incorrect versioned hashes"
                );
                return Err(ErrorResponse::invalid_request(
                    id,
                    "incorrect versioned hashes".into(),
                ));
            }

            if is_recent {
                tracing::info!("sending SYNCING response on recent newPayload");
            } else {
                tracing::info!("sending instant SYNCING response for old newPayload");
            }
            // Synthetic syncing response.
            JsonPayloadStatusV1 {
                status: JsonPayloadStatusV1Status::Syncing,
                latest_valid_hash: None,
                validation_error: None,
            }
        };

        Response::new(id, status)
    }

    fn new_payload_request_from_parts(
        execution_payload: &ExecutionPayload<E>,
        versioned_hashes: Option<Vec<VersionedHash>>,
        parent_beacon_block_root: Option<Hash256>,
    ) -> NewPayloadRequest<E> {
        match execution_payload {
            ExecutionPayload::Bellatrix(execution_payload) => {
                NewPayloadRequest::Bellatrix(NewPayloadRequestBellatrix { execution_payload })
            }
            ExecutionPayload::Capella(execution_payload) => {
                NewPayloadRequest::Capella(NewPayloadRequestCapella { execution_payload })
            }
            // The `versioned_hashes` and `parent_block_root` should also be populated post-Deneb.
            // We could error here, but we defer to `verify_payload_block_hash` and similar, or
            // the actual EL to do that validation.
            ExecutionPayload::Deneb(execution_payload) => {
                NewPayloadRequest::Deneb(NewPayloadRequestDeneb {
                    execution_payload,
                    versioned_hashes: versioned_hashes.unwrap_or_default(),
                    parent_beacon_block_root: parent_beacon_block_root.unwrap_or_default(),
                })
            }
            // TODO: Electra
            ExecutionPayload::Electra(_) => todo!("Electra"),
        }
    }

    #[allow(clippy::type_complexity)]
    fn decode_new_payload(
        &self,
        request: Request,
    ) -> Result<
        (
            JsonValue,
            JsonExecutionPayload<E>,
            Option<Vec<VersionedHash>>,
            Option<Hash256>,
        ),
        ErrorResponse,
    > {
        let method = request.method.clone();

        let (id, params) = request.parse_as::<Vec<JsonValue>>()?;

        let (versioned_hashes, parent_block_root) = if method == ENGINE_NEW_PAYLOAD_V3 {
            if params.len() != 3 {
                return Err(ErrorResponse::parse_error_generic(
                    id,
                    "wrong number of parameters for newPayloadV3".to_string(),
                ));
            }
            let versioned_hashes = serde_json::from_value(params[1].clone())
                .map_err(|e| ErrorResponse::parse_error(id.clone(), e))?;
            let parent_block_root = serde_json::from_value(params[2].clone())
                .map_err(|e| ErrorResponse::parse_error(id.clone(), e))?;
            (Some(versioned_hashes), Some(parent_block_root))
        } else if params.len() == 1 {
            (None, None)
        } else {
            return Err(ErrorResponse::parse_error_generic(
                id,
                format!("wrong number of parameters for {method}: {}", params.len()),
            ));
        };

        let payload_json = params[0].clone();
        let QuantityU64 { value: timestamp } =
            if let Some(timestamp_json) = payload_json.get("timestamp") {
                serde_json::from_value(timestamp_json.clone())
                    .map_err(|e| ErrorResponse::parse_error(id.clone(), e))?
            } else {
                return Err(ErrorResponse::parse_error_generic(
                    id,
                    "timestamp value missing".to_string(),
                ));
            };

        let slot = self.timestamp_to_slot(timestamp).ok_or_else(|| {
            ErrorResponse::parse_error_generic(
                id.clone(),
                format!("invalid timestamp: {timestamp}"),
            )
        })?;

        let fork_name = self.spec.fork_name_at_slot::<E>(slot);

        let payload = if method == ENGINE_NEW_PAYLOAD_V1 || fork_name == ForkName::Bellatrix {
            serde_json::from_value(payload_json).map(JsonExecutionPayload::V1)
        } else if method == ENGINE_NEW_PAYLOAD_V2 || fork_name == ForkName::Capella {
            serde_json::from_value(payload_json).map(JsonExecutionPayload::V2)
        } else {
            serde_json::from_value(payload_json).map(JsonExecutionPayload::V3)
        }
        .map_err(|e| ErrorResponse::parse_error(id.clone(), e))?;

        Ok((id, payload, versioned_hashes, parent_block_root))
    }

    pub fn timestamp_to_slot(&self, timestamp: u64) -> Option<Slot> {
        timestamp
            .checked_sub(self.genesis_time)?
            .checked_div(self.spec.seconds_per_slot)
            .map(Slot::new)
    }

    pub async fn get_cached_payload_status(
        &self,
        execution_block_hash: &ExecutionBlockHash,
        definite_only: bool,
    ) -> Option<JsonPayloadStatusV1> {
        let mut cache = self.new_payload_cache.lock().await;
        if let Some(existing) = cache.get(execution_block_hash) {
            if !definite_only || Self::is_definite(&existing.status) {
                return Some(existing.status.clone());
            }
        }
        None
    }

    /// Return the highest `block_number` of any cached payload, or 0 if none is cached.
    ///
    /// This is useful for approximately time-based cutoffs & heuristics.
    pub async fn highest_cached_payload_number(&self) -> u64 {
        let cache = self.new_payload_cache.lock().await;
        cache
            .iter()
            .map(|(_, entry)| entry.block_number)
            .max()
            .unwrap_or(0)
    }

    /// Check if the given block number is recent based on the `highest_cached_payload_number`.
    pub async fn is_recent_payload(&self, block_number: u64) -> bool {
        let cutoff = self
            .highest_cached_payload_number()
            .await
            .saturating_sub(self.config.new_payload_wait_cutoff);
        block_number >= cutoff
    }
}
