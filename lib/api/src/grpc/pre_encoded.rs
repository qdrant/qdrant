//! Unary gRPC requests whose protobuf body is encoded ahead of time.
//!
//! The generated clients take the message by value and let `tonic` encode it while the request body
//! is polled, which happens on the caller's async runtime. Encoding is a full pass over the message,
//! so for a large message that is a long synchronous block on an async worker, repeated on every
//! retry the channel pool makes.
//!
//! [`PreEncodedMessage`] encodes once, wherever the caller chooses, and hands `tonic` bytes to copy
//! out. Cloning it for a retry is a refcount bump.

use std::marker::PhantomData;

use prost::Message;
use prost::bytes::{BufMut, Bytes};
use tonic::client::{Grpc, GrpcService};
use tonic::codec::{BufferSettings, Codec, EncodeBuf, Encoder};
use tonic::codegen::{Body, StdError, http};
use tonic::{GrpcMethod, Request, Response, Status};
use tonic_prost::ProstDecoder;

use crate::grpc::qdrant::points_internal_server::SERVICE_NAME;
use crate::grpc::qdrant::{PointsOperationResponseInternal, UpdateBatchInternal};

/// Identity of `PointsInternal/UpdateBatch`, as the generated client spells it.
///
/// Bypassing the generated client means these no longer follow the proto automatically, so
/// `tests::update_batch_rpc_matches_proto` checks them against the compiled descriptor set.
const UPDATE_BATCH_METHOD: &str = "UpdateBatch";
const UPDATE_BATCH_PATH: &str = "/qdrant.PointsInternal/UpdateBatch";

/// A protobuf message that has already been encoded to its wire representation.
///
/// Cloning shares the encoded bytes, so retrying a request does not encode it again.
#[derive(Clone, Debug)]
pub struct PreEncodedMessage<M> {
    body: Bytes,
    _pd: PhantomData<fn() -> M>,
}

impl<M: Message> PreEncodedMessage<M> {
    /// Encode `message`.
    ///
    /// This is a full pass over the message. Callers holding a large message should do it outside
    /// an async runtime.
    pub fn encode(message: &M) -> Self {
        Self {
            body: Bytes::from(message.encode_to_vec()),
            _pd: PhantomData,
        }
    }
}

/// [`PointsInternalClient::update_batch`] with an already-encoded request body.
///
/// `grpc` is what [`PointsInternalClient::new`] wraps, configured the same way.
///
/// [`PointsInternalClient::update_batch`]: crate::grpc::qdrant::points_internal_client::PointsInternalClient::update_batch
/// [`PointsInternalClient::new`]: crate::grpc::qdrant::points_internal_client::PointsInternalClient::new
pub async fn update_batch_pre_encoded<T>(
    mut grpc: Grpc<T>,
    request: PreEncodedMessage<UpdateBatchInternal>,
) -> Result<Response<PointsOperationResponseInternal>, Status>
where
    T: GrpcService<tonic::body::Body>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    grpc.ready()
        .await
        .map_err(|err| Status::unknown(format!("Service was not ready: {}", err.into())))?;

    let mut request = Request::new(request);
    request
        .extensions_mut()
        .insert(GrpcMethod::new(SERVICE_NAME, UPDATE_BATCH_METHOD));

    grpc.unary(
        request,
        http::uri::PathAndQuery::from_static(UPDATE_BATCH_PATH),
        PreEncodedCodec::default(),
    )
    .await
}

/// Writes an already-encoded request body as is, decodes the response with prost.
struct PreEncodedCodec<M, U> {
    _pd: PhantomData<fn() -> (M, U)>,
}

impl<M, U> Default for PreEncodedCodec<M, U> {
    fn default() -> Self {
        Self { _pd: PhantomData }
    }
}

impl<M, U> Codec for PreEncodedCodec<M, U>
where
    M: Send + 'static,
    U: Message + Default + Send + 'static,
{
    type Encode = PreEncodedMessage<M>;
    type Decode = U;

    type Encoder = PreEncodedEncoder<M>;
    type Decoder = ProstDecoder<U>;

    fn encoder(&mut self) -> Self::Encoder {
        PreEncodedEncoder { _pd: PhantomData }
    }

    fn decoder(&mut self) -> Self::Decoder {
        ProstDecoder::new(BufferSettings::default())
    }
}

struct PreEncodedEncoder<M> {
    _pd: PhantomData<fn() -> M>,
}

impl<M> Encoder for PreEncodedEncoder<M> {
    type Item = PreEncodedMessage<M>;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        // `tonic` writes the gRPC frame header around this itself.
        dst.reserve(item.body.len());
        dst.put_slice(&item.body);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use prost_types::FileDescriptorSet;

    use super::*;
    use crate::grpc::QDRANT_DESCRIPTOR_SET;

    /// The generated client derives its path and message types from the proto, this module spells
    /// them out. Check they still agree, so renaming the RPC cannot silently break this call.
    #[test]
    fn update_batch_rpc_matches_proto() {
        let descriptor_set = FileDescriptorSet::decode(QDRANT_DESCRIPTOR_SET).unwrap();

        let method = descriptor_set
            .file
            .iter()
            .flat_map(|file| {
                let package = file.package();
                file.service.iter().map(move |service| (package, service))
            })
            .filter(|(package, service)| format!("{package}.{}", service.name()) == SERVICE_NAME)
            .flat_map(|(_, service)| &service.method)
            .find(|method| method.name() == UPDATE_BATCH_METHOD)
            .expect("proto defines the RPC this module calls");

        assert_eq!(
            UPDATE_BATCH_PATH,
            format!("/{SERVICE_NAME}/{UPDATE_BATCH_METHOD}"),
        );
        // The types `update_batch_pre_encoded` sends and decodes.
        assert_eq!(method.input_type(), ".qdrant.UpdateBatchInternal");
        assert_eq!(
            method.output_type(),
            ".qdrant.PointsOperationResponseInternal",
        );
        assert!(!method.client_streaming() && !method.server_streaming());
    }

    #[test]
    fn pre_encoded_message_matches_prost() {
        let message = UpdateBatchInternal {
            operations: vec![],
            wait_override: Some(1),
        };

        let pre_encoded = PreEncodedMessage::encode(&message);
        assert_eq!(pre_encoded.body.len(), message.encoded_len());
        assert_eq!(
            UpdateBatchInternal::decode(pre_encoded.body.clone()).unwrap(),
            message,
        );
    }
}
