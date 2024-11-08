use tonic::Status;

use crate::conversions::json::{dict_to_proto, json_to_proto, proto_dict_to_json, proto_to_json};
use crate::grpc::qdrant as grpc;
use crate::rest::{schema as rest, Options};

impl From<rest::Document> for grpc::Document {
    fn from(document: rest::Document) -> Self {
        Self {
            text: document.text,
            model: document.model,
            options: document
                .options
                .options
                .map(dict_to_proto)
                .unwrap_or_default(),
        }
    }
}

impl TryFrom<grpc::Document> for rest::Document {
    type Error = Status;

    fn try_from(document: grpc::Document) -> Result<Self, Self::Error> {
        Ok(Self {
            text: document.text,
            model: document.model,
            options: Options {
                options: Some(proto_dict_to_json(document.options)?),
            },
        })
    }
}

impl From<rest::Image> for grpc::Image {
    fn from(image: rest::Image) -> Self {
        Self {
            image: Some(json_to_proto(image.image)),
            model: image.model,
            options: image.options.options.map(dict_to_proto).unwrap_or_default(),
        }
    }
}

impl TryFrom<grpc::Image> for rest::Image {
    type Error = Status;

    fn try_from(image: grpc::Image) -> Result<Self, Self::Error> {
        let grpc::Image {
            image,
            model,
            options,
        } = image;

        let image = image.ok_or_else(|| Status::invalid_argument("Empty image is not allowed"))?;

        Ok(Self {
            image: proto_to_json(image)?,
            model,
            options: Options {
                options: Some(proto_dict_to_json(options)?),
            },
        })
    }
}

impl From<rest::InferenceObject> for grpc::InferenceObject {
    fn from(object: rest::InferenceObject) -> Self {
        Self {
            object: Some(json_to_proto(object.object)),
            model: object.model,
            options: object
                .options
                .options
                .map(dict_to_proto)
                .unwrap_or_default(),
        }
    }
}

impl TryFrom<grpc::InferenceObject> for rest::InferenceObject {
    type Error = Status;

    fn try_from(object: grpc::InferenceObject) -> Result<Self, Self::Error> {
        let grpc::InferenceObject {
            object,
            model,
            options,
        } = object;

        let object =
            object.ok_or_else(|| Status::invalid_argument("Empty object is not allowed"))?;

        Ok(Self {
            object: proto_to_json(object)?,
            model,
            options: Options {
                options: Some(proto_dict_to_json(options)?),
            },
        })
    }
}
