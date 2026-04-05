use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::types::VectorNameBuf;
use shard::operations::vector_name_ops::VectorNameConfig;
use shard::operations::{
    CollectionUpdateOperations, CreateVectorName, DeleteVectorName, VectorNameOperations,
};

use crate::collection::Collection;
use crate::operations::types::CollectionResult;
use crate::shards::shard_trait::WaitUntil;

impl Collection {
    pub async fn create_named_vector(
        &self,
        vector_name: VectorNameBuf,
        config: VectorNameConfig,
        hw_acc: HwMeasurementAcc,
    ) -> CollectionResult<()> {
        let operation = CollectionUpdateOperations::VectorNameOperation(
            VectorNameOperations::CreateVectorName(CreateVectorName {
                vector_name,
                config,
            }),
        );

        // Called from consensus, so we use wait=false since the operation
        // should be fast (creates empty/placeholder storage)
        self.update_all_local(operation, WaitUntil::from(false), hw_acc)
            .await?;

        Ok(())
    }

    pub async fn delete_named_vector(
        &self,
        vector_name: VectorNameBuf,
    ) -> CollectionResult<()> {
        let operation = CollectionUpdateOperations::VectorNameOperation(
            VectorNameOperations::DeleteVectorName(DeleteVectorName { vector_name }),
        );

        self.update_all_local(operation, WaitUntil::from(false), HwMeasurementAcc::disposable())
            .await?;

        Ok(())
    }
}
