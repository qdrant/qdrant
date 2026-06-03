use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use crate::common::operation_error::OperationResult;
use crate::index::field_index::LiveReload;
use crate::index::field_index::numeric_index::Encodable;
<<<<<<< HEAD
use crate::index::field_index::numeric_index::on_disk_numeric_index::OnDiskNumericIndex;
use crate::index::field_index::numeric_point::Numericable;
use crate::index::field_index::on_disk_point_to_values::StoredValue;

impl<T: Encodable + Numericable + Default + StoredValue + 'static, S: UniversalRead> LiveReload
    for OnDiskNumericIndex<T, S>
=======
use crate::index::field_index::numeric_index::universal_numeric_index::UniversalNumericIndex;
use crate::index::field_index::numeric_point::Numericable;
use crate::index::field_index::stored_point_to_values::StoredValue;

impl<T: Encodable + Numericable + Default + StoredValue + 'static, S: UniversalRead> LiveReload
    for UniversalNumericIndex<T, S>
>>>>>>> a1e2bdd1a (feat: implement LiveReload for numeric index)
{
    type Fs = S::Fs;

    fn live_reload(
        &mut self,
        _fs: &S::Fs,
        deleted_points: &[PointOffsetType],
        _new_points: &[PointOffsetType],
        _hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        // No on-disk state changes on reload: this index is immutable, so only
        // the in-memory deletion bitvec is patched. `fs` / `new_points` are
        // unused because nothing is appended after build.
        for deleted_point in deleted_points {
            self.remove_point(*deleted_point);
        }

        Ok(())
    }
}
