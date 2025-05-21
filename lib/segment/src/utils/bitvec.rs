use bitvec::store::BitStore;
use bitvec::vec::BitVec;

/// Sets all bits in the bitvec to `bit`.
pub fn bitvec_set_all(out: &mut BitVec<usize>, bit: bool) {
    out.as_raw_mut_slice().fill_with(|| {
        BitStore::new(if bit {
            !<<usize as bitvec::store::BitStore>::Mem>::ZERO
        } else {
            <<usize as bitvec::store::BitStore>::Mem>::ZERO
        })
    });
}
