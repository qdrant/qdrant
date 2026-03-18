use std::fmt;

#[derive(Copy, Clone, Debug)]
pub struct SerdeValue<'a>(pub &'a serde_value::Value);

impl fmt::Display for SerdeValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let val: &dyn fmt::Display = match &self.0 {
            serde_value::Value::Bool(val) => val,
            serde_value::Value::U8(val) => val,
            serde_value::Value::U16(val) => val,
            serde_value::Value::U32(val) => val,
            serde_value::Value::U64(val) => val,
            serde_value::Value::I8(val) => val,
            serde_value::Value::I16(val) => val,
            serde_value::Value::I32(val) => val,
            serde_value::Value::I64(val) => val,
            serde_value::Value::F32(val) => val,
            serde_value::Value::F64(val) => val,
            serde_value::Value::Char(val) => val,
            serde_value::Value::String(val) => val,
            serde_value::Value::Unit => &"Unit",
            serde_value::Value::Option(val) => return write!(f, "{val:?}"),
            serde_value::Value::Newtype(val) => return write!(f, "{val:?}"),
            serde_value::Value::Seq(val) => return write!(f, "{val:?}"),
            serde_value::Value::Map(val) => return write!(f, "{val:?}"),
            serde_value::Value::Bytes(val) => return write!(f, "{val:?}"),
        };

        val.fmt(f)
    }
}
