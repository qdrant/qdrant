mod pyclass_repr;

/// `#[pyclass_repr]` - implements `trait Repr`.
///
/// Only methods with `#[getter]` are included into the output.
///
/// # Usage example
///
/// Input:
/// ```ignore
/// #[pyclass_repr]
/// #[pymethods]
/// impl PyMyStruct {
///     #[getter]
///     pub fn foo(&self) -> &str {
///         &self.0.foo
///     }
///
///     #[getter]
///     pub fn bar(&self) -> f32  {
///         self.0.bar
///     }
///
///     pub fn __repr__(&self) -> String {
///         self.repr()
///     }
/// }
/// ```
///
/// Generated output is roughly equivalent to:
/// ```ignore
/// impl crate::repr::Repr for PyMyStruct {
///     fn fmt(&self, f: &mut crate::repr::Formatter<'_>) -> std::fmt::Result {
///         use crate::repr::WriteExt as _;
///
///         f.class::<Self>(&[
///             ("foo", &self.foo()),
///             ("bar", &self.bar()),
///         ])
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn pyclass_repr(
    _attributes: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    match pyclass_repr::pyclass_repr(input.into()) {
        Ok(output) => output.into(),
        Err(error) => error.to_compile_error().into(),
    }
}
