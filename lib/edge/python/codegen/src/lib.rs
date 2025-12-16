mod pyclass_repr;

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
