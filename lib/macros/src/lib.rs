use proc_macro::TokenStream;

mod anonymize;

/// Grep for `trait Anonymize` for doc.
#[proc_macro_derive(Anonymize, attributes(anonymize))]
pub fn derive_anonymize(input: TokenStream) -> TokenStream {
    match anonymize::derive_anonymize(input.into()) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}
