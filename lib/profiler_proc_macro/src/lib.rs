extern crate proc_macro;

use ::syn::{parse_macro_input, ItemFn};
use proc_macro::TokenStream;

#[proc_macro_attribute]
// #[cfg(feature = "profiling_enabled")]
pub fn trace(_: TokenStream, input: TokenStream) -> TokenStream {
    let mut input_string = input.to_string();
    let input_fn = parse_macro_input!(input as ItemFn);
    let string_to_insert = format!(
        "let _span = tracy_client::span!(\"{}\");",
        input_fn.sig.ident
    );

    if let Some(idx) = input_string.find("{") {
        input_string.insert_str(idx + 1, &string_to_insert);
    }
    let result = input_string.parse().unwrap();

    result
}

/*
#[proc_macro_attribute]
#[cfg(not(feature = "profiling_enabled"))]
pub fn trace(_: TokenStream, input: TokenStream) -> TokenStream {
    input
}
*/
