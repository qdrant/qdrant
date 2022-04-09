extern crate proc_macro;

use proc_macro::TokenStream;

#[proc_macro_attribute]
#[cfg(feature = "tracy")]
pub fn trace(_: TokenStream, input: TokenStream) -> TokenStream {
    use ::syn::{parse_macro_input, ItemFn};

    let mut input_string = input.to_string();
    let input_fn = parse_macro_input!(input as ItemFn);
    let string_to_insert = format!(
        "let _span = tracy_client::span!(\"{}\");",
        input_fn.sig.ident
    );

    if let Some(idx) = input_string.find('{') {
        input_string.insert_str(idx + 1, &string_to_insert);
    }
    input_string.parse().unwrap()
}

#[proc_macro_attribute]
#[cfg(not(feature = "tracy"))]
pub fn trace(_: TokenStream, input: TokenStream) -> TokenStream {
    input
}
