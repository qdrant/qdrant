use proc_macro2::TokenStream;
use quote::quote;
use syn::{Error, FnArg, Ident, ImplItem, ItemImpl, PatType, Result, ReturnType, parse2};

/// Attribute macro for `impl Trait for Service` blocks.
///
/// Generates a wrapper struct and a delegating trait impl that automatically
/// extracts `collection_name` from every request and attaches `CollectionName`
/// to every response. Zero manual bookkeeping — new trait methods are picked up
/// automatically from the annotated impl block.
///
/// ```ignore
/// #[macros::attach_collection_name(PointsTelemetryWrapper)]
/// #[tonic::async_trait]
/// impl Points for PointsService { … }
/// ```
pub fn attach_collection_name(attr: TokenStream, item: TokenStream) -> Result<TokenStream> {
    let wrapper_name: Ident = parse2(attr)?;
    let impl_block: ItemImpl = parse2(item.clone())?;

    let trait_path = impl_block
        .trait_
        .as_ref()
        .map(|(_, path, _)| path)
        .ok_or_else(|| Error::new_spanned(&impl_block, "expected a trait impl"))?;

    let delegated_methods = impl_block
        .items
        .iter()
        .filter_map(|item| match item {
            ImplItem::Fn(method) => Some(method),
            _ => None,
        })
        .map(|method| {
            let name = &method.sig.ident;

            let req_type = method
                .sig
                .inputs
                .iter()
                .nth(1) // skip &self
                .and_then(|arg| match arg {
                    FnArg::Typed(PatType { ty, .. }) => Some(ty.as_ref()),
                    _ => None,
                })
                .ok_or_else(|| {
                    Error::new_spanned(name, "expected a request parameter after &self")
                })?;

            let ret_type = match &method.sig.output {
                ReturnType::Type(_, ty) => ty,
                _ => {
                    return Err(Error::new_spanned(name, "expected a return type"));
                }
            };

            Ok(quote! {
                fn #name<'life0, 'async_trait>(
                    &'life0 self,
                    request: #req_type,
                ) -> futures_util::future::BoxFuture<'async_trait, #ret_type>
                where
                    'life0: 'async_trait,
                    Self: 'async_trait,
                {
                    Box::pin(async move {
                        let collection_name = request.get_ref().collection_name.clone();
                        let mut response = self.inner.#name(request).await?;
                        response.extensions_mut().insert(
                            crate::common::telemetry_ops::requests_telemetry::CollectionName(
                                collection_name,
                            ),
                        );
                        Ok(response)
                    })
                }
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(quote! {
        #item

        pub struct #wrapper_name<T> {
            inner: T,
        }

        impl<T> #wrapper_name<T> {
            pub fn new(inner: T) -> Self {
                Self { inner }
            }
        }

        impl<T: #trait_path> #trait_path for #wrapper_name<T> {
            #(#delegated_methods)*
        }
    })
}
