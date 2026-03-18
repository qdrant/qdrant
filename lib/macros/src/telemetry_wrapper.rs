use proc_macro2::TokenStream;
use quote::quote;
use syn::{Error, FnArg, Ident, ImplItem, ItemImpl, PatType, Result, ReturnType, parse2};

/// Generates a wrapper struct that delegates every trait method to the inner
/// service, extracting `collection_name` from the request and attaching
/// `CollectionName` to the response automatically.
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

    let methods = impl_block
        .items
        .iter()
        .filter_map(|i| match i {
            ImplItem::Fn(m) => Some(build_method(m)),
            _ => None,
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(quote! {
        #item

        pub struct #wrapper_name<T> { inner: T }

        impl<T> #wrapper_name<T> {
            pub fn new(inner: T) -> Self { Self { inner } }
        }

        impl<T: #trait_path> #trait_path for #wrapper_name<T> {
            #(#methods)*
        }
    })
}

fn build_method(method: &syn::ImplItemFn) -> Result<TokenStream> {
    let name = &method.sig.ident;
    let req_type = match method.sig.inputs.iter().nth(1) {
        Some(FnArg::Typed(PatType { ty, .. })) => ty.as_ref(),
        _ => return Err(Error::new_spanned(name, "expected request parameter")),
    };
    let ReturnType::Type(_, ret_type) = &method.sig.output else {
        return Err(Error::new_spanned(name, "expected return type"));
    };

    Ok(quote! {
        fn #name<'life0, 'async_trait>(
            &'life0 self, request: #req_type,
        ) -> futures_util::future::BoxFuture<'async_trait, #ret_type>
        where 'life0: 'async_trait, Self: 'async_trait,
        {
            Box::pin(async move {
                let cn = request.get_ref().collection_name.clone();
                let mut resp = self.inner.#name(request).await?;
                resp.extensions_mut().insert(
                    crate::common::telemetry_ops::requests_telemetry::CollectionName(cn),
                );
                Ok(resp)
            })
        }
    })
}
