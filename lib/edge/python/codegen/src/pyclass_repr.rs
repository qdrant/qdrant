pub fn pyclass_repr(input: proc_macro2::TokenStream) -> syn::Result<proc_macro2::TokenStream> {
    let impl_block: syn::ItemImpl = syn::parse2(input)?;

    let type_name = &impl_block.self_ty;
    let mut fields = Vec::new();

    for item in &impl_block.items {
        let syn::ImplItem::Fn(func) = item else {
            continue;
        };

        if !func.attrs.iter().any(|attr| attr.path().is_ident("getter")) {
            continue;
        }

        fields.push(&func.sig.ident);
    }

    let output = quote::quote! {
        #impl_block

        impl crate::repr::Repr for #type_name {
            fn fmt(&self, f: &mut crate::repr::Formatter<'_>) -> std::fmt::Result {
                use crate::repr::WriteExt as _;

                f.class::<Self>(&[
                    #( (stringify!(#fields), &self.#fields()) ),*
                ])
            }
        }
    };

    Ok(output)
}
