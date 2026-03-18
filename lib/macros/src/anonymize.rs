use std::borrow::Cow;

use proc_macro2::TokenStream;
use quote::{ToTokens as _, format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned as _;
use syn::{Attribute, Data, DeriveInput, Error, Expr, Index, LitBool, Path, Result, Token, parse2};

mod kw {
    syn::custom_keyword!(value);
    syn::custom_keyword!(with);
}

#[derive(Clone)]
enum AnonymizeAttr {
    /// `#[anonymize(true)]`
    True,
    /// `#[anonymize(false)]`
    False,
    /// `#[anonymize(value = 42)]`
    Value(Expr),
    /// `#[anonymize(with = path::to::function)]`
    With(Path),
}

impl Parse for AnonymizeAttr {
    fn parse(input: ParseStream) -> Result<Self> {
        let lookahead = input.lookahead1();
        let result = if lookahead.peek(LitBool) {
            if input.parse::<LitBool>()?.value {
                AnonymizeAttr::True
            } else {
                AnonymizeAttr::False
            }
        } else if lookahead.peek(kw::value) {
            input.parse::<kw::value>()?;
            input.parse::<Token![=]>()?;
            AnonymizeAttr::Value(input.parse()?)
        } else if lookahead.peek(kw::with) {
            input.parse::<kw::with>()?;
            input.parse::<Token![=]>()?;
            AnonymizeAttr::With(input.parse()?)
        } else {
            return Err(lookahead.error());
        };
        input.parse::<Option<Token![,]>>()?;
        Ok(result)
    }
}

fn parse_attrs<'a>(
    default: &'a AnonymizeAttr,
    attrs: &[Attribute],
) -> Result<Cow<'a, AnonymizeAttr>> {
    let mut it = attrs.iter().filter(|a| a.path().is_ident("anonymize"));
    match (it.next(), it.next()) {
        (None, None) => Ok(Cow::Borrowed(default)),
        (Some(attr), None) => attr.parse_args().map(Cow::Owned),
        (_, Some(attr2)) => Err(Error::new(
            attr2.span(),
            "only one #[anonymize(...)] attribute is allowed",
        )),
    }
}

fn anonymize_expr(attr: &AnonymizeAttr, expr: TokenStream) -> TokenStream {
    match attr {
        AnonymizeAttr::True => quote! { Anonymize::anonymize(#expr) },
        AnonymizeAttr::False => quote! { ::core::clone::Clone::clone(#expr) },
        AnonymizeAttr::Value(expr) => quote! { #expr },
        AnonymizeAttr::With(path) => quote! { #path(#expr) },
    }
}

pub(crate) fn derive_anonymize(input: TokenStream) -> Result<TokenStream> {
    let input_ast: DeriveInput = parse2(input)?;
    let container_attr = parse_attrs(&AnonymizeAttr::True, &input_ast.attrs)?;

    let function_body = match &input_ast.data {
        Data::Struct(data_struct) => {
            let field_initializers = data_struct
                .fields
                .iter()
                .enumerate()
                .map(|(idx, field)| {
                    let fi = match &field.ident {
                        Some(id) => quote! { #id },
                        None => Index::from(idx).into_token_stream(),
                    };
                    let field_attr = parse_attrs(&container_attr, &field.attrs)?;
                    let expr = anonymize_expr(&field_attr, quote! { &self.#fi });
                    Ok(quote! { #fi: #expr, })
                })
                .collect::<Result<Vec<_>>>()?;
            quote! { Self { #(#field_initializers)* } }
        }
        Data::Enum(data_enum) => {
            let arms = data_enum
                .variants
                .iter()
                .map(|variant| {
                    let variant_attr = parse_attrs(&container_attr, &variant.attrs)?;

                    let mut pattern = Vec::with_capacity(variant.fields.iter().len());
                    let mut body = Vec::with_capacity(variant.fields.iter().len());
                    for (idx, field) in variant.fields.iter().enumerate() {
                        let fi = match &field.ident {
                            Some(id) => quote! { #id },
                            None => Index::from(idx).into_token_stream(),
                        };
                        let binding = format_ident!("__anonymize_binding_{idx}");
                        let field_attr = parse_attrs(&variant_attr, &field.attrs)?;
                        let expr = anonymize_expr(&field_attr, quote! { #binding });
                        pattern.push(quote! { #fi: #binding, });
                        body.push(quote! { #fi: #expr, });
                    }

                    let ident = &variant.ident;
                    Ok(quote! {
                        Self::#ident { #(#pattern)* } => Self::#ident { #(#body)* },
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            quote! { match self { #(#arms)* } }
        }
        Data::Union(data_union) => {
            return Err(Error::new(
                data_union.union_token.span,
                "unions are not supported",
            ));
        }
    };

    let ident = &input_ast.ident;
    Ok(quote! {
        impl Anonymize for #ident {
            fn anonymize(&self) -> Self {
                #function_body
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[track_caller]
    fn check_derive(input: TokenStream, expected: TokenStream) {
        let actual = derive_anonymize(input).unwrap();
        assert_eq!(
            actual.to_string(),
            expected.to_string(),
            "\n// actual\n{}\n\n// expected\n{}",
            pretty(actual),
            pretty(expected),
        );
    }

    fn pretty(ts: TokenStream) -> String {
        syn::parse_file(&ts.to_string())
            .map_or_else(|e| e.to_string(), |f| prettyplease::unparse(&f))
    }

    #[test]
    fn test_derive_anonymize() {
        check_derive(
            quote! {
                struct Test {
                    foo: Foo,
                    #[anonymize(true)]
                    bar: Bar,
                    #[anonymize(false)]
                    baz: Baz,
                    #[anonymize(with = crate::anonymize_qux)]
                    qux: Qux,
                    #[anonymize(value = 42)]
                    quux: u32,
                }
            },
            quote! {
                impl Anonymize for Test {
                    fn anonymize(&self) -> Self {
                        Self {
                            foo: Anonymize::anonymize(&self.foo),
                            bar: Anonymize::anonymize(&self.bar),
                            baz: ::core::clone::Clone::clone(&self.baz),
                            qux: crate::anonymize_qux(&self.qux),
                            quux: 42,
                        }
                    }
                }
            },
        );

        check_derive(
            quote! {
                #[anonymize(false)]
                struct Test {
                    foo: Foo,
                    #[anonymize(true)]
                    bar: Bar,
                    #[anonymize(false)]
                    baz: Baz,
                    #[anonymize(with = crate::anonymize_qux)]
                    qux: Qux,
                    #[anonymize(value = 42)]
                    quux: u32,
                }
            },
            quote! {
                impl Anonymize for Test {
                    fn anonymize(&self) -> Self {
                        Self {
                            foo: ::core::clone::Clone::clone(&self.foo),
                            bar: Anonymize::anonymize(&self.bar),
                            baz: ::core::clone::Clone::clone(&self.baz),
                            qux: crate::anonymize_qux(&self.qux),
                            quux: 42,
                        }
                    }
                }
            },
        );

        check_derive(
            quote! {
                struct Test(
                    Foo,
                    #[anonymize(with = crate::anonymize_bar)] Bar,
                    #[anonymize(false)] Baz,
                    #[anonymize(value = 42)] u32,
                );
            },
            quote! {
                impl Anonymize for Test {
                    fn anonymize(&self) -> Self {
                        Self {
                            0: Anonymize::anonymize(&self.0),
                            1: crate::anonymize_bar(&self.1),
                            2: ::core::clone::Clone::clone(&self.2),
                            3: 42,
                        }
                    }
                }
            },
        );

        check_derive(
            quote! {
                struct Test;
            },
            quote! {
                impl Anonymize for Test {
                    fn anonymize(&self) -> Self {
                        Self {}
                    }
                }
            },
        );

        check_derive(
            quote! {
                enum Test {
                    A,
                    B(),
                    C {},

                    D(Foo, Bar),
                    E {
                        foo: Foo,
                        bar: Bar,
                    },

                    F(#[anonymize(with = crate::anonymize_foo)] Foo, Bar),
                    H(#[anonymize(false)] Foo, Bar),
                    I(#[anonymize(value = 42)] u32, Bar),

                    #[anonymize(with = crate::anonymize_bar)]
                    J(Foo, #[anonymize(true)] Bar, Baz),
                }
            },
            quote! {
                impl Anonymize for Test {
                    fn anonymize(&self) -> Self {
                        match self {
                            Self::A {} => Self::A {},
                            Self::B {} => Self::B {},
                            Self::C {} => Self::C {},
                            Self::D {
                                0: __anonymize_binding_0,
                                1: __anonymize_binding_1,
                            } => Self::D {
                                0: Anonymize::anonymize(__anonymize_binding_0),
                                1: Anonymize::anonymize(__anonymize_binding_1),
                            },
                            Self::E {
                                foo: __anonymize_binding_0,
                                bar: __anonymize_binding_1,
                            } => Self::E {
                                foo: Anonymize::anonymize(__anonymize_binding_0),
                                bar: Anonymize::anonymize(__anonymize_binding_1),
                            },
                            Self::F {
                                0: __anonymize_binding_0,
                                1: __anonymize_binding_1,
                            } => Self::F {
                                0: crate::anonymize_foo(__anonymize_binding_0),
                                1: Anonymize::anonymize(__anonymize_binding_1),
                            },
                            Self::H {
                                0: __anonymize_binding_0,
                                1: __anonymize_binding_1,
                            } => Self::H {
                                0: ::core::clone::Clone::clone(__anonymize_binding_0),
                                1: Anonymize::anonymize(__anonymize_binding_1),
                            },
                            Self::I {
                                0: __anonymize_binding_0,
                                1: __anonymize_binding_1,
                            } => Self::I {
                                0: 42,
                                1: Anonymize::anonymize(__anonymize_binding_1),
                            },
                            Self::J {
                                0: __anonymize_binding_0,
                                1: __anonymize_binding_1,
                                2: __anonymize_binding_2,
                            } => Self::J {
                                0: crate::anonymize_bar(__anonymize_binding_0),
                                1: Anonymize::anonymize(__anonymize_binding_1),
                                2: crate::anonymize_bar(__anonymize_binding_2),
                            },
                        }
                    }
                }
            },
        );
    }
}
