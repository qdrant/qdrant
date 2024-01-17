/// Similar to `#[derive(JsonSchema)]`, but allows to override `schema_name()`
/// for each generic specialization using the following syntax:
/// ```
/// #[macro_rules_attribute::macro_rules_derive(schemars_rename_generics)]
/// #[derive_args(<i32> => "NewName", ...)]
/// ```
/// Workaround for https://github.com/GREsau/schemars/issues/193
macro_rules! schemars_rename_generics {
    {
        #[doc = $doc:literal]
        #[derive_args(
            $(,)*
            <$($old_params:ident),*> => $new_name:literal
            $( $rest:tt )*
        )]
        $( #[$attrs:meta] )*
        $vis:vis struct $name:ident<$($param:ident),*> { $($body:tt)* }
    } => {
        impl ::schemars::JsonSchema for $name<$($old_params),*> {
            fn schema_name() -> String {
                $new_name.to_string()
            }
            fn json_schema(gen: &mut ::schemars::gen::SchemaGenerator) -> ::schemars::schema::Schema {
                #[doc = $doc]
                #[derive(::schemars::JsonSchema)]
                $( #[$attrs] )*
                struct Temp<$($param),*>{ $($body)* }
                Temp::<$($old_params),*>::json_schema(gen)
            }
        }
        $crate::common::macros::schemars_rename_generics! {
            #[doc = $doc]
            #[derive_args( $( $rest )* )]
            $( #[$attrs] )*
            $vis struct $name<$($param),*>
            { $($body)* }
        }
    };
    { #[doc = $doc:literal] #[derive_args()] $( $rest:tt )* } => {}
}
pub(crate) use schemars_rename_generics;
