/// Default type conversions between builders and their built types.
macro_rules! builder_type_conversions {
    ($main_type:ident,$builder_type:ident,$build_fn:ident) => {
        impl From<$builder_type> for $main_type {
            fn from(value: $builder_type) -> Self {
                value.$build_fn().unwrap()
            }
        }

        impl From<&mut $builder_type> for $main_type {
            fn from(value: &mut $builder_type) -> Self {
                value.clone().$build_fn().unwrap()
            }
        }
    };
    ($main_type:ident,$builder_type:ident) => {
        builder_type_conversions!($main_type, $builder_type, build_inner);
    };
}
