#[doc(hidden)]
#[macro_export]
macro_rules! cfg_tokio {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "tokio")]
            #[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
            $item
        )*
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! cfg_serde {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "serde")]
            #[cfg_attr(docsrs, doc(cfg(feature = "serde")))]
            $item
        )*
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! cfg_uuid {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "uuid")]
            #[cfg_attr(docsrs, doc(cfg(feature = "uuid")))]
            $item
        )*
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! cfg_chrono {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "chrono")]
            #[cfg_attr(docsrs, doc(cfg(feature = "chrono")))]
            $item
        )*
    }
}
