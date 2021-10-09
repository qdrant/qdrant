#[cfg(feature = "grpc")]
pub mod proto {
    pub struct GetCollectionsRequest {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for GetCollectionsRequest {
        #[inline]
        fn clone(&self) -> GetCollectionsRequest {
            match *self {
                GetCollectionsRequest {} => GetCollectionsRequest {},
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for GetCollectionsRequest {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for GetCollectionsRequest {
        #[inline]
        fn eq(&self, other: &GetCollectionsRequest) -> bool {
            match *other {
                GetCollectionsRequest {} => match *self {
                    GetCollectionsRequest {} => true,
                },
            }
        }
    }
    impl ::prost::Message for GetCollectionsRequest {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            match tag {
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0
        }
        fn clear(&mut self) {}
    }
    impl Default for GetCollectionsRequest {
        fn default() -> Self {
            GetCollectionsRequest {}
        }
    }
    impl ::core::fmt::Debug for GetCollectionsRequest {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("GetCollectionsRequest");
            builder.finish()
        }
    }
    pub struct CollectionDescription {
        #[prost(string, tag = "1")]
        pub name: ::prost::alloc::string::String,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for CollectionDescription {
        #[inline]
        fn clone(&self) -> CollectionDescription {
            match *self {
                CollectionDescription {
                    name: ref __self_0_0,
                } => CollectionDescription {
                    name: ::core::clone::Clone::clone(&(*__self_0_0)),
                },
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for CollectionDescription {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for CollectionDescription {
        #[inline]
        fn eq(&self, other: &CollectionDescription) -> bool {
            match *other {
                CollectionDescription {
                    name: ref __self_1_0,
                } => match *self {
                    CollectionDescription {
                        name: ref __self_0_0,
                    } => (*__self_0_0) == (*__self_1_0),
                },
            }
        }
        #[inline]
        fn ne(&self, other: &CollectionDescription) -> bool {
            match *other {
                CollectionDescription {
                    name: ref __self_1_0,
                } => match *self {
                    CollectionDescription {
                        name: ref __self_0_0,
                    } => (*__self_0_0) != (*__self_1_0),
                },
            }
        }
    }
    impl ::prost::Message for CollectionDescription {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
            if self.name != "" {
                ::prost::encoding::string::encode(1u32, &self.name, buf);
            }
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "CollectionDescription";
            match tag {
                1u32 => {
                    let mut value = &mut self.name;
                    ::prost::encoding::string::merge(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "name");
                            error
                        },
                    )
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0 + if self.name != "" {
                ::prost::encoding::string::encoded_len(1u32, &self.name)
            } else {
                0
            }
        }
        fn clear(&mut self) {
            self.name.clear();
        }
    }
    impl Default for CollectionDescription {
        fn default() -> Self {
            CollectionDescription {
                name: ::prost::alloc::string::String::new(),
            }
        }
    }
    impl ::core::fmt::Debug for CollectionDescription {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("CollectionDescription");
            let builder = {
                let wrapper = {
                    fn ScalarWrapper<T>(v: T) -> T {
                        v
                    }
                    ScalarWrapper(&self.name)
                };
                builder.field("name", &wrapper)
            };
            builder.finish()
        }
    }
    pub struct GetCollectionsResponse {
        #[prost(message, repeated, tag = "1")]
        pub collections: ::prost::alloc::vec::Vec<CollectionDescription>,
        #[prost(double, tag = "2")]
        pub time: f64,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for GetCollectionsResponse {
        #[inline]
        fn clone(&self) -> GetCollectionsResponse {
            match *self {
                GetCollectionsResponse {
                    collections: ref __self_0_0,
                    time: ref __self_0_1,
                } => GetCollectionsResponse {
                    collections: ::core::clone::Clone::clone(&(*__self_0_0)),
                    time: ::core::clone::Clone::clone(&(*__self_0_1)),
                },
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for GetCollectionsResponse {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for GetCollectionsResponse {
        #[inline]
        fn eq(&self, other: &GetCollectionsResponse) -> bool {
            match *other {
                GetCollectionsResponse {
                    collections: ref __self_1_0,
                    time: ref __self_1_1,
                } => match *self {
                    GetCollectionsResponse {
                        collections: ref __self_0_0,
                        time: ref __self_0_1,
                    } => (*__self_0_0) == (*__self_1_0) && (*__self_0_1) == (*__self_1_1),
                },
            }
        }
        #[inline]
        fn ne(&self, other: &GetCollectionsResponse) -> bool {
            match *other {
                GetCollectionsResponse {
                    collections: ref __self_1_0,
                    time: ref __self_1_1,
                } => match *self {
                    GetCollectionsResponse {
                        collections: ref __self_0_0,
                        time: ref __self_0_1,
                    } => (*__self_0_0) != (*__self_1_0) || (*__self_0_1) != (*__self_1_1),
                },
            }
        }
    }
    impl ::prost::Message for GetCollectionsResponse {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
            for msg in &self.collections {
                ::prost::encoding::message::encode(1u32, msg, buf);
            }
            if self.time != 0f64 {
                ::prost::encoding::double::encode(2u32, &self.time, buf);
            }
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "GetCollectionsResponse";
            match tag {
                1u32 => {
                    let mut value = &mut self.collections;
                    ::prost::encoding::message::merge_repeated(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "collections");
                            error
                        },
                    )
                }
                2u32 => {
                    let mut value = &mut self.time;
                    ::prost::encoding::double::merge(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "time");
                            error
                        },
                    )
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0 + ::prost::encoding::message::encoded_len_repeated(1u32, &self.collections)
                + if self.time != 0f64 {
                    ::prost::encoding::double::encoded_len(2u32, &self.time)
                } else {
                    0
                }
        }
        fn clear(&mut self) {
            self.collections.clear();
            self.time = 0f64;
        }
    }
    impl Default for GetCollectionsResponse {
        fn default() -> Self {
            GetCollectionsResponse {
                collections: ::core::default::Default::default(),
                time: 0f64,
            }
        }
    }
    impl ::core::fmt::Debug for GetCollectionsResponse {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("GetCollectionsResponse");
            let builder = {
                let wrapper = &self.collections;
                builder.field("collections", &wrapper)
            };
            let builder = {
                let wrapper = {
                    fn ScalarWrapper<T>(v: T) -> T {
                        v
                    }
                    ScalarWrapper(&self.time)
                };
                builder.field("time", &wrapper)
            };
            builder.finish()
        }
    }
    pub struct HnswConfigDiff {
        #[prost(uint64, optional, tag = "1")]
        pub m: ::core::option::Option<u64>,
        #[prost(uint64, optional, tag = "2")]
        pub ef_construct: ::core::option::Option<u64>,
        #[prost(uint64, optional, tag = "3")]
        pub full_scan_threshold: ::core::option::Option<u64>,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for HnswConfigDiff {
        #[inline]
        fn clone(&self) -> HnswConfigDiff {
            match *self {
                HnswConfigDiff {
                    m: ref __self_0_0,
                    ef_construct: ref __self_0_1,
                    full_scan_threshold: ref __self_0_2,
                } => HnswConfigDiff {
                    m: ::core::clone::Clone::clone(&(*__self_0_0)),
                    ef_construct: ::core::clone::Clone::clone(&(*__self_0_1)),
                    full_scan_threshold: ::core::clone::Clone::clone(&(*__self_0_2)),
                },
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for HnswConfigDiff {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for HnswConfigDiff {
        #[inline]
        fn eq(&self, other: &HnswConfigDiff) -> bool {
            match *other {
                HnswConfigDiff {
                    m: ref __self_1_0,
                    ef_construct: ref __self_1_1,
                    full_scan_threshold: ref __self_1_2,
                } => match *self {
                    HnswConfigDiff {
                        m: ref __self_0_0,
                        ef_construct: ref __self_0_1,
                        full_scan_threshold: ref __self_0_2,
                    } => {
                        (*__self_0_0) == (*__self_1_0)
                            && (*__self_0_1) == (*__self_1_1)
                            && (*__self_0_2) == (*__self_1_2)
                    }
                },
            }
        }
        #[inline]
        fn ne(&self, other: &HnswConfigDiff) -> bool {
            match *other {
                HnswConfigDiff {
                    m: ref __self_1_0,
                    ef_construct: ref __self_1_1,
                    full_scan_threshold: ref __self_1_2,
                } => match *self {
                    HnswConfigDiff {
                        m: ref __self_0_0,
                        ef_construct: ref __self_0_1,
                        full_scan_threshold: ref __self_0_2,
                    } => {
                        (*__self_0_0) != (*__self_1_0)
                            || (*__self_0_1) != (*__self_1_1)
                            || (*__self_0_2) != (*__self_1_2)
                    }
                },
            }
        }
    }
    impl ::prost::Message for HnswConfigDiff {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
            if let ::core::option::Option::Some(ref value) = self.m {
                ::prost::encoding::uint64::encode(1u32, value, buf);
            }
            if let ::core::option::Option::Some(ref value) = self.ef_construct {
                ::prost::encoding::uint64::encode(2u32, value, buf);
            }
            if let ::core::option::Option::Some(ref value) = self.full_scan_threshold {
                ::prost::encoding::uint64::encode(3u32, value, buf);
            }
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "HnswConfigDiff";
            match tag {
                1u32 => {
                    let mut value = &mut self.m;
                    ::prost::encoding::uint64::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "m");
                        error
                    })
                }
                2u32 => {
                    let mut value = &mut self.ef_construct;
                    ::prost::encoding::uint64::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "ef_construct");
                        error
                    })
                }
                3u32 => {
                    let mut value = &mut self.full_scan_threshold;
                    ::prost::encoding::uint64::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "full_scan_threshold");
                        error
                    })
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0 + self.m.as_ref().map_or(0, |value| {
                ::prost::encoding::uint64::encoded_len(1u32, value)
            }) + self.ef_construct.as_ref().map_or(0, |value| {
                ::prost::encoding::uint64::encoded_len(2u32, value)
            }) + self.full_scan_threshold.as_ref().map_or(0, |value| {
                ::prost::encoding::uint64::encoded_len(3u32, value)
            })
        }
        fn clear(&mut self) {
            self.m = ::core::option::Option::None;
            self.ef_construct = ::core::option::Option::None;
            self.full_scan_threshold = ::core::option::Option::None;
        }
    }
    impl Default for HnswConfigDiff {
        fn default() -> Self {
            HnswConfigDiff {
                m: ::core::option::Option::None,
                ef_construct: ::core::option::Option::None,
                full_scan_threshold: ::core::option::Option::None,
            }
        }
    }
    impl ::core::fmt::Debug for HnswConfigDiff {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("HnswConfigDiff");
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(&'a ::core::option::Option<u64>);
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            fn Inner<T>(v: T) -> T {
                                v
                            }
                            ::core::fmt::Debug::fmt(&self.0.as_ref().map(Inner), f)
                        }
                    }
                    ScalarWrapper(&self.m)
                };
                builder.field("m", &wrapper)
            };
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(&'a ::core::option::Option<u64>);
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            fn Inner<T>(v: T) -> T {
                                v
                            }
                            ::core::fmt::Debug::fmt(&self.0.as_ref().map(Inner), f)
                        }
                    }
                    ScalarWrapper(&self.ef_construct)
                };
                builder.field("ef_construct", &wrapper)
            };
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(&'a ::core::option::Option<u64>);
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            fn Inner<T>(v: T) -> T {
                                v
                            }
                            ::core::fmt::Debug::fmt(&self.0.as_ref().map(Inner), f)
                        }
                    }
                    ScalarWrapper(&self.full_scan_threshold)
                };
                builder.field("full_scan_threshold", &wrapper)
            };
            builder.finish()
        }
    }
    #[allow(dead_code)]
    impl HnswConfigDiff {
        ///Returns the value of `m`, or the default value if `m` is unset.
        pub fn m(&self) -> u64 {
            match self.m {
                ::core::option::Option::Some(val) => val,
                ::core::option::Option::None => 0u64,
            }
        }
        ///Returns the value of `ef_construct`, or the default value if `ef_construct` is unset.
        pub fn ef_construct(&self) -> u64 {
            match self.ef_construct {
                ::core::option::Option::Some(val) => val,
                ::core::option::Option::None => 0u64,
            }
        }
        ///Returns the value of `full_scan_threshold`, or the default value if `full_scan_threshold` is unset.
        pub fn full_scan_threshold(&self) -> u64 {
            match self.full_scan_threshold {
                ::core::option::Option::Some(val) => val,
                ::core::option::Option::None => 0u64,
            }
        }
    }
    pub struct WalConfigDiff {
        #[prost(uint64, optional, tag = "1")]
        pub wal_capacity_mb: ::core::option::Option<u64>,
        #[prost(uint64, optional, tag = "2")]
        pub wal_segments_ahead: ::core::option::Option<u64>,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for WalConfigDiff {
        #[inline]
        fn clone(&self) -> WalConfigDiff {
            match *self {
                WalConfigDiff {
                    wal_capacity_mb: ref __self_0_0,
                    wal_segments_ahead: ref __self_0_1,
                } => WalConfigDiff {
                    wal_capacity_mb: ::core::clone::Clone::clone(&(*__self_0_0)),
                    wal_segments_ahead: ::core::clone::Clone::clone(&(*__self_0_1)),
                },
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for WalConfigDiff {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for WalConfigDiff {
        #[inline]
        fn eq(&self, other: &WalConfigDiff) -> bool {
            match *other {
                WalConfigDiff {
                    wal_capacity_mb: ref __self_1_0,
                    wal_segments_ahead: ref __self_1_1,
                } => match *self {
                    WalConfigDiff {
                        wal_capacity_mb: ref __self_0_0,
                        wal_segments_ahead: ref __self_0_1,
                    } => (*__self_0_0) == (*__self_1_0) && (*__self_0_1) == (*__self_1_1),
                },
            }
        }
        #[inline]
        fn ne(&self, other: &WalConfigDiff) -> bool {
            match *other {
                WalConfigDiff {
                    wal_capacity_mb: ref __self_1_0,
                    wal_segments_ahead: ref __self_1_1,
                } => match *self {
                    WalConfigDiff {
                        wal_capacity_mb: ref __self_0_0,
                        wal_segments_ahead: ref __self_0_1,
                    } => (*__self_0_0) != (*__self_1_0) || (*__self_0_1) != (*__self_1_1),
                },
            }
        }
    }
    impl ::prost::Message for WalConfigDiff {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
            if let ::core::option::Option::Some(ref value) = self.wal_capacity_mb {
                ::prost::encoding::uint64::encode(1u32, value, buf);
            }
            if let ::core::option::Option::Some(ref value) = self.wal_segments_ahead {
                ::prost::encoding::uint64::encode(2u32, value, buf);
            }
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "WalConfigDiff";
            match tag {
                1u32 => {
                    let mut value = &mut self.wal_capacity_mb;
                    ::prost::encoding::uint64::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "wal_capacity_mb");
                        error
                    })
                }
                2u32 => {
                    let mut value = &mut self.wal_segments_ahead;
                    ::prost::encoding::uint64::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "wal_segments_ahead");
                        error
                    })
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0 + self.wal_capacity_mb.as_ref().map_or(0, |value| {
                ::prost::encoding::uint64::encoded_len(1u32, value)
            }) + self.wal_segments_ahead.as_ref().map_or(0, |value| {
                ::prost::encoding::uint64::encoded_len(2u32, value)
            })
        }
        fn clear(&mut self) {
            self.wal_capacity_mb = ::core::option::Option::None;
            self.wal_segments_ahead = ::core::option::Option::None;
        }
    }
    impl Default for WalConfigDiff {
        fn default() -> Self {
            WalConfigDiff {
                wal_capacity_mb: ::core::option::Option::None,
                wal_segments_ahead: ::core::option::Option::None,
            }
        }
    }
    impl ::core::fmt::Debug for WalConfigDiff {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("WalConfigDiff");
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(&'a ::core::option::Option<u64>);
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            fn Inner<T>(v: T) -> T {
                                v
                            }
                            ::core::fmt::Debug::fmt(&self.0.as_ref().map(Inner), f)
                        }
                    }
                    ScalarWrapper(&self.wal_capacity_mb)
                };
                builder.field("wal_capacity_mb", &wrapper)
            };
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(&'a ::core::option::Option<u64>);
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            fn Inner<T>(v: T) -> T {
                                v
                            }
                            ::core::fmt::Debug::fmt(&self.0.as_ref().map(Inner), f)
                        }
                    }
                    ScalarWrapper(&self.wal_segments_ahead)
                };
                builder.field("wal_segments_ahead", &wrapper)
            };
            builder.finish()
        }
    }
    #[allow(dead_code)]
    impl WalConfigDiff {
        ///Returns the value of `wal_capacity_mb`, or the default value if `wal_capacity_mb` is unset.
        pub fn wal_capacity_mb(&self) -> u64 {
            match self.wal_capacity_mb {
                ::core::option::Option::Some(val) => val,
                ::core::option::Option::None => 0u64,
            }
        }
        ///Returns the value of `wal_segments_ahead`, or the default value if `wal_segments_ahead` is unset.
        pub fn wal_segments_ahead(&self) -> u64 {
            match self.wal_segments_ahead {
                ::core::option::Option::Some(val) => val,
                ::core::option::Option::None => 0u64,
            }
        }
    }
    pub struct OptimizersConfigDiff {
        #[prost(double, optional, tag = "1")]
        pub deleted_threshold: ::core::option::Option<f64>,
        #[prost(uint64, optional, tag = "2")]
        pub vacuum_min_vector_number: ::core::option::Option<u64>,
        #[prost(uint64, optional, tag = "3")]
        pub max_segment_number: ::core::option::Option<u64>,
        #[prost(uint64, optional, tag = "4")]
        pub memmap_threshold: ::core::option::Option<u64>,
        #[prost(uint64, optional, tag = "5")]
        pub indexing_threshold: ::core::option::Option<u64>,
        #[prost(uint64, optional, tag = "6")]
        pub payload_indexing_threshold: ::core::option::Option<u64>,
        #[prost(uint64, optional, tag = "7")]
        pub flush_interval_sec: ::core::option::Option<u64>,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for OptimizersConfigDiff {
        #[inline]
        fn clone(&self) -> OptimizersConfigDiff {
            match *self {
                OptimizersConfigDiff {
                    deleted_threshold: ref __self_0_0,
                    vacuum_min_vector_number: ref __self_0_1,
                    max_segment_number: ref __self_0_2,
                    memmap_threshold: ref __self_0_3,
                    indexing_threshold: ref __self_0_4,
                    payload_indexing_threshold: ref __self_0_5,
                    flush_interval_sec: ref __self_0_6,
                } => OptimizersConfigDiff {
                    deleted_threshold: ::core::clone::Clone::clone(&(*__self_0_0)),
                    vacuum_min_vector_number: ::core::clone::Clone::clone(&(*__self_0_1)),
                    max_segment_number: ::core::clone::Clone::clone(&(*__self_0_2)),
                    memmap_threshold: ::core::clone::Clone::clone(&(*__self_0_3)),
                    indexing_threshold: ::core::clone::Clone::clone(&(*__self_0_4)),
                    payload_indexing_threshold: ::core::clone::Clone::clone(&(*__self_0_5)),
                    flush_interval_sec: ::core::clone::Clone::clone(&(*__self_0_6)),
                },
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for OptimizersConfigDiff {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for OptimizersConfigDiff {
        #[inline]
        fn eq(&self, other: &OptimizersConfigDiff) -> bool {
            match *other {
                OptimizersConfigDiff {
                    deleted_threshold: ref __self_1_0,
                    vacuum_min_vector_number: ref __self_1_1,
                    max_segment_number: ref __self_1_2,
                    memmap_threshold: ref __self_1_3,
                    indexing_threshold: ref __self_1_4,
                    payload_indexing_threshold: ref __self_1_5,
                    flush_interval_sec: ref __self_1_6,
                } => match *self {
                    OptimizersConfigDiff {
                        deleted_threshold: ref __self_0_0,
                        vacuum_min_vector_number: ref __self_0_1,
                        max_segment_number: ref __self_0_2,
                        memmap_threshold: ref __self_0_3,
                        indexing_threshold: ref __self_0_4,
                        payload_indexing_threshold: ref __self_0_5,
                        flush_interval_sec: ref __self_0_6,
                    } => {
                        (*__self_0_0) == (*__self_1_0)
                            && (*__self_0_1) == (*__self_1_1)
                            && (*__self_0_2) == (*__self_1_2)
                            && (*__self_0_3) == (*__self_1_3)
                            && (*__self_0_4) == (*__self_1_4)
                            && (*__self_0_5) == (*__self_1_5)
                            && (*__self_0_6) == (*__self_1_6)
                    }
                },
            }
        }
        #[inline]
        fn ne(&self, other: &OptimizersConfigDiff) -> bool {
            match *other {
                OptimizersConfigDiff {
                    deleted_threshold: ref __self_1_0,
                    vacuum_min_vector_number: ref __self_1_1,
                    max_segment_number: ref __self_1_2,
                    memmap_threshold: ref __self_1_3,
                    indexing_threshold: ref __self_1_4,
                    payload_indexing_threshold: ref __self_1_5,
                    flush_interval_sec: ref __self_1_6,
                } => match *self {
                    OptimizersConfigDiff {
                        deleted_threshold: ref __self_0_0,
                        vacuum_min_vector_number: ref __self_0_1,
                        max_segment_number: ref __self_0_2,
                        memmap_threshold: ref __self_0_3,
                        indexing_threshold: ref __self_0_4,
                        payload_indexing_threshold: ref __self_0_5,
                        flush_interval_sec: ref __self_0_6,
                    } => {
                        (*__self_0_0) != (*__self_1_0)
                            || (*__self_0_1) != (*__self_1_1)
                            || (*__self_0_2) != (*__self_1_2)
                            || (*__self_0_3) != (*__self_1_3)
                            || (*__self_0_4) != (*__self_1_4)
                            || (*__self_0_5) != (*__self_1_5)
                            || (*__self_0_6) != (*__self_1_6)
                    }
                },
            }
        }
    }
    impl ::prost::Message for OptimizersConfigDiff {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
            if let ::core::option::Option::Some(ref value) = self.deleted_threshold {
                ::prost::encoding::double::encode(1u32, value, buf);
            }
            if let ::core::option::Option::Some(ref value) = self.vacuum_min_vector_number {
                ::prost::encoding::uint64::encode(2u32, value, buf);
            }
            if let ::core::option::Option::Some(ref value) = self.max_segment_number {
                ::prost::encoding::uint64::encode(3u32, value, buf);
            }
            if let ::core::option::Option::Some(ref value) = self.memmap_threshold {
                ::prost::encoding::uint64::encode(4u32, value, buf);
            }
            if let ::core::option::Option::Some(ref value) = self.indexing_threshold {
                ::prost::encoding::uint64::encode(5u32, value, buf);
            }
            if let ::core::option::Option::Some(ref value) = self.payload_indexing_threshold {
                ::prost::encoding::uint64::encode(6u32, value, buf);
            }
            if let ::core::option::Option::Some(ref value) = self.flush_interval_sec {
                ::prost::encoding::uint64::encode(7u32, value, buf);
            }
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "OptimizersConfigDiff";
            match tag {
                1u32 => {
                    let mut value = &mut self.deleted_threshold;
                    ::prost::encoding::double::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "deleted_threshold");
                        error
                    })
                }
                2u32 => {
                    let mut value = &mut self.vacuum_min_vector_number;
                    ::prost::encoding::uint64::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "vacuum_min_vector_number");
                        error
                    })
                }
                3u32 => {
                    let mut value = &mut self.max_segment_number;
                    ::prost::encoding::uint64::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "max_segment_number");
                        error
                    })
                }
                4u32 => {
                    let mut value = &mut self.memmap_threshold;
                    ::prost::encoding::uint64::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "memmap_threshold");
                        error
                    })
                }
                5u32 => {
                    let mut value = &mut self.indexing_threshold;
                    ::prost::encoding::uint64::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "indexing_threshold");
                        error
                    })
                }
                6u32 => {
                    let mut value = &mut self.payload_indexing_threshold;
                    ::prost::encoding::uint64::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "payload_indexing_threshold");
                        error
                    })
                }
                7u32 => {
                    let mut value = &mut self.flush_interval_sec;
                    ::prost::encoding::uint64::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "flush_interval_sec");
                        error
                    })
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0 + self.deleted_threshold.as_ref().map_or(0, |value| {
                ::prost::encoding::double::encoded_len(1u32, value)
            }) + self.vacuum_min_vector_number.as_ref().map_or(0, |value| {
                ::prost::encoding::uint64::encoded_len(2u32, value)
            }) + self.max_segment_number.as_ref().map_or(0, |value| {
                ::prost::encoding::uint64::encoded_len(3u32, value)
            }) + self.memmap_threshold.as_ref().map_or(0, |value| {
                ::prost::encoding::uint64::encoded_len(4u32, value)
            }) + self.indexing_threshold.as_ref().map_or(0, |value| {
                ::prost::encoding::uint64::encoded_len(5u32, value)
            }) + self.payload_indexing_threshold.as_ref().map_or(0, |value| {
                ::prost::encoding::uint64::encoded_len(6u32, value)
            }) + self.flush_interval_sec.as_ref().map_or(0, |value| {
                ::prost::encoding::uint64::encoded_len(7u32, value)
            })
        }
        fn clear(&mut self) {
            self.deleted_threshold = ::core::option::Option::None;
            self.vacuum_min_vector_number = ::core::option::Option::None;
            self.max_segment_number = ::core::option::Option::None;
            self.memmap_threshold = ::core::option::Option::None;
            self.indexing_threshold = ::core::option::Option::None;
            self.payload_indexing_threshold = ::core::option::Option::None;
            self.flush_interval_sec = ::core::option::Option::None;
        }
    }
    impl Default for OptimizersConfigDiff {
        fn default() -> Self {
            OptimizersConfigDiff {
                deleted_threshold: ::core::option::Option::None,
                vacuum_min_vector_number: ::core::option::Option::None,
                max_segment_number: ::core::option::Option::None,
                memmap_threshold: ::core::option::Option::None,
                indexing_threshold: ::core::option::Option::None,
                payload_indexing_threshold: ::core::option::Option::None,
                flush_interval_sec: ::core::option::Option::None,
            }
        }
    }
    impl ::core::fmt::Debug for OptimizersConfigDiff {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("OptimizersConfigDiff");
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(&'a ::core::option::Option<f64>);
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            fn Inner<T>(v: T) -> T {
                                v
                            }
                            ::core::fmt::Debug::fmt(&self.0.as_ref().map(Inner), f)
                        }
                    }
                    ScalarWrapper(&self.deleted_threshold)
                };
                builder.field("deleted_threshold", &wrapper)
            };
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(&'a ::core::option::Option<u64>);
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            fn Inner<T>(v: T) -> T {
                                v
                            }
                            ::core::fmt::Debug::fmt(&self.0.as_ref().map(Inner), f)
                        }
                    }
                    ScalarWrapper(&self.vacuum_min_vector_number)
                };
                builder.field("vacuum_min_vector_number", &wrapper)
            };
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(&'a ::core::option::Option<u64>);
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            fn Inner<T>(v: T) -> T {
                                v
                            }
                            ::core::fmt::Debug::fmt(&self.0.as_ref().map(Inner), f)
                        }
                    }
                    ScalarWrapper(&self.max_segment_number)
                };
                builder.field("max_segment_number", &wrapper)
            };
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(&'a ::core::option::Option<u64>);
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            fn Inner<T>(v: T) -> T {
                                v
                            }
                            ::core::fmt::Debug::fmt(&self.0.as_ref().map(Inner), f)
                        }
                    }
                    ScalarWrapper(&self.memmap_threshold)
                };
                builder.field("memmap_threshold", &wrapper)
            };
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(&'a ::core::option::Option<u64>);
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            fn Inner<T>(v: T) -> T {
                                v
                            }
                            ::core::fmt::Debug::fmt(&self.0.as_ref().map(Inner), f)
                        }
                    }
                    ScalarWrapper(&self.indexing_threshold)
                };
                builder.field("indexing_threshold", &wrapper)
            };
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(&'a ::core::option::Option<u64>);
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            fn Inner<T>(v: T) -> T {
                                v
                            }
                            ::core::fmt::Debug::fmt(&self.0.as_ref().map(Inner), f)
                        }
                    }
                    ScalarWrapper(&self.payload_indexing_threshold)
                };
                builder.field("payload_indexing_threshold", &wrapper)
            };
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(&'a ::core::option::Option<u64>);
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            fn Inner<T>(v: T) -> T {
                                v
                            }
                            ::core::fmt::Debug::fmt(&self.0.as_ref().map(Inner), f)
                        }
                    }
                    ScalarWrapper(&self.flush_interval_sec)
                };
                builder.field("flush_interval_sec", &wrapper)
            };
            builder.finish()
        }
    }
    #[allow(dead_code)]
    impl OptimizersConfigDiff {
        ///Returns the value of `deleted_threshold`, or the default value if `deleted_threshold` is unset.
        pub fn deleted_threshold(&self) -> f64 {
            match self.deleted_threshold {
                ::core::option::Option::Some(val) => val,
                ::core::option::Option::None => 0f64,
            }
        }
        ///Returns the value of `vacuum_min_vector_number`, or the default value if `vacuum_min_vector_number` is unset.
        pub fn vacuum_min_vector_number(&self) -> u64 {
            match self.vacuum_min_vector_number {
                ::core::option::Option::Some(val) => val,
                ::core::option::Option::None => 0u64,
            }
        }
        ///Returns the value of `max_segment_number`, or the default value if `max_segment_number` is unset.
        pub fn max_segment_number(&self) -> u64 {
            match self.max_segment_number {
                ::core::option::Option::Some(val) => val,
                ::core::option::Option::None => 0u64,
            }
        }
        ///Returns the value of `memmap_threshold`, or the default value if `memmap_threshold` is unset.
        pub fn memmap_threshold(&self) -> u64 {
            match self.memmap_threshold {
                ::core::option::Option::Some(val) => val,
                ::core::option::Option::None => 0u64,
            }
        }
        ///Returns the value of `indexing_threshold`, or the default value if `indexing_threshold` is unset.
        pub fn indexing_threshold(&self) -> u64 {
            match self.indexing_threshold {
                ::core::option::Option::Some(val) => val,
                ::core::option::Option::None => 0u64,
            }
        }
        ///Returns the value of `payload_indexing_threshold`, or the default value if `payload_indexing_threshold` is unset.
        pub fn payload_indexing_threshold(&self) -> u64 {
            match self.payload_indexing_threshold {
                ::core::option::Option::Some(val) => val,
                ::core::option::Option::None => 0u64,
            }
        }
        ///Returns the value of `flush_interval_sec`, or the default value if `flush_interval_sec` is unset.
        pub fn flush_interval_sec(&self) -> u64 {
            match self.flush_interval_sec {
                ::core::option::Option::Some(val) => val,
                ::core::option::Option::None => 0u64,
            }
        }
    }
    pub struct CreateCollection {
        #[prost(string, tag = "1")]
        pub name: ::prost::alloc::string::String,
        #[prost(uint64, tag = "2")]
        pub vector_size: u64,
        #[prost(enumeration = "Distance", tag = "3")]
        pub distance: i32,
        #[prost(message, optional, tag = "4")]
        pub hnsw_config: ::core::option::Option<HnswConfigDiff>,
        #[prost(message, optional, tag = "5")]
        pub wal_config: ::core::option::Option<WalConfigDiff>,
        #[prost(message, optional, tag = "6")]
        pub optimizers_config: ::core::option::Option<OptimizersConfigDiff>,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for CreateCollection {
        #[inline]
        fn clone(&self) -> CreateCollection {
            match *self {
                CreateCollection {
                    name: ref __self_0_0,
                    vector_size: ref __self_0_1,
                    distance: ref __self_0_2,
                    hnsw_config: ref __self_0_3,
                    wal_config: ref __self_0_4,
                    optimizers_config: ref __self_0_5,
                } => CreateCollection {
                    name: ::core::clone::Clone::clone(&(*__self_0_0)),
                    vector_size: ::core::clone::Clone::clone(&(*__self_0_1)),
                    distance: ::core::clone::Clone::clone(&(*__self_0_2)),
                    hnsw_config: ::core::clone::Clone::clone(&(*__self_0_3)),
                    wal_config: ::core::clone::Clone::clone(&(*__self_0_4)),
                    optimizers_config: ::core::clone::Clone::clone(&(*__self_0_5)),
                },
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for CreateCollection {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for CreateCollection {
        #[inline]
        fn eq(&self, other: &CreateCollection) -> bool {
            match *other {
                CreateCollection {
                    name: ref __self_1_0,
                    vector_size: ref __self_1_1,
                    distance: ref __self_1_2,
                    hnsw_config: ref __self_1_3,
                    wal_config: ref __self_1_4,
                    optimizers_config: ref __self_1_5,
                } => match *self {
                    CreateCollection {
                        name: ref __self_0_0,
                        vector_size: ref __self_0_1,
                        distance: ref __self_0_2,
                        hnsw_config: ref __self_0_3,
                        wal_config: ref __self_0_4,
                        optimizers_config: ref __self_0_5,
                    } => {
                        (*__self_0_0) == (*__self_1_0)
                            && (*__self_0_1) == (*__self_1_1)
                            && (*__self_0_2) == (*__self_1_2)
                            && (*__self_0_3) == (*__self_1_3)
                            && (*__self_0_4) == (*__self_1_4)
                            && (*__self_0_5) == (*__self_1_5)
                    }
                },
            }
        }
        #[inline]
        fn ne(&self, other: &CreateCollection) -> bool {
            match *other {
                CreateCollection {
                    name: ref __self_1_0,
                    vector_size: ref __self_1_1,
                    distance: ref __self_1_2,
                    hnsw_config: ref __self_1_3,
                    wal_config: ref __self_1_4,
                    optimizers_config: ref __self_1_5,
                } => match *self {
                    CreateCollection {
                        name: ref __self_0_0,
                        vector_size: ref __self_0_1,
                        distance: ref __self_0_2,
                        hnsw_config: ref __self_0_3,
                        wal_config: ref __self_0_4,
                        optimizers_config: ref __self_0_5,
                    } => {
                        (*__self_0_0) != (*__self_1_0)
                            || (*__self_0_1) != (*__self_1_1)
                            || (*__self_0_2) != (*__self_1_2)
                            || (*__self_0_3) != (*__self_1_3)
                            || (*__self_0_4) != (*__self_1_4)
                            || (*__self_0_5) != (*__self_1_5)
                    }
                },
            }
        }
    }
    impl ::prost::Message for CreateCollection {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
            if self.name != "" {
                ::prost::encoding::string::encode(1u32, &self.name, buf);
            }
            if self.vector_size != 0u64 {
                ::prost::encoding::uint64::encode(2u32, &self.vector_size, buf);
            }
            if self.distance != Distance::default() as i32 {
                ::prost::encoding::int32::encode(3u32, &self.distance, buf);
            }
            if let Some(ref msg) = self.hnsw_config {
                ::prost::encoding::message::encode(4u32, msg, buf);
            }
            if let Some(ref msg) = self.wal_config {
                ::prost::encoding::message::encode(5u32, msg, buf);
            }
            if let Some(ref msg) = self.optimizers_config {
                ::prost::encoding::message::encode(6u32, msg, buf);
            }
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "CreateCollection";
            match tag {
                1u32 => {
                    let mut value = &mut self.name;
                    ::prost::encoding::string::merge(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "name");
                            error
                        },
                    )
                }
                2u32 => {
                    let mut value = &mut self.vector_size;
                    ::prost::encoding::uint64::merge(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "vector_size");
                            error
                        },
                    )
                }
                3u32 => {
                    let mut value = &mut self.distance;
                    ::prost::encoding::int32::merge(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "distance");
                            error
                        },
                    )
                }
                4u32 => {
                    let mut value = &mut self.hnsw_config;
                    ::prost::encoding::message::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "hnsw_config");
                        error
                    })
                }
                5u32 => {
                    let mut value = &mut self.wal_config;
                    ::prost::encoding::message::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "wal_config");
                        error
                    })
                }
                6u32 => {
                    let mut value = &mut self.optimizers_config;
                    ::prost::encoding::message::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "optimizers_config");
                        error
                    })
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0 + if self.name != "" {
                ::prost::encoding::string::encoded_len(1u32, &self.name)
            } else {
                0
            } + if self.vector_size != 0u64 {
                ::prost::encoding::uint64::encoded_len(2u32, &self.vector_size)
            } else {
                0
            } + if self.distance != Distance::default() as i32 {
                ::prost::encoding::int32::encoded_len(3u32, &self.distance)
            } else {
                0
            } + self
                .hnsw_config
                .as_ref()
                .map_or(0, |msg| ::prost::encoding::message::encoded_len(4u32, msg))
                + self
                    .wal_config
                    .as_ref()
                    .map_or(0, |msg| ::prost::encoding::message::encoded_len(5u32, msg))
                + self
                    .optimizers_config
                    .as_ref()
                    .map_or(0, |msg| ::prost::encoding::message::encoded_len(6u32, msg))
        }
        fn clear(&mut self) {
            self.name.clear();
            self.vector_size = 0u64;
            self.distance = Distance::default() as i32;
            self.hnsw_config = ::core::option::Option::None;
            self.wal_config = ::core::option::Option::None;
            self.optimizers_config = ::core::option::Option::None;
        }
    }
    impl Default for CreateCollection {
        fn default() -> Self {
            CreateCollection {
                name: ::prost::alloc::string::String::new(),
                vector_size: 0u64,
                distance: Distance::default() as i32,
                hnsw_config: ::core::default::Default::default(),
                wal_config: ::core::default::Default::default(),
                optimizers_config: ::core::default::Default::default(),
            }
        }
    }
    impl ::core::fmt::Debug for CreateCollection {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("CreateCollection");
            let builder = {
                let wrapper = {
                    fn ScalarWrapper<T>(v: T) -> T {
                        v
                    }
                    ScalarWrapper(&self.name)
                };
                builder.field("name", &wrapper)
            };
            let builder = {
                let wrapper = {
                    fn ScalarWrapper<T>(v: T) -> T {
                        v
                    }
                    ScalarWrapper(&self.vector_size)
                };
                builder.field("vector_size", &wrapper)
            };
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(&'a i32);
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            match Distance::from_i32(*self.0) {
                                None => ::core::fmt::Debug::fmt(&self.0, f),
                                Some(en) => ::core::fmt::Debug::fmt(&en, f),
                            }
                        }
                    }
                    ScalarWrapper(&self.distance)
                };
                builder.field("distance", &wrapper)
            };
            let builder = {
                let wrapper = &self.hnsw_config;
                builder.field("hnsw_config", &wrapper)
            };
            let builder = {
                let wrapper = &self.wal_config;
                builder.field("wal_config", &wrapper)
            };
            let builder = {
                let wrapper = &self.optimizers_config;
                builder.field("optimizers_config", &wrapper)
            };
            builder.finish()
        }
    }
    #[allow(dead_code)]
    impl CreateCollection {
        ///Returns the enum value of `distance`, or the default if the field is set to an invalid enum value.
        pub fn distance(&self) -> Distance {
            Distance::from_i32(self.distance).unwrap_or(Distance::default())
        }
        ///Sets `distance` to the provided enum value.
        pub fn set_distance(&mut self, value: Distance) {
            self.distance = value as i32;
        }
    }
    pub struct UpdateCollection {
        #[prost(string, tag = "1")]
        pub name: ::prost::alloc::string::String,
        #[prost(message, optional, tag = "2")]
        pub optimizers_config: ::core::option::Option<OptimizersConfigDiff>,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for UpdateCollection {
        #[inline]
        fn clone(&self) -> UpdateCollection {
            match *self {
                UpdateCollection {
                    name: ref __self_0_0,
                    optimizers_config: ref __self_0_1,
                } => UpdateCollection {
                    name: ::core::clone::Clone::clone(&(*__self_0_0)),
                    optimizers_config: ::core::clone::Clone::clone(&(*__self_0_1)),
                },
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for UpdateCollection {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for UpdateCollection {
        #[inline]
        fn eq(&self, other: &UpdateCollection) -> bool {
            match *other {
                UpdateCollection {
                    name: ref __self_1_0,
                    optimizers_config: ref __self_1_1,
                } => match *self {
                    UpdateCollection {
                        name: ref __self_0_0,
                        optimizers_config: ref __self_0_1,
                    } => (*__self_0_0) == (*__self_1_0) && (*__self_0_1) == (*__self_1_1),
                },
            }
        }
        #[inline]
        fn ne(&self, other: &UpdateCollection) -> bool {
            match *other {
                UpdateCollection {
                    name: ref __self_1_0,
                    optimizers_config: ref __self_1_1,
                } => match *self {
                    UpdateCollection {
                        name: ref __self_0_0,
                        optimizers_config: ref __self_0_1,
                    } => (*__self_0_0) != (*__self_1_0) || (*__self_0_1) != (*__self_1_1),
                },
            }
        }
    }
    impl ::prost::Message for UpdateCollection {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
            if self.name != "" {
                ::prost::encoding::string::encode(1u32, &self.name, buf);
            }
            if let Some(ref msg) = self.optimizers_config {
                ::prost::encoding::message::encode(2u32, msg, buf);
            }
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "UpdateCollection";
            match tag {
                1u32 => {
                    let mut value = &mut self.name;
                    ::prost::encoding::string::merge(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "name");
                            error
                        },
                    )
                }
                2u32 => {
                    let mut value = &mut self.optimizers_config;
                    ::prost::encoding::message::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "optimizers_config");
                        error
                    })
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0 + if self.name != "" {
                ::prost::encoding::string::encoded_len(1u32, &self.name)
            } else {
                0
            } + self
                .optimizers_config
                .as_ref()
                .map_or(0, |msg| ::prost::encoding::message::encoded_len(2u32, msg))
        }
        fn clear(&mut self) {
            self.name.clear();
            self.optimizers_config = ::core::option::Option::None;
        }
    }
    impl Default for UpdateCollection {
        fn default() -> Self {
            UpdateCollection {
                name: ::prost::alloc::string::String::new(),
                optimizers_config: ::core::default::Default::default(),
            }
        }
    }
    impl ::core::fmt::Debug for UpdateCollection {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("UpdateCollection");
            let builder = {
                let wrapper = {
                    fn ScalarWrapper<T>(v: T) -> T {
                        v
                    }
                    ScalarWrapper(&self.name)
                };
                builder.field("name", &wrapper)
            };
            let builder = {
                let wrapper = &self.optimizers_config;
                builder.field("optimizers_config", &wrapper)
            };
            builder.finish()
        }
    }
    pub struct DeleteCollection {
        #[prost(string, tag = "1")]
        pub name: ::prost::alloc::string::String,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for DeleteCollection {
        #[inline]
        fn clone(&self) -> DeleteCollection {
            match *self {
                DeleteCollection {
                    name: ref __self_0_0,
                } => DeleteCollection {
                    name: ::core::clone::Clone::clone(&(*__self_0_0)),
                },
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for DeleteCollection {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for DeleteCollection {
        #[inline]
        fn eq(&self, other: &DeleteCollection) -> bool {
            match *other {
                DeleteCollection {
                    name: ref __self_1_0,
                } => match *self {
                    DeleteCollection {
                        name: ref __self_0_0,
                    } => (*__self_0_0) == (*__self_1_0),
                },
            }
        }
        #[inline]
        fn ne(&self, other: &DeleteCollection) -> bool {
            match *other {
                DeleteCollection {
                    name: ref __self_1_0,
                } => match *self {
                    DeleteCollection {
                        name: ref __self_0_0,
                    } => (*__self_0_0) != (*__self_1_0),
                },
            }
        }
    }
    impl ::prost::Message for DeleteCollection {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
            if self.name != "" {
                ::prost::encoding::string::encode(1u32, &self.name, buf);
            }
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "DeleteCollection";
            match tag {
                1u32 => {
                    let mut value = &mut self.name;
                    ::prost::encoding::string::merge(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "name");
                            error
                        },
                    )
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0 + if self.name != "" {
                ::prost::encoding::string::encoded_len(1u32, &self.name)
            } else {
                0
            }
        }
        fn clear(&mut self) {
            self.name.clear();
        }
    }
    impl Default for DeleteCollection {
        fn default() -> Self {
            DeleteCollection {
                name: ::prost::alloc::string::String::new(),
            }
        }
    }
    impl ::core::fmt::Debug for DeleteCollection {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("DeleteCollection");
            let builder = {
                let wrapper = {
                    fn ScalarWrapper<T>(v: T) -> T {
                        v
                    }
                    ScalarWrapper(&self.name)
                };
                builder.field("name", &wrapper)
            };
            builder.finish()
        }
    }
    pub struct CollectionOperationResponse {
        #[prost(bool, optional, tag = "1")]
        pub result: ::core::option::Option<bool>,
        #[prost(string, optional, tag = "2")]
        pub error: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(double, tag = "3")]
        pub time: f64,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for CollectionOperationResponse {
        #[inline]
        fn clone(&self) -> CollectionOperationResponse {
            match *self {
                CollectionOperationResponse {
                    result: ref __self_0_0,
                    error: ref __self_0_1,
                    time: ref __self_0_2,
                } => CollectionOperationResponse {
                    result: ::core::clone::Clone::clone(&(*__self_0_0)),
                    error: ::core::clone::Clone::clone(&(*__self_0_1)),
                    time: ::core::clone::Clone::clone(&(*__self_0_2)),
                },
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for CollectionOperationResponse {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for CollectionOperationResponse {
        #[inline]
        fn eq(&self, other: &CollectionOperationResponse) -> bool {
            match *other {
                CollectionOperationResponse {
                    result: ref __self_1_0,
                    error: ref __self_1_1,
                    time: ref __self_1_2,
                } => match *self {
                    CollectionOperationResponse {
                        result: ref __self_0_0,
                        error: ref __self_0_1,
                        time: ref __self_0_2,
                    } => {
                        (*__self_0_0) == (*__self_1_0)
                            && (*__self_0_1) == (*__self_1_1)
                            && (*__self_0_2) == (*__self_1_2)
                    }
                },
            }
        }
        #[inline]
        fn ne(&self, other: &CollectionOperationResponse) -> bool {
            match *other {
                CollectionOperationResponse {
                    result: ref __self_1_0,
                    error: ref __self_1_1,
                    time: ref __self_1_2,
                } => match *self {
                    CollectionOperationResponse {
                        result: ref __self_0_0,
                        error: ref __self_0_1,
                        time: ref __self_0_2,
                    } => {
                        (*__self_0_0) != (*__self_1_0)
                            || (*__self_0_1) != (*__self_1_1)
                            || (*__self_0_2) != (*__self_1_2)
                    }
                },
            }
        }
    }
    impl ::prost::Message for CollectionOperationResponse {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
            if let ::core::option::Option::Some(ref value) = self.result {
                ::prost::encoding::bool::encode(1u32, value, buf);
            }
            if let ::core::option::Option::Some(ref value) = self.error {
                ::prost::encoding::string::encode(2u32, value, buf);
            }
            if self.time != 0f64 {
                ::prost::encoding::double::encode(3u32, &self.time, buf);
            }
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "CollectionOperationResponse";
            match tag {
                1u32 => {
                    let mut value = &mut self.result;
                    ::prost::encoding::bool::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "result");
                        error
                    })
                }
                2u32 => {
                    let mut value = &mut self.error;
                    ::prost::encoding::string::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "error");
                        error
                    })
                }
                3u32 => {
                    let mut value = &mut self.time;
                    ::prost::encoding::double::merge(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "time");
                            error
                        },
                    )
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0 + self
                .result
                .as_ref()
                .map_or(0, |value| ::prost::encoding::bool::encoded_len(1u32, value))
                + self.error.as_ref().map_or(0, |value| {
                    ::prost::encoding::string::encoded_len(2u32, value)
                })
                + if self.time != 0f64 {
                    ::prost::encoding::double::encoded_len(3u32, &self.time)
                } else {
                    0
                }
        }
        fn clear(&mut self) {
            self.result = ::core::option::Option::None;
            self.error = ::core::option::Option::None;
            self.time = 0f64;
        }
    }
    impl Default for CollectionOperationResponse {
        fn default() -> Self {
            CollectionOperationResponse {
                result: ::core::option::Option::None,
                error: ::core::option::Option::None,
                time: 0f64,
            }
        }
    }
    impl ::core::fmt::Debug for CollectionOperationResponse {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("CollectionOperationResponse");
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(&'a ::core::option::Option<bool>);
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            fn Inner<T>(v: T) -> T {
                                v
                            }
                            ::core::fmt::Debug::fmt(&self.0.as_ref().map(Inner), f)
                        }
                    }
                    ScalarWrapper(&self.result)
                };
                builder.field("result", &wrapper)
            };
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(
                        &'a ::core::option::Option<::prost::alloc::string::String>,
                    );
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            fn Inner<T>(v: T) -> T {
                                v
                            }
                            ::core::fmt::Debug::fmt(&self.0.as_ref().map(Inner), f)
                        }
                    }
                    ScalarWrapper(&self.error)
                };
                builder.field("error", &wrapper)
            };
            let builder = {
                let wrapper = {
                    fn ScalarWrapper<T>(v: T) -> T {
                        v
                    }
                    ScalarWrapper(&self.time)
                };
                builder.field("time", &wrapper)
            };
            builder.finish()
        }
    }
    #[allow(dead_code)]
    impl CollectionOperationResponse {
        ///Returns the value of `result`, or the default value if `result` is unset.
        pub fn result(&self) -> bool {
            match self.result {
                ::core::option::Option::Some(val) => val,
                ::core::option::Option::None => false,
            }
        }
        ///Returns the value of `error`, or the default value if `error` is unset.
        pub fn error(&self) -> &str {
            match self.error {
                ::core::option::Option::Some(ref val) => &val[..],
                ::core::option::Option::None => "",
            }
        }
    }
    #[repr(i32)]
    pub enum Distance {
        Cosine = 0,
        Euclid = 1,
        Dot = 2,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for Distance {
        #[inline]
        fn clone(&self) -> Distance {
            {
                *self
            }
        }
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::marker::Copy for Distance {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::fmt::Debug for Distance {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match (&*self,) {
                (&Distance::Cosine,) => {
                    let debug_trait_builder = &mut ::core::fmt::Formatter::debug_tuple(f, "Cosine");
                    ::core::fmt::DebugTuple::finish(debug_trait_builder)
                }
                (&Distance::Euclid,) => {
                    let debug_trait_builder = &mut ::core::fmt::Formatter::debug_tuple(f, "Euclid");
                    ::core::fmt::DebugTuple::finish(debug_trait_builder)
                }
                (&Distance::Dot,) => {
                    let debug_trait_builder = &mut ::core::fmt::Formatter::debug_tuple(f, "Dot");
                    ::core::fmt::DebugTuple::finish(debug_trait_builder)
                }
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for Distance {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for Distance {
        #[inline]
        fn eq(&self, other: &Distance) -> bool {
            {
                let __self_vi = ::core::intrinsics::discriminant_value(&*self);
                let __arg_1_vi = ::core::intrinsics::discriminant_value(&*other);
                if true && __self_vi == __arg_1_vi {
                    match (&*self, &*other) {
                        _ => true,
                    }
                } else {
                    false
                }
            }
        }
    }
    impl ::core::marker::StructuralEq for Distance {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::Eq for Distance {
        #[inline]
        #[doc(hidden)]
        #[no_coverage]
        fn assert_receiver_is_total_eq(&self) -> () {
            {}
        }
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::hash::Hash for Distance {
        fn hash<__H: ::core::hash::Hasher>(&self, state: &mut __H) -> () {
            match (&*self,) {
                _ => ::core::hash::Hash::hash(&::core::intrinsics::discriminant_value(self), state),
            }
        }
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialOrd for Distance {
        #[inline]
        fn partial_cmp(&self, other: &Distance) -> ::core::option::Option<::core::cmp::Ordering> {
            {
                let __self_vi = ::core::intrinsics::discriminant_value(&*self);
                let __arg_1_vi = ::core::intrinsics::discriminant_value(&*other);
                if true && __self_vi == __arg_1_vi {
                    match (&*self, &*other) {
                        _ => ::core::option::Option::Some(::core::cmp::Ordering::Equal),
                    }
                } else {
                    ::core::cmp::PartialOrd::partial_cmp(&__self_vi, &__arg_1_vi)
                }
            }
        }
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::Ord for Distance {
        #[inline]
        fn cmp(&self, other: &Distance) -> ::core::cmp::Ordering {
            {
                let __self_vi = ::core::intrinsics::discriminant_value(&*self);
                let __arg_1_vi = ::core::intrinsics::discriminant_value(&*other);
                if true && __self_vi == __arg_1_vi {
                    match (&*self, &*other) {
                        _ => ::core::cmp::Ordering::Equal,
                    }
                } else {
                    ::core::cmp::Ord::cmp(&__self_vi, &__arg_1_vi)
                }
            }
        }
    }
    impl Distance {
        ///Returns `true` if `value` is a variant of `Distance`.
        pub fn is_valid(value: i32) -> bool {
            match value {
                0 => true,
                1 => true,
                2 => true,
                _ => false,
            }
        }
        ///Converts an `i32` to a `Distance`, or `None` if `value` is not a valid variant.
        pub fn from_i32(value: i32) -> ::core::option::Option<Distance> {
            match value {
                0 => ::core::option::Option::Some(Distance::Cosine),
                1 => ::core::option::Option::Some(Distance::Euclid),
                2 => ::core::option::Option::Some(Distance::Dot),
                _ => ::core::option::Option::None,
            }
        }
    }
    impl ::core::default::Default for Distance {
        fn default() -> Distance {
            Distance::Cosine
        }
    }
    impl ::core::convert::From<Distance> for i32 {
        fn from(value: Distance) -> i32 {
            value as i32
        }
    }
    /// Generated client implementations.
    pub mod collections_client {
        #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
        use tonic::codegen::*;
        pub struct CollectionsClient<T> {
            inner: tonic::client::Grpc<T>,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl<T: ::core::fmt::Debug> ::core::fmt::Debug for CollectionsClient<T> {
            fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                match *self {
                    CollectionsClient {
                        inner: ref __self_0_0,
                    } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "CollectionsClient");
                        let _ = ::core::fmt::DebugStruct::field(
                            debug_trait_builder,
                            "inner",
                            &&(*__self_0_0),
                        );
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl<T: ::core::clone::Clone> ::core::clone::Clone for CollectionsClient<T> {
            #[inline]
            fn clone(&self) -> CollectionsClient<T> {
                match *self {
                    CollectionsClient {
                        inner: ref __self_0_0,
                    } => CollectionsClient {
                        inner: ::core::clone::Clone::clone(&(*__self_0_0)),
                    },
                }
            }
        }
        impl CollectionsClient<tonic::transport::Channel> {
            /// Attempt to create a new client by connecting to a given endpoint.
            pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
            where
                D: std::convert::TryInto<tonic::transport::Endpoint>,
                D::Error: Into<StdError>,
            {
                let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
                Ok(Self::new(conn))
            }
        }
        impl<T> CollectionsClient<T>
        where
            T: tonic::client::GrpcService<tonic::body::BoxBody>,
            T::ResponseBody: Body + Send + Sync + 'static,
            T::Error: Into<StdError>,
            <T::ResponseBody as Body>::Error: Into<StdError> + Send,
        {
            pub fn new(inner: T) -> Self {
                let inner = tonic::client::Grpc::new(inner);
                Self { inner }
            }
            pub fn with_interceptor<F>(
                inner: T,
                interceptor: F,
            ) -> CollectionsClient<InterceptedService<T, F>>
            where
                F: FnMut(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
                T: tonic::codegen::Service<
                    http::Request<tonic::body::BoxBody>,
                    Response = http::Response<
                        <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                    >,
                >,
                <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                    Into<StdError> + Send + Sync,
            {
                CollectionsClient::new(InterceptedService::new(inner, interceptor))
            }
            /// Compress requests with `gzip`.
            ///
            /// This requires the server to support it otherwise it might respond with an
            /// error.
            pub fn send_gzip(mut self) -> Self {
                self.inner = self.inner.send_gzip();
                self
            }
            /// Enable decompressing responses with `gzip`.
            pub fn accept_gzip(mut self) -> Self {
                self.inner = self.inner.accept_gzip();
                self
            }
            pub async fn get(
                &mut self,
                request: impl tonic::IntoRequest<super::GetCollectionsRequest>,
            ) -> Result<tonic::Response<super::GetCollectionsResponse>, tonic::Status> {
                self.inner.ready().await.map_err(|e| {
                    tonic::Status::new(tonic::Code::Unknown, {
                        let res = ::alloc::fmt::format(::core::fmt::Arguments::new_v1(
                            &["Service was not ready: "],
                            &match (&e.into(),) {
                                (arg0,) => [::core::fmt::ArgumentV1::new(
                                    arg0,
                                    ::core::fmt::Display::fmt,
                                )],
                            },
                        ));
                        res
                    })
                })?;
                let codec = tonic::codec::ProstCodec::default();
                let path = http::uri::PathAndQuery::from_static("/qdrant.Collections/Get");
                self.inner.unary(request.into_request(), path, codec).await
            }
            pub async fn create(
                &mut self,
                request: impl tonic::IntoRequest<super::CreateCollection>,
            ) -> Result<tonic::Response<super::CollectionOperationResponse>, tonic::Status>
            {
                self.inner.ready().await.map_err(|e| {
                    tonic::Status::new(tonic::Code::Unknown, {
                        let res = ::alloc::fmt::format(::core::fmt::Arguments::new_v1(
                            &["Service was not ready: "],
                            &match (&e.into(),) {
                                (arg0,) => [::core::fmt::ArgumentV1::new(
                                    arg0,
                                    ::core::fmt::Display::fmt,
                                )],
                            },
                        ));
                        res
                    })
                })?;
                let codec = tonic::codec::ProstCodec::default();
                let path = http::uri::PathAndQuery::from_static("/qdrant.Collections/Create");
                self.inner.unary(request.into_request(), path, codec).await
            }
            pub async fn update(
                &mut self,
                request: impl tonic::IntoRequest<super::UpdateCollection>,
            ) -> Result<tonic::Response<super::CollectionOperationResponse>, tonic::Status>
            {
                self.inner.ready().await.map_err(|e| {
                    tonic::Status::new(tonic::Code::Unknown, {
                        let res = ::alloc::fmt::format(::core::fmt::Arguments::new_v1(
                            &["Service was not ready: "],
                            &match (&e.into(),) {
                                (arg0,) => [::core::fmt::ArgumentV1::new(
                                    arg0,
                                    ::core::fmt::Display::fmt,
                                )],
                            },
                        ));
                        res
                    })
                })?;
                let codec = tonic::codec::ProstCodec::default();
                let path = http::uri::PathAndQuery::from_static("/qdrant.Collections/Update");
                self.inner.unary(request.into_request(), path, codec).await
            }
            pub async fn delete(
                &mut self,
                request: impl tonic::IntoRequest<super::DeleteCollection>,
            ) -> Result<tonic::Response<super::CollectionOperationResponse>, tonic::Status>
            {
                self.inner.ready().await.map_err(|e| {
                    tonic::Status::new(tonic::Code::Unknown, {
                        let res = ::alloc::fmt::format(::core::fmt::Arguments::new_v1(
                            &["Service was not ready: "],
                            &match (&e.into(),) {
                                (arg0,) => [::core::fmt::ArgumentV1::new(
                                    arg0,
                                    ::core::fmt::Display::fmt,
                                )],
                            },
                        ));
                        res
                    })
                })?;
                let codec = tonic::codec::ProstCodec::default();
                let path = http::uri::PathAndQuery::from_static("/qdrant.Collections/Delete");
                self.inner.unary(request.into_request(), path, codec).await
            }
        }
    }
    /// Generated server implementations.
    pub mod collections_server {
        #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
        use tonic::codegen::*;
        ///Generated trait containing gRPC methods that should be implemented for use with CollectionsServer.
        pub trait Collections: Send + Sync + 'static {
            #[must_use]
            #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
            fn get<'life0, 'async_trait>(
                &'life0 self,
                request: tonic::Request<super::GetCollectionsRequest>,
            ) -> ::core::pin::Pin<
                Box<
                    dyn ::core::future::Future<
                            Output = Result<
                                tonic::Response<super::GetCollectionsResponse>,
                                tonic::Status,
                            >,
                        > + ::core::marker::Send
                        + 'async_trait,
                >,
            >
            where
                'life0: 'async_trait,
                Self: 'async_trait;
            #[must_use]
            #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
            fn create<'life0, 'async_trait>(
                &'life0 self,
                request: tonic::Request<super::CreateCollection>,
            ) -> ::core::pin::Pin<
                Box<
                    dyn ::core::future::Future<
                            Output = Result<
                                tonic::Response<super::CollectionOperationResponse>,
                                tonic::Status,
                            >,
                        > + ::core::marker::Send
                        + 'async_trait,
                >,
            >
            where
                'life0: 'async_trait,
                Self: 'async_trait;
            #[must_use]
            #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
            fn update<'life0, 'async_trait>(
                &'life0 self,
                request: tonic::Request<super::UpdateCollection>,
            ) -> ::core::pin::Pin<
                Box<
                    dyn ::core::future::Future<
                            Output = Result<
                                tonic::Response<super::CollectionOperationResponse>,
                                tonic::Status,
                            >,
                        > + ::core::marker::Send
                        + 'async_trait,
                >,
            >
            where
                'life0: 'async_trait,
                Self: 'async_trait;
            #[must_use]
            #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
            fn delete<'life0, 'async_trait>(
                &'life0 self,
                request: tonic::Request<super::DeleteCollection>,
            ) -> ::core::pin::Pin<
                Box<
                    dyn ::core::future::Future<
                            Output = Result<
                                tonic::Response<super::CollectionOperationResponse>,
                                tonic::Status,
                            >,
                        > + ::core::marker::Send
                        + 'async_trait,
                >,
            >
            where
                'life0: 'async_trait,
                Self: 'async_trait;
        }
        pub struct CollectionsServer<T: Collections> {
            inner: _Inner<T>,
            accept_compression_encodings: (),
            send_compression_encodings: (),
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl<T: ::core::fmt::Debug + Collections> ::core::fmt::Debug for CollectionsServer<T> {
            fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                match *self {
                    CollectionsServer {
                        inner: ref __self_0_0,
                        accept_compression_encodings: ref __self_0_1,
                        send_compression_encodings: ref __self_0_2,
                    } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "CollectionsServer");
                        let _ = ::core::fmt::DebugStruct::field(
                            debug_trait_builder,
                            "inner",
                            &&(*__self_0_0),
                        );
                        let _ = ::core::fmt::DebugStruct::field(
                            debug_trait_builder,
                            "accept_compression_encodings",
                            &&(*__self_0_1),
                        );
                        let _ = ::core::fmt::DebugStruct::field(
                            debug_trait_builder,
                            "send_compression_encodings",
                            &&(*__self_0_2),
                        );
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        struct _Inner<T>(Arc<T>);
        impl<T: Collections> CollectionsServer<T> {
            pub fn new(inner: T) -> Self {
                let inner = Arc::new(inner);
                let inner = _Inner(inner);
                Self {
                    inner,
                    accept_compression_encodings: Default::default(),
                    send_compression_encodings: Default::default(),
                }
            }
            pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
            where
                F: FnMut(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
            {
                InterceptedService::new(Self::new(inner), interceptor)
            }
        }
        impl<T, B> tonic::codegen::Service<http::Request<B>> for CollectionsServer<T>
        where
            T: Collections,
            B: Body + Send + Sync + 'static,
            B::Error: Into<StdError> + Send + 'static,
        {
            type Response = http::Response<tonic::body::BoxBody>;
            type Error = Never;
            type Future = BoxFuture<Self::Response, Self::Error>;
            fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }
            fn call(&mut self, req: http::Request<B>) -> Self::Future {
                let inner = self.inner.clone();
                match req.uri().path() {
                    "/qdrant.Collections/Get" => {
                        #[allow(non_camel_case_types)]
                        struct GetSvc<T: Collections>(pub Arc<T>);
                        impl<T: Collections>
                            tonic::server::UnaryService<super::GetCollectionsRequest>
                            for GetSvc<T>
                        {
                            type Response = super::GetCollectionsResponse;
                            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                            fn call(
                                &mut self,
                                request: tonic::Request<super::GetCollectionsRequest>,
                            ) -> Self::Future {
                                let inner = self.0.clone();
                                let fut = async move { (*inner).get(request).await };
                                Box::pin(fut)
                            }
                        }
                        let accept_compression_encodings = self.accept_compression_encodings;
                        let send_compression_encodings = self.send_compression_encodings;
                        let inner = self.inner.clone();
                        let fut = async move {
                            let inner = inner.0;
                            let method = GetSvc(inner);
                            let codec = tonic::codec::ProstCodec::default();
                            let mut grpc = tonic::server::Grpc::new(codec)
                                .apply_compression_config(
                                    accept_compression_encodings,
                                    send_compression_encodings,
                                );
                            let res = grpc.unary(method, req).await;
                            Ok(res)
                        };
                        Box::pin(fut)
                    }
                    "/qdrant.Collections/Create" => {
                        #[allow(non_camel_case_types)]
                        struct CreateSvc<T: Collections>(pub Arc<T>);
                        impl<T: Collections> tonic::server::UnaryService<super::CreateCollection> for CreateSvc<T> {
                            type Response = super::CollectionOperationResponse;
                            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                            fn call(
                                &mut self,
                                request: tonic::Request<super::CreateCollection>,
                            ) -> Self::Future {
                                let inner = self.0.clone();
                                let fut = async move { (*inner).create(request).await };
                                Box::pin(fut)
                            }
                        }
                        let accept_compression_encodings = self.accept_compression_encodings;
                        let send_compression_encodings = self.send_compression_encodings;
                        let inner = self.inner.clone();
                        let fut = async move {
                            let inner = inner.0;
                            let method = CreateSvc(inner);
                            let codec = tonic::codec::ProstCodec::default();
                            let mut grpc = tonic::server::Grpc::new(codec)
                                .apply_compression_config(
                                    accept_compression_encodings,
                                    send_compression_encodings,
                                );
                            let res = grpc.unary(method, req).await;
                            Ok(res)
                        };
                        Box::pin(fut)
                    }
                    "/qdrant.Collections/Update" => {
                        #[allow(non_camel_case_types)]
                        struct UpdateSvc<T: Collections>(pub Arc<T>);
                        impl<T: Collections> tonic::server::UnaryService<super::UpdateCollection> for UpdateSvc<T> {
                            type Response = super::CollectionOperationResponse;
                            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                            fn call(
                                &mut self,
                                request: tonic::Request<super::UpdateCollection>,
                            ) -> Self::Future {
                                let inner = self.0.clone();
                                let fut = async move { (*inner).update(request).await };
                                Box::pin(fut)
                            }
                        }
                        let accept_compression_encodings = self.accept_compression_encodings;
                        let send_compression_encodings = self.send_compression_encodings;
                        let inner = self.inner.clone();
                        let fut = async move {
                            let inner = inner.0;
                            let method = UpdateSvc(inner);
                            let codec = tonic::codec::ProstCodec::default();
                            let mut grpc = tonic::server::Grpc::new(codec)
                                .apply_compression_config(
                                    accept_compression_encodings,
                                    send_compression_encodings,
                                );
                            let res = grpc.unary(method, req).await;
                            Ok(res)
                        };
                        Box::pin(fut)
                    }
                    "/qdrant.Collections/Delete" => {
                        #[allow(non_camel_case_types)]
                        struct DeleteSvc<T: Collections>(pub Arc<T>);
                        impl<T: Collections> tonic::server::UnaryService<super::DeleteCollection> for DeleteSvc<T> {
                            type Response = super::CollectionOperationResponse;
                            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                            fn call(
                                &mut self,
                                request: tonic::Request<super::DeleteCollection>,
                            ) -> Self::Future {
                                let inner = self.0.clone();
                                let fut = async move { (*inner).delete(request).await };
                                Box::pin(fut)
                            }
                        }
                        let accept_compression_encodings = self.accept_compression_encodings;
                        let send_compression_encodings = self.send_compression_encodings;
                        let inner = self.inner.clone();
                        let fut = async move {
                            let inner = inner.0;
                            let method = DeleteSvc(inner);
                            let codec = tonic::codec::ProstCodec::default();
                            let mut grpc = tonic::server::Grpc::new(codec)
                                .apply_compression_config(
                                    accept_compression_encodings,
                                    send_compression_encodings,
                                );
                            let res = grpc.unary(method, req).await;
                            Ok(res)
                        };
                        Box::pin(fut)
                    }
                    _ => Box::pin(async move {
                        Ok(http::Response::builder()
                            .status(200)
                            .header("grpc-status", "12")
                            .header("content-type", "application/grpc")
                            .body(empty_body())
                            .unwrap())
                    }),
                }
            }
        }
        impl<T: Collections> Clone for CollectionsServer<T> {
            fn clone(&self) -> Self {
                let inner = self.inner.clone();
                Self {
                    inner,
                    accept_compression_encodings: self.accept_compression_encodings,
                    send_compression_encodings: self.send_compression_encodings,
                }
            }
        }
        impl<T: Collections> Clone for _Inner<T> {
            fn clone(&self) -> Self {
                Self(self.0.clone())
            }
        }
        impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_fmt(::core::fmt::Arguments::new_v1(
                    &[""],
                    &match (&self.0,) {
                        (arg0,) => [::core::fmt::ArgumentV1::new(arg0, ::core::fmt::Debug::fmt)],
                    },
                ))
            }
        }
        impl<T: Collections> tonic::transport::NamedService for CollectionsServer<T> {
            const NAME: &'static str = "qdrant.Collections";
        }
    }
    pub struct UpsertPoints {
        #[prost(string, tag = "1")]
        pub collection: ::prost::alloc::string::String,
        #[prost(bool, optional, tag = "2")]
        pub wait: ::core::option::Option<bool>,
        #[prost(message, repeated, tag = "3")]
        pub points: ::prost::alloc::vec::Vec<PointStruct>,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for UpsertPoints {
        #[inline]
        fn clone(&self) -> UpsertPoints {
            match *self {
                UpsertPoints {
                    collection: ref __self_0_0,
                    wait: ref __self_0_1,
                    points: ref __self_0_2,
                } => UpsertPoints {
                    collection: ::core::clone::Clone::clone(&(*__self_0_0)),
                    wait: ::core::clone::Clone::clone(&(*__self_0_1)),
                    points: ::core::clone::Clone::clone(&(*__self_0_2)),
                },
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for UpsertPoints {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for UpsertPoints {
        #[inline]
        fn eq(&self, other: &UpsertPoints) -> bool {
            match *other {
                UpsertPoints {
                    collection: ref __self_1_0,
                    wait: ref __self_1_1,
                    points: ref __self_1_2,
                } => match *self {
                    UpsertPoints {
                        collection: ref __self_0_0,
                        wait: ref __self_0_1,
                        points: ref __self_0_2,
                    } => {
                        (*__self_0_0) == (*__self_1_0)
                            && (*__self_0_1) == (*__self_1_1)
                            && (*__self_0_2) == (*__self_1_2)
                    }
                },
            }
        }
        #[inline]
        fn ne(&self, other: &UpsertPoints) -> bool {
            match *other {
                UpsertPoints {
                    collection: ref __self_1_0,
                    wait: ref __self_1_1,
                    points: ref __self_1_2,
                } => match *self {
                    UpsertPoints {
                        collection: ref __self_0_0,
                        wait: ref __self_0_1,
                        points: ref __self_0_2,
                    } => {
                        (*__self_0_0) != (*__self_1_0)
                            || (*__self_0_1) != (*__self_1_1)
                            || (*__self_0_2) != (*__self_1_2)
                    }
                },
            }
        }
    }
    impl ::prost::Message for UpsertPoints {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
            if self.collection != "" {
                ::prost::encoding::string::encode(1u32, &self.collection, buf);
            }
            if let ::core::option::Option::Some(ref value) = self.wait {
                ::prost::encoding::bool::encode(2u32, value, buf);
            }
            for msg in &self.points {
                ::prost::encoding::message::encode(3u32, msg, buf);
            }
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "UpsertPoints";
            match tag {
                1u32 => {
                    let mut value = &mut self.collection;
                    ::prost::encoding::string::merge(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "collection");
                            error
                        },
                    )
                }
                2u32 => {
                    let mut value = &mut self.wait;
                    ::prost::encoding::bool::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "wait");
                        error
                    })
                }
                3u32 => {
                    let mut value = &mut self.points;
                    ::prost::encoding::message::merge_repeated(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "points");
                            error
                        },
                    )
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0 + if self.collection != "" {
                ::prost::encoding::string::encoded_len(1u32, &self.collection)
            } else {
                0
            } + self
                .wait
                .as_ref()
                .map_or(0, |value| ::prost::encoding::bool::encoded_len(2u32, value))
                + ::prost::encoding::message::encoded_len_repeated(3u32, &self.points)
        }
        fn clear(&mut self) {
            self.collection.clear();
            self.wait = ::core::option::Option::None;
            self.points.clear();
        }
    }
    impl Default for UpsertPoints {
        fn default() -> Self {
            UpsertPoints {
                collection: ::prost::alloc::string::String::new(),
                wait: ::core::option::Option::None,
                points: ::core::default::Default::default(),
            }
        }
    }
    impl ::core::fmt::Debug for UpsertPoints {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("UpsertPoints");
            let builder = {
                let wrapper = {
                    fn ScalarWrapper<T>(v: T) -> T {
                        v
                    }
                    ScalarWrapper(&self.collection)
                };
                builder.field("collection", &wrapper)
            };
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(&'a ::core::option::Option<bool>);
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            fn Inner<T>(v: T) -> T {
                                v
                            }
                            ::core::fmt::Debug::fmt(&self.0.as_ref().map(Inner), f)
                        }
                    }
                    ScalarWrapper(&self.wait)
                };
                builder.field("wait", &wrapper)
            };
            let builder = {
                let wrapper = &self.points;
                builder.field("points", &wrapper)
            };
            builder.finish()
        }
    }
    #[allow(dead_code)]
    impl UpsertPoints {
        ///Returns the value of `wait`, or the default value if `wait` is unset.
        pub fn wait(&self) -> bool {
            match self.wait {
                ::core::option::Option::Some(val) => val,
                ::core::option::Option::None => false,
            }
        }
    }
    pub struct PointStruct {
        #[prost(uint64, tag = "1")]
        pub id: u64,
        #[prost(float, repeated, tag = "2")]
        pub vector: ::prost::alloc::vec::Vec<f32>,
        #[prost(message, repeated, tag = "3")]
        pub payload: ::prost::alloc::vec::Vec<Payload>,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for PointStruct {
        #[inline]
        fn clone(&self) -> PointStruct {
            match *self {
                PointStruct {
                    id: ref __self_0_0,
                    vector: ref __self_0_1,
                    payload: ref __self_0_2,
                } => PointStruct {
                    id: ::core::clone::Clone::clone(&(*__self_0_0)),
                    vector: ::core::clone::Clone::clone(&(*__self_0_1)),
                    payload: ::core::clone::Clone::clone(&(*__self_0_2)),
                },
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for PointStruct {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for PointStruct {
        #[inline]
        fn eq(&self, other: &PointStruct) -> bool {
            match *other {
                PointStruct {
                    id: ref __self_1_0,
                    vector: ref __self_1_1,
                    payload: ref __self_1_2,
                } => match *self {
                    PointStruct {
                        id: ref __self_0_0,
                        vector: ref __self_0_1,
                        payload: ref __self_0_2,
                    } => {
                        (*__self_0_0) == (*__self_1_0)
                            && (*__self_0_1) == (*__self_1_1)
                            && (*__self_0_2) == (*__self_1_2)
                    }
                },
            }
        }
        #[inline]
        fn ne(&self, other: &PointStruct) -> bool {
            match *other {
                PointStruct {
                    id: ref __self_1_0,
                    vector: ref __self_1_1,
                    payload: ref __self_1_2,
                } => match *self {
                    PointStruct {
                        id: ref __self_0_0,
                        vector: ref __self_0_1,
                        payload: ref __self_0_2,
                    } => {
                        (*__self_0_0) != (*__self_1_0)
                            || (*__self_0_1) != (*__self_1_1)
                            || (*__self_0_2) != (*__self_1_2)
                    }
                },
            }
        }
    }
    impl ::prost::Message for PointStruct {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
            if self.id != 0u64 {
                ::prost::encoding::uint64::encode(1u32, &self.id, buf);
            }
            ::prost::encoding::float::encode_packed(2u32, &self.vector, buf);
            for msg in &self.payload {
                ::prost::encoding::message::encode(3u32, msg, buf);
            }
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "PointStruct";
            match tag {
                1u32 => {
                    let mut value = &mut self.id;
                    ::prost::encoding::uint64::merge(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "id");
                            error
                        },
                    )
                }
                2u32 => {
                    let mut value = &mut self.vector;
                    ::prost::encoding::float::merge_repeated(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "vector");
                            error
                        },
                    )
                }
                3u32 => {
                    let mut value = &mut self.payload;
                    ::prost::encoding::message::merge_repeated(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "payload");
                            error
                        },
                    )
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0 + if self.id != 0u64 {
                ::prost::encoding::uint64::encoded_len(1u32, &self.id)
            } else {
                0
            } + ::prost::encoding::float::encoded_len_packed(2u32, &self.vector)
                + ::prost::encoding::message::encoded_len_repeated(3u32, &self.payload)
        }
        fn clear(&mut self) {
            self.id = 0u64;
            self.vector.clear();
            self.payload.clear();
        }
    }
    impl Default for PointStruct {
        fn default() -> Self {
            PointStruct {
                id: 0u64,
                vector: ::prost::alloc::vec::Vec::new(),
                payload: ::core::default::Default::default(),
            }
        }
    }
    impl ::core::fmt::Debug for PointStruct {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("PointStruct");
            let builder = {
                let wrapper = {
                    fn ScalarWrapper<T>(v: T) -> T {
                        v
                    }
                    ScalarWrapper(&self.id)
                };
                builder.field("id", &wrapper)
            };
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(&'a ::prost::alloc::vec::Vec<f32>);
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            let mut vec_builder = f.debug_list();
                            for v in self.0 {
                                fn Inner<T>(v: T) -> T {
                                    v
                                }
                                vec_builder.entry(&Inner(v));
                            }
                            vec_builder.finish()
                        }
                    }
                    ScalarWrapper(&self.vector)
                };
                builder.field("vector", &wrapper)
            };
            let builder = {
                let wrapper = &self.payload;
                builder.field("payload", &wrapper)
            };
            builder.finish()
        }
    }
    pub struct Payload {
        #[prost(string, tag = "1")]
        pub key: ::prost::alloc::string::String,
        #[prost(message, optional, tag = "2")]
        pub value: ::core::option::Option<PayloadInterface>,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for Payload {
        #[inline]
        fn clone(&self) -> Payload {
            match *self {
                Payload {
                    key: ref __self_0_0,
                    value: ref __self_0_1,
                } => Payload {
                    key: ::core::clone::Clone::clone(&(*__self_0_0)),
                    value: ::core::clone::Clone::clone(&(*__self_0_1)),
                },
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for Payload {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for Payload {
        #[inline]
        fn eq(&self, other: &Payload) -> bool {
            match *other {
                Payload {
                    key: ref __self_1_0,
                    value: ref __self_1_1,
                } => match *self {
                    Payload {
                        key: ref __self_0_0,
                        value: ref __self_0_1,
                    } => (*__self_0_0) == (*__self_1_0) && (*__self_0_1) == (*__self_1_1),
                },
            }
        }
        #[inline]
        fn ne(&self, other: &Payload) -> bool {
            match *other {
                Payload {
                    key: ref __self_1_0,
                    value: ref __self_1_1,
                } => match *self {
                    Payload {
                        key: ref __self_0_0,
                        value: ref __self_0_1,
                    } => (*__self_0_0) != (*__self_1_0) || (*__self_0_1) != (*__self_1_1),
                },
            }
        }
    }
    impl ::prost::Message for Payload {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
            if self.key != "" {
                ::prost::encoding::string::encode(1u32, &self.key, buf);
            }
            if let Some(ref msg) = self.value {
                ::prost::encoding::message::encode(2u32, msg, buf);
            }
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "Payload";
            match tag {
                1u32 => {
                    let mut value = &mut self.key;
                    ::prost::encoding::string::merge(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "key");
                            error
                        },
                    )
                }
                2u32 => {
                    let mut value = &mut self.value;
                    ::prost::encoding::message::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "value");
                        error
                    })
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0 + if self.key != "" {
                ::prost::encoding::string::encoded_len(1u32, &self.key)
            } else {
                0
            } + self
                .value
                .as_ref()
                .map_or(0, |msg| ::prost::encoding::message::encoded_len(2u32, msg))
        }
        fn clear(&mut self) {
            self.key.clear();
            self.value = ::core::option::Option::None;
        }
    }
    impl Default for Payload {
        fn default() -> Self {
            Payload {
                key: ::prost::alloc::string::String::new(),
                value: ::core::default::Default::default(),
            }
        }
    }
    impl ::core::fmt::Debug for Payload {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("Payload");
            let builder = {
                let wrapper = {
                    fn ScalarWrapper<T>(v: T) -> T {
                        v
                    }
                    ScalarWrapper(&self.key)
                };
                builder.field("key", &wrapper)
            };
            let builder = {
                let wrapper = &self.value;
                builder.field("value", &wrapper)
            };
            builder.finish()
        }
    }
    pub struct PayloadInterface {
        #[prost(message, optional, tag = "1")]
        pub keyword: ::core::option::Option<KeywordPayload>,
        #[prost(message, optional, tag = "2")]
        pub integer: ::core::option::Option<IntegerPayload>,
        #[prost(message, optional, tag = "3")]
        pub float: ::core::option::Option<FloatPayload>,
        #[prost(message, optional, tag = "4")]
        pub geo: ::core::option::Option<GeoPayload>,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for PayloadInterface {
        #[inline]
        fn clone(&self) -> PayloadInterface {
            match *self {
                PayloadInterface {
                    keyword: ref __self_0_0,
                    integer: ref __self_0_1,
                    float: ref __self_0_2,
                    geo: ref __self_0_3,
                } => PayloadInterface {
                    keyword: ::core::clone::Clone::clone(&(*__self_0_0)),
                    integer: ::core::clone::Clone::clone(&(*__self_0_1)),
                    float: ::core::clone::Clone::clone(&(*__self_0_2)),
                    geo: ::core::clone::Clone::clone(&(*__self_0_3)),
                },
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for PayloadInterface {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for PayloadInterface {
        #[inline]
        fn eq(&self, other: &PayloadInterface) -> bool {
            match *other {
                PayloadInterface {
                    keyword: ref __self_1_0,
                    integer: ref __self_1_1,
                    float: ref __self_1_2,
                    geo: ref __self_1_3,
                } => match *self {
                    PayloadInterface {
                        keyword: ref __self_0_0,
                        integer: ref __self_0_1,
                        float: ref __self_0_2,
                        geo: ref __self_0_3,
                    } => {
                        (*__self_0_0) == (*__self_1_0)
                            && (*__self_0_1) == (*__self_1_1)
                            && (*__self_0_2) == (*__self_1_2)
                            && (*__self_0_3) == (*__self_1_3)
                    }
                },
            }
        }
        #[inline]
        fn ne(&self, other: &PayloadInterface) -> bool {
            match *other {
                PayloadInterface {
                    keyword: ref __self_1_0,
                    integer: ref __self_1_1,
                    float: ref __self_1_2,
                    geo: ref __self_1_3,
                } => match *self {
                    PayloadInterface {
                        keyword: ref __self_0_0,
                        integer: ref __self_0_1,
                        float: ref __self_0_2,
                        geo: ref __self_0_3,
                    } => {
                        (*__self_0_0) != (*__self_1_0)
                            || (*__self_0_1) != (*__self_1_1)
                            || (*__self_0_2) != (*__self_1_2)
                            || (*__self_0_3) != (*__self_1_3)
                    }
                },
            }
        }
    }
    impl ::prost::Message for PayloadInterface {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
            if let Some(ref msg) = self.keyword {
                ::prost::encoding::message::encode(1u32, msg, buf);
            }
            if let Some(ref msg) = self.integer {
                ::prost::encoding::message::encode(2u32, msg, buf);
            }
            if let Some(ref msg) = self.float {
                ::prost::encoding::message::encode(3u32, msg, buf);
            }
            if let Some(ref msg) = self.geo {
                ::prost::encoding::message::encode(4u32, msg, buf);
            }
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "PayloadInterface";
            match tag {
                1u32 => {
                    let mut value = &mut self.keyword;
                    ::prost::encoding::message::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "keyword");
                        error
                    })
                }
                2u32 => {
                    let mut value = &mut self.integer;
                    ::prost::encoding::message::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "integer");
                        error
                    })
                }
                3u32 => {
                    let mut value = &mut self.float;
                    ::prost::encoding::message::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "float");
                        error
                    })
                }
                4u32 => {
                    let mut value = &mut self.geo;
                    ::prost::encoding::message::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "geo");
                        error
                    })
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0 + self
                .keyword
                .as_ref()
                .map_or(0, |msg| ::prost::encoding::message::encoded_len(1u32, msg))
                + self
                    .integer
                    .as_ref()
                    .map_or(0, |msg| ::prost::encoding::message::encoded_len(2u32, msg))
                + self
                    .float
                    .as_ref()
                    .map_or(0, |msg| ::prost::encoding::message::encoded_len(3u32, msg))
                + self
                    .geo
                    .as_ref()
                    .map_or(0, |msg| ::prost::encoding::message::encoded_len(4u32, msg))
        }
        fn clear(&mut self) {
            self.keyword = ::core::option::Option::None;
            self.integer = ::core::option::Option::None;
            self.float = ::core::option::Option::None;
            self.geo = ::core::option::Option::None;
        }
    }
    impl Default for PayloadInterface {
        fn default() -> Self {
            PayloadInterface {
                keyword: ::core::default::Default::default(),
                integer: ::core::default::Default::default(),
                float: ::core::default::Default::default(),
                geo: ::core::default::Default::default(),
            }
        }
    }
    impl ::core::fmt::Debug for PayloadInterface {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("PayloadInterface");
            let builder = {
                let wrapper = &self.keyword;
                builder.field("keyword", &wrapper)
            };
            let builder = {
                let wrapper = &self.integer;
                builder.field("integer", &wrapper)
            };
            let builder = {
                let wrapper = &self.float;
                builder.field("float", &wrapper)
            };
            let builder = {
                let wrapper = &self.geo;
                builder.field("geo", &wrapper)
            };
            builder.finish()
        }
    }
    pub struct KeywordPayload {
        #[prost(string, repeated, tag = "1")]
        pub list: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        #[prost(string, optional, tag = "2")]
        pub value: ::core::option::Option<::prost::alloc::string::String>,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for KeywordPayload {
        #[inline]
        fn clone(&self) -> KeywordPayload {
            match *self {
                KeywordPayload {
                    list: ref __self_0_0,
                    value: ref __self_0_1,
                } => KeywordPayload {
                    list: ::core::clone::Clone::clone(&(*__self_0_0)),
                    value: ::core::clone::Clone::clone(&(*__self_0_1)),
                },
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for KeywordPayload {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for KeywordPayload {
        #[inline]
        fn eq(&self, other: &KeywordPayload) -> bool {
            match *other {
                KeywordPayload {
                    list: ref __self_1_0,
                    value: ref __self_1_1,
                } => match *self {
                    KeywordPayload {
                        list: ref __self_0_0,
                        value: ref __self_0_1,
                    } => (*__self_0_0) == (*__self_1_0) && (*__self_0_1) == (*__self_1_1),
                },
            }
        }
        #[inline]
        fn ne(&self, other: &KeywordPayload) -> bool {
            match *other {
                KeywordPayload {
                    list: ref __self_1_0,
                    value: ref __self_1_1,
                } => match *self {
                    KeywordPayload {
                        list: ref __self_0_0,
                        value: ref __self_0_1,
                    } => (*__self_0_0) != (*__self_1_0) || (*__self_0_1) != (*__self_1_1),
                },
            }
        }
    }
    impl ::prost::Message for KeywordPayload {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
            ::prost::encoding::string::encode_repeated(1u32, &self.list, buf);
            if let ::core::option::Option::Some(ref value) = self.value {
                ::prost::encoding::string::encode(2u32, value, buf);
            }
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "KeywordPayload";
            match tag {
                1u32 => {
                    let mut value = &mut self.list;
                    ::prost::encoding::string::merge_repeated(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "list");
                            error
                        },
                    )
                }
                2u32 => {
                    let mut value = &mut self.value;
                    ::prost::encoding::string::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "value");
                        error
                    })
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0 + ::prost::encoding::string::encoded_len_repeated(1u32, &self.list)
                + self.value.as_ref().map_or(0, |value| {
                    ::prost::encoding::string::encoded_len(2u32, value)
                })
        }
        fn clear(&mut self) {
            self.list.clear();
            self.value = ::core::option::Option::None;
        }
    }
    impl Default for KeywordPayload {
        fn default() -> Self {
            KeywordPayload {
                list: ::prost::alloc::vec::Vec::new(),
                value: ::core::option::Option::None,
            }
        }
    }
    impl ::core::fmt::Debug for KeywordPayload {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("KeywordPayload");
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(
                        &'a ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
                    );
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            let mut vec_builder = f.debug_list();
                            for v in self.0 {
                                fn Inner<T>(v: T) -> T {
                                    v
                                }
                                vec_builder.entry(&Inner(v));
                            }
                            vec_builder.finish()
                        }
                    }
                    ScalarWrapper(&self.list)
                };
                builder.field("list", &wrapper)
            };
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(
                        &'a ::core::option::Option<::prost::alloc::string::String>,
                    );
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            fn Inner<T>(v: T) -> T {
                                v
                            }
                            ::core::fmt::Debug::fmt(&self.0.as_ref().map(Inner), f)
                        }
                    }
                    ScalarWrapper(&self.value)
                };
                builder.field("value", &wrapper)
            };
            builder.finish()
        }
    }
    #[allow(dead_code)]
    impl KeywordPayload {
        ///Returns the value of `value`, or the default value if `value` is unset.
        pub fn value(&self) -> &str {
            match self.value {
                ::core::option::Option::Some(ref val) => &val[..],
                ::core::option::Option::None => "",
            }
        }
    }
    pub struct IntegerPayload {
        #[prost(int64, repeated, tag = "1")]
        pub list: ::prost::alloc::vec::Vec<i64>,
        #[prost(int64, optional, tag = "2")]
        pub value: ::core::option::Option<i64>,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for IntegerPayload {
        #[inline]
        fn clone(&self) -> IntegerPayload {
            match *self {
                IntegerPayload {
                    list: ref __self_0_0,
                    value: ref __self_0_1,
                } => IntegerPayload {
                    list: ::core::clone::Clone::clone(&(*__self_0_0)),
                    value: ::core::clone::Clone::clone(&(*__self_0_1)),
                },
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for IntegerPayload {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for IntegerPayload {
        #[inline]
        fn eq(&self, other: &IntegerPayload) -> bool {
            match *other {
                IntegerPayload {
                    list: ref __self_1_0,
                    value: ref __self_1_1,
                } => match *self {
                    IntegerPayload {
                        list: ref __self_0_0,
                        value: ref __self_0_1,
                    } => (*__self_0_0) == (*__self_1_0) && (*__self_0_1) == (*__self_1_1),
                },
            }
        }
        #[inline]
        fn ne(&self, other: &IntegerPayload) -> bool {
            match *other {
                IntegerPayload {
                    list: ref __self_1_0,
                    value: ref __self_1_1,
                } => match *self {
                    IntegerPayload {
                        list: ref __self_0_0,
                        value: ref __self_0_1,
                    } => (*__self_0_0) != (*__self_1_0) || (*__self_0_1) != (*__self_1_1),
                },
            }
        }
    }
    impl ::prost::Message for IntegerPayload {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
            ::prost::encoding::int64::encode_packed(1u32, &self.list, buf);
            if let ::core::option::Option::Some(ref value) = self.value {
                ::prost::encoding::int64::encode(2u32, value, buf);
            }
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "IntegerPayload";
            match tag {
                1u32 => {
                    let mut value = &mut self.list;
                    ::prost::encoding::int64::merge_repeated(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "list");
                            error
                        },
                    )
                }
                2u32 => {
                    let mut value = &mut self.value;
                    ::prost::encoding::int64::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "value");
                        error
                    })
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0 + ::prost::encoding::int64::encoded_len_packed(1u32, &self.list)
                + self.value.as_ref().map_or(0, |value| {
                    ::prost::encoding::int64::encoded_len(2u32, value)
                })
        }
        fn clear(&mut self) {
            self.list.clear();
            self.value = ::core::option::Option::None;
        }
    }
    impl Default for IntegerPayload {
        fn default() -> Self {
            IntegerPayload {
                list: ::prost::alloc::vec::Vec::new(),
                value: ::core::option::Option::None,
            }
        }
    }
    impl ::core::fmt::Debug for IntegerPayload {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("IntegerPayload");
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(&'a ::prost::alloc::vec::Vec<i64>);
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            let mut vec_builder = f.debug_list();
                            for v in self.0 {
                                fn Inner<T>(v: T) -> T {
                                    v
                                }
                                vec_builder.entry(&Inner(v));
                            }
                            vec_builder.finish()
                        }
                    }
                    ScalarWrapper(&self.list)
                };
                builder.field("list", &wrapper)
            };
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(&'a ::core::option::Option<i64>);
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            fn Inner<T>(v: T) -> T {
                                v
                            }
                            ::core::fmt::Debug::fmt(&self.0.as_ref().map(Inner), f)
                        }
                    }
                    ScalarWrapper(&self.value)
                };
                builder.field("value", &wrapper)
            };
            builder.finish()
        }
    }
    #[allow(dead_code)]
    impl IntegerPayload {
        ///Returns the value of `value`, or the default value if `value` is unset.
        pub fn value(&self) -> i64 {
            match self.value {
                ::core::option::Option::Some(val) => val,
                ::core::option::Option::None => 0i64,
            }
        }
    }
    pub struct FloatPayload {
        #[prost(double, repeated, tag = "1")]
        pub list: ::prost::alloc::vec::Vec<f64>,
        #[prost(double, optional, tag = "2")]
        pub value: ::core::option::Option<f64>,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for FloatPayload {
        #[inline]
        fn clone(&self) -> FloatPayload {
            match *self {
                FloatPayload {
                    list: ref __self_0_0,
                    value: ref __self_0_1,
                } => FloatPayload {
                    list: ::core::clone::Clone::clone(&(*__self_0_0)),
                    value: ::core::clone::Clone::clone(&(*__self_0_1)),
                },
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for FloatPayload {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for FloatPayload {
        #[inline]
        fn eq(&self, other: &FloatPayload) -> bool {
            match *other {
                FloatPayload {
                    list: ref __self_1_0,
                    value: ref __self_1_1,
                } => match *self {
                    FloatPayload {
                        list: ref __self_0_0,
                        value: ref __self_0_1,
                    } => (*__self_0_0) == (*__self_1_0) && (*__self_0_1) == (*__self_1_1),
                },
            }
        }
        #[inline]
        fn ne(&self, other: &FloatPayload) -> bool {
            match *other {
                FloatPayload {
                    list: ref __self_1_0,
                    value: ref __self_1_1,
                } => match *self {
                    FloatPayload {
                        list: ref __self_0_0,
                        value: ref __self_0_1,
                    } => (*__self_0_0) != (*__self_1_0) || (*__self_0_1) != (*__self_1_1),
                },
            }
        }
    }
    impl ::prost::Message for FloatPayload {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
            ::prost::encoding::double::encode_packed(1u32, &self.list, buf);
            if let ::core::option::Option::Some(ref value) = self.value {
                ::prost::encoding::double::encode(2u32, value, buf);
            }
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "FloatPayload";
            match tag {
                1u32 => {
                    let mut value = &mut self.list;
                    ::prost::encoding::double::merge_repeated(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "list");
                            error
                        },
                    )
                }
                2u32 => {
                    let mut value = &mut self.value;
                    ::prost::encoding::double::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "value");
                        error
                    })
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0 + ::prost::encoding::double::encoded_len_packed(1u32, &self.list)
                + self.value.as_ref().map_or(0, |value| {
                    ::prost::encoding::double::encoded_len(2u32, value)
                })
        }
        fn clear(&mut self) {
            self.list.clear();
            self.value = ::core::option::Option::None;
        }
    }
    impl Default for FloatPayload {
        fn default() -> Self {
            FloatPayload {
                list: ::prost::alloc::vec::Vec::new(),
                value: ::core::option::Option::None,
            }
        }
    }
    impl ::core::fmt::Debug for FloatPayload {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("FloatPayload");
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(&'a ::prost::alloc::vec::Vec<f64>);
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            let mut vec_builder = f.debug_list();
                            for v in self.0 {
                                fn Inner<T>(v: T) -> T {
                                    v
                                }
                                vec_builder.entry(&Inner(v));
                            }
                            vec_builder.finish()
                        }
                    }
                    ScalarWrapper(&self.list)
                };
                builder.field("list", &wrapper)
            };
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(&'a ::core::option::Option<f64>);
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            fn Inner<T>(v: T) -> T {
                                v
                            }
                            ::core::fmt::Debug::fmt(&self.0.as_ref().map(Inner), f)
                        }
                    }
                    ScalarWrapper(&self.value)
                };
                builder.field("value", &wrapper)
            };
            builder.finish()
        }
    }
    #[allow(dead_code)]
    impl FloatPayload {
        ///Returns the value of `value`, or the default value if `value` is unset.
        pub fn value(&self) -> f64 {
            match self.value {
                ::core::option::Option::Some(val) => val,
                ::core::option::Option::None => 0f64,
            }
        }
    }
    pub struct GeoPayload {
        #[prost(message, repeated, tag = "1")]
        pub list: ::prost::alloc::vec::Vec<GeoPoint>,
        #[prost(message, optional, tag = "2")]
        pub value: ::core::option::Option<GeoPoint>,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for GeoPayload {
        #[inline]
        fn clone(&self) -> GeoPayload {
            match *self {
                GeoPayload {
                    list: ref __self_0_0,
                    value: ref __self_0_1,
                } => GeoPayload {
                    list: ::core::clone::Clone::clone(&(*__self_0_0)),
                    value: ::core::clone::Clone::clone(&(*__self_0_1)),
                },
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for GeoPayload {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for GeoPayload {
        #[inline]
        fn eq(&self, other: &GeoPayload) -> bool {
            match *other {
                GeoPayload {
                    list: ref __self_1_0,
                    value: ref __self_1_1,
                } => match *self {
                    GeoPayload {
                        list: ref __self_0_0,
                        value: ref __self_0_1,
                    } => (*__self_0_0) == (*__self_1_0) && (*__self_0_1) == (*__self_1_1),
                },
            }
        }
        #[inline]
        fn ne(&self, other: &GeoPayload) -> bool {
            match *other {
                GeoPayload {
                    list: ref __self_1_0,
                    value: ref __self_1_1,
                } => match *self {
                    GeoPayload {
                        list: ref __self_0_0,
                        value: ref __self_0_1,
                    } => (*__self_0_0) != (*__self_1_0) || (*__self_0_1) != (*__self_1_1),
                },
            }
        }
    }
    impl ::prost::Message for GeoPayload {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
            for msg in &self.list {
                ::prost::encoding::message::encode(1u32, msg, buf);
            }
            if let Some(ref msg) = self.value {
                ::prost::encoding::message::encode(2u32, msg, buf);
            }
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "GeoPayload";
            match tag {
                1u32 => {
                    let mut value = &mut self.list;
                    ::prost::encoding::message::merge_repeated(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "list");
                            error
                        },
                    )
                }
                2u32 => {
                    let mut value = &mut self.value;
                    ::prost::encoding::message::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "value");
                        error
                    })
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0 + ::prost::encoding::message::encoded_len_repeated(1u32, &self.list)
                + self
                    .value
                    .as_ref()
                    .map_or(0, |msg| ::prost::encoding::message::encoded_len(2u32, msg))
        }
        fn clear(&mut self) {
            self.list.clear();
            self.value = ::core::option::Option::None;
        }
    }
    impl Default for GeoPayload {
        fn default() -> Self {
            GeoPayload {
                list: ::core::default::Default::default(),
                value: ::core::default::Default::default(),
            }
        }
    }
    impl ::core::fmt::Debug for GeoPayload {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("GeoPayload");
            let builder = {
                let wrapper = &self.list;
                builder.field("list", &wrapper)
            };
            let builder = {
                let wrapper = &self.value;
                builder.field("value", &wrapper)
            };
            builder.finish()
        }
    }
    pub struct GeoPoint {
        #[prost(double, tag = "1")]
        pub lon: f64,
        #[prost(double, tag = "2")]
        pub lat: f64,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for GeoPoint {
        #[inline]
        fn clone(&self) -> GeoPoint {
            match *self {
                GeoPoint {
                    lon: ref __self_0_0,
                    lat: ref __self_0_1,
                } => GeoPoint {
                    lon: ::core::clone::Clone::clone(&(*__self_0_0)),
                    lat: ::core::clone::Clone::clone(&(*__self_0_1)),
                },
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for GeoPoint {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for GeoPoint {
        #[inline]
        fn eq(&self, other: &GeoPoint) -> bool {
            match *other {
                GeoPoint {
                    lon: ref __self_1_0,
                    lat: ref __self_1_1,
                } => match *self {
                    GeoPoint {
                        lon: ref __self_0_0,
                        lat: ref __self_0_1,
                    } => (*__self_0_0) == (*__self_1_0) && (*__self_0_1) == (*__self_1_1),
                },
            }
        }
        #[inline]
        fn ne(&self, other: &GeoPoint) -> bool {
            match *other {
                GeoPoint {
                    lon: ref __self_1_0,
                    lat: ref __self_1_1,
                } => match *self {
                    GeoPoint {
                        lon: ref __self_0_0,
                        lat: ref __self_0_1,
                    } => (*__self_0_0) != (*__self_1_0) || (*__self_0_1) != (*__self_1_1),
                },
            }
        }
    }
    impl ::prost::Message for GeoPoint {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
            if self.lon != 0f64 {
                ::prost::encoding::double::encode(1u32, &self.lon, buf);
            }
            if self.lat != 0f64 {
                ::prost::encoding::double::encode(2u32, &self.lat, buf);
            }
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "GeoPoint";
            match tag {
                1u32 => {
                    let mut value = &mut self.lon;
                    ::prost::encoding::double::merge(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "lon");
                            error
                        },
                    )
                }
                2u32 => {
                    let mut value = &mut self.lat;
                    ::prost::encoding::double::merge(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "lat");
                            error
                        },
                    )
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0 + if self.lon != 0f64 {
                ::prost::encoding::double::encoded_len(1u32, &self.lon)
            } else {
                0
            } + if self.lat != 0f64 {
                ::prost::encoding::double::encoded_len(2u32, &self.lat)
            } else {
                0
            }
        }
        fn clear(&mut self) {
            self.lon = 0f64;
            self.lat = 0f64;
        }
    }
    impl Default for GeoPoint {
        fn default() -> Self {
            GeoPoint {
                lon: 0f64,
                lat: 0f64,
            }
        }
    }
    impl ::core::fmt::Debug for GeoPoint {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("GeoPoint");
            let builder = {
                let wrapper = {
                    fn ScalarWrapper<T>(v: T) -> T {
                        v
                    }
                    ScalarWrapper(&self.lon)
                };
                builder.field("lon", &wrapper)
            };
            let builder = {
                let wrapper = {
                    fn ScalarWrapper<T>(v: T) -> T {
                        v
                    }
                    ScalarWrapper(&self.lat)
                };
                builder.field("lat", &wrapper)
            };
            builder.finish()
        }
    }
    pub struct PointsOperationResponse {
        #[prost(message, optional, tag = "1")]
        pub result: ::core::option::Option<UpdateResult>,
        #[prost(string, optional, tag = "2")]
        pub error: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(double, tag = "3")]
        pub time: f64,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for PointsOperationResponse {
        #[inline]
        fn clone(&self) -> PointsOperationResponse {
            match *self {
                PointsOperationResponse {
                    result: ref __self_0_0,
                    error: ref __self_0_1,
                    time: ref __self_0_2,
                } => PointsOperationResponse {
                    result: ::core::clone::Clone::clone(&(*__self_0_0)),
                    error: ::core::clone::Clone::clone(&(*__self_0_1)),
                    time: ::core::clone::Clone::clone(&(*__self_0_2)),
                },
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for PointsOperationResponse {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for PointsOperationResponse {
        #[inline]
        fn eq(&self, other: &PointsOperationResponse) -> bool {
            match *other {
                PointsOperationResponse {
                    result: ref __self_1_0,
                    error: ref __self_1_1,
                    time: ref __self_1_2,
                } => match *self {
                    PointsOperationResponse {
                        result: ref __self_0_0,
                        error: ref __self_0_1,
                        time: ref __self_0_2,
                    } => {
                        (*__self_0_0) == (*__self_1_0)
                            && (*__self_0_1) == (*__self_1_1)
                            && (*__self_0_2) == (*__self_1_2)
                    }
                },
            }
        }
        #[inline]
        fn ne(&self, other: &PointsOperationResponse) -> bool {
            match *other {
                PointsOperationResponse {
                    result: ref __self_1_0,
                    error: ref __self_1_1,
                    time: ref __self_1_2,
                } => match *self {
                    PointsOperationResponse {
                        result: ref __self_0_0,
                        error: ref __self_0_1,
                        time: ref __self_0_2,
                    } => {
                        (*__self_0_0) != (*__self_1_0)
                            || (*__self_0_1) != (*__self_1_1)
                            || (*__self_0_2) != (*__self_1_2)
                    }
                },
            }
        }
    }
    impl ::prost::Message for PointsOperationResponse {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
            if let Some(ref msg) = self.result {
                ::prost::encoding::message::encode(1u32, msg, buf);
            }
            if let ::core::option::Option::Some(ref value) = self.error {
                ::prost::encoding::string::encode(2u32, value, buf);
            }
            if self.time != 0f64 {
                ::prost::encoding::double::encode(3u32, &self.time, buf);
            }
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "PointsOperationResponse";
            match tag {
                1u32 => {
                    let mut value = &mut self.result;
                    ::prost::encoding::message::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "result");
                        error
                    })
                }
                2u32 => {
                    let mut value = &mut self.error;
                    ::prost::encoding::string::merge(
                        wire_type,
                        value.get_or_insert_with(Default::default),
                        buf,
                        ctx,
                    )
                    .map_err(|mut error| {
                        error.push(STRUCT_NAME, "error");
                        error
                    })
                }
                3u32 => {
                    let mut value = &mut self.time;
                    ::prost::encoding::double::merge(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "time");
                            error
                        },
                    )
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0 + self
                .result
                .as_ref()
                .map_or(0, |msg| ::prost::encoding::message::encoded_len(1u32, msg))
                + self.error.as_ref().map_or(0, |value| {
                    ::prost::encoding::string::encoded_len(2u32, value)
                })
                + if self.time != 0f64 {
                    ::prost::encoding::double::encoded_len(3u32, &self.time)
                } else {
                    0
                }
        }
        fn clear(&mut self) {
            self.result = ::core::option::Option::None;
            self.error = ::core::option::Option::None;
            self.time = 0f64;
        }
    }
    impl Default for PointsOperationResponse {
        fn default() -> Self {
            PointsOperationResponse {
                result: ::core::default::Default::default(),
                error: ::core::option::Option::None,
                time: 0f64,
            }
        }
    }
    impl ::core::fmt::Debug for PointsOperationResponse {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("PointsOperationResponse");
            let builder = {
                let wrapper = &self.result;
                builder.field("result", &wrapper)
            };
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(
                        &'a ::core::option::Option<::prost::alloc::string::String>,
                    );
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            fn Inner<T>(v: T) -> T {
                                v
                            }
                            ::core::fmt::Debug::fmt(&self.0.as_ref().map(Inner), f)
                        }
                    }
                    ScalarWrapper(&self.error)
                };
                builder.field("error", &wrapper)
            };
            let builder = {
                let wrapper = {
                    fn ScalarWrapper<T>(v: T) -> T {
                        v
                    }
                    ScalarWrapper(&self.time)
                };
                builder.field("time", &wrapper)
            };
            builder.finish()
        }
    }
    #[allow(dead_code)]
    impl PointsOperationResponse {
        ///Returns the value of `error`, or the default value if `error` is unset.
        pub fn error(&self) -> &str {
            match self.error {
                ::core::option::Option::Some(ref val) => &val[..],
                ::core::option::Option::None => "",
            }
        }
    }
    pub struct UpdateResult {
        #[prost(uint64, tag = "1")]
        pub operation_id: u64,
        #[prost(enumeration = "UpdateStatus", tag = "2")]
        pub status: i32,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for UpdateResult {
        #[inline]
        fn clone(&self) -> UpdateResult {
            match *self {
                UpdateResult {
                    operation_id: ref __self_0_0,
                    status: ref __self_0_1,
                } => UpdateResult {
                    operation_id: ::core::clone::Clone::clone(&(*__self_0_0)),
                    status: ::core::clone::Clone::clone(&(*__self_0_1)),
                },
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for UpdateResult {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for UpdateResult {
        #[inline]
        fn eq(&self, other: &UpdateResult) -> bool {
            match *other {
                UpdateResult {
                    operation_id: ref __self_1_0,
                    status: ref __self_1_1,
                } => match *self {
                    UpdateResult {
                        operation_id: ref __self_0_0,
                        status: ref __self_0_1,
                    } => (*__self_0_0) == (*__self_1_0) && (*__self_0_1) == (*__self_1_1),
                },
            }
        }
        #[inline]
        fn ne(&self, other: &UpdateResult) -> bool {
            match *other {
                UpdateResult {
                    operation_id: ref __self_1_0,
                    status: ref __self_1_1,
                } => match *self {
                    UpdateResult {
                        operation_id: ref __self_0_0,
                        status: ref __self_0_1,
                    } => (*__self_0_0) != (*__self_1_0) || (*__self_0_1) != (*__self_1_1),
                },
            }
        }
    }
    impl ::prost::Message for UpdateResult {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
            if self.operation_id != 0u64 {
                ::prost::encoding::uint64::encode(1u32, &self.operation_id, buf);
            }
            if self.status != UpdateStatus::default() as i32 {
                ::prost::encoding::int32::encode(2u32, &self.status, buf);
            }
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "UpdateResult";
            match tag {
                1u32 => {
                    let mut value = &mut self.operation_id;
                    ::prost::encoding::uint64::merge(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "operation_id");
                            error
                        },
                    )
                }
                2u32 => {
                    let mut value = &mut self.status;
                    ::prost::encoding::int32::merge(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "status");
                            error
                        },
                    )
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0 + if self.operation_id != 0u64 {
                ::prost::encoding::uint64::encoded_len(1u32, &self.operation_id)
            } else {
                0
            } + if self.status != UpdateStatus::default() as i32 {
                ::prost::encoding::int32::encoded_len(2u32, &self.status)
            } else {
                0
            }
        }
        fn clear(&mut self) {
            self.operation_id = 0u64;
            self.status = UpdateStatus::default() as i32;
        }
    }
    impl Default for UpdateResult {
        fn default() -> Self {
            UpdateResult {
                operation_id: 0u64,
                status: UpdateStatus::default() as i32,
            }
        }
    }
    impl ::core::fmt::Debug for UpdateResult {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("UpdateResult");
            let builder = {
                let wrapper = {
                    fn ScalarWrapper<T>(v: T) -> T {
                        v
                    }
                    ScalarWrapper(&self.operation_id)
                };
                builder.field("operation_id", &wrapper)
            };
            let builder = {
                let wrapper = {
                    struct ScalarWrapper<'a>(&'a i32);
                    impl<'a> ::core::fmt::Debug for ScalarWrapper<'a> {
                        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                            match UpdateStatus::from_i32(*self.0) {
                                None => ::core::fmt::Debug::fmt(&self.0, f),
                                Some(en) => ::core::fmt::Debug::fmt(&en, f),
                            }
                        }
                    }
                    ScalarWrapper(&self.status)
                };
                builder.field("status", &wrapper)
            };
            builder.finish()
        }
    }
    #[allow(dead_code)]
    impl UpdateResult {
        ///Returns the enum value of `status`, or the default if the field is set to an invalid enum value.
        pub fn status(&self) -> UpdateStatus {
            UpdateStatus::from_i32(self.status).unwrap_or(UpdateStatus::default())
        }
        ///Sets `status` to the provided enum value.
        pub fn set_status(&mut self, value: UpdateStatus) {
            self.status = value as i32;
        }
    }
    #[repr(i32)]
    pub enum UpdateStatus {
        Acknowledged = 0,
        Completed = 1,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for UpdateStatus {
        #[inline]
        fn clone(&self) -> UpdateStatus {
            {
                *self
            }
        }
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::marker::Copy for UpdateStatus {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::fmt::Debug for UpdateStatus {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match (&*self,) {
                (&UpdateStatus::Acknowledged,) => {
                    let debug_trait_builder =
                        &mut ::core::fmt::Formatter::debug_tuple(f, "Acknowledged");
                    ::core::fmt::DebugTuple::finish(debug_trait_builder)
                }
                (&UpdateStatus::Completed,) => {
                    let debug_trait_builder =
                        &mut ::core::fmt::Formatter::debug_tuple(f, "Completed");
                    ::core::fmt::DebugTuple::finish(debug_trait_builder)
                }
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for UpdateStatus {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for UpdateStatus {
        #[inline]
        fn eq(&self, other: &UpdateStatus) -> bool {
            {
                let __self_vi = ::core::intrinsics::discriminant_value(&*self);
                let __arg_1_vi = ::core::intrinsics::discriminant_value(&*other);
                if true && __self_vi == __arg_1_vi {
                    match (&*self, &*other) {
                        _ => true,
                    }
                } else {
                    false
                }
            }
        }
    }
    impl ::core::marker::StructuralEq for UpdateStatus {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::Eq for UpdateStatus {
        #[inline]
        #[doc(hidden)]
        #[no_coverage]
        fn assert_receiver_is_total_eq(&self) -> () {
            {}
        }
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::hash::Hash for UpdateStatus {
        fn hash<__H: ::core::hash::Hasher>(&self, state: &mut __H) -> () {
            match (&*self,) {
                _ => ::core::hash::Hash::hash(&::core::intrinsics::discriminant_value(self), state),
            }
        }
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialOrd for UpdateStatus {
        #[inline]
        fn partial_cmp(
            &self,
            other: &UpdateStatus,
        ) -> ::core::option::Option<::core::cmp::Ordering> {
            {
                let __self_vi = ::core::intrinsics::discriminant_value(&*self);
                let __arg_1_vi = ::core::intrinsics::discriminant_value(&*other);
                if true && __self_vi == __arg_1_vi {
                    match (&*self, &*other) {
                        _ => ::core::option::Option::Some(::core::cmp::Ordering::Equal),
                    }
                } else {
                    ::core::cmp::PartialOrd::partial_cmp(&__self_vi, &__arg_1_vi)
                }
            }
        }
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::Ord for UpdateStatus {
        #[inline]
        fn cmp(&self, other: &UpdateStatus) -> ::core::cmp::Ordering {
            {
                let __self_vi = ::core::intrinsics::discriminant_value(&*self);
                let __arg_1_vi = ::core::intrinsics::discriminant_value(&*other);
                if true && __self_vi == __arg_1_vi {
                    match (&*self, &*other) {
                        _ => ::core::cmp::Ordering::Equal,
                    }
                } else {
                    ::core::cmp::Ord::cmp(&__self_vi, &__arg_1_vi)
                }
            }
        }
    }
    impl UpdateStatus {
        ///Returns `true` if `value` is a variant of `UpdateStatus`.
        pub fn is_valid(value: i32) -> bool {
            match value {
                0 => true,
                1 => true,
                _ => false,
            }
        }
        ///Converts an `i32` to a `UpdateStatus`, or `None` if `value` is not a valid variant.
        pub fn from_i32(value: i32) -> ::core::option::Option<UpdateStatus> {
            match value {
                0 => ::core::option::Option::Some(UpdateStatus::Acknowledged),
                1 => ::core::option::Option::Some(UpdateStatus::Completed),
                _ => ::core::option::Option::None,
            }
        }
    }
    impl ::core::default::Default for UpdateStatus {
        fn default() -> UpdateStatus {
            UpdateStatus::Acknowledged
        }
    }
    impl ::core::convert::From<UpdateStatus> for i32 {
        fn from(value: UpdateStatus) -> i32 {
            value as i32
        }
    }
    /// Generated client implementations.
    pub mod points_client {
        #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
        use tonic::codegen::*;
        pub struct PointsClient<T> {
            inner: tonic::client::Grpc<T>,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl<T: ::core::fmt::Debug> ::core::fmt::Debug for PointsClient<T> {
            fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                match *self {
                    PointsClient {
                        inner: ref __self_0_0,
                    } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "PointsClient");
                        let _ = ::core::fmt::DebugStruct::field(
                            debug_trait_builder,
                            "inner",
                            &&(*__self_0_0),
                        );
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl<T: ::core::clone::Clone> ::core::clone::Clone for PointsClient<T> {
            #[inline]
            fn clone(&self) -> PointsClient<T> {
                match *self {
                    PointsClient {
                        inner: ref __self_0_0,
                    } => PointsClient {
                        inner: ::core::clone::Clone::clone(&(*__self_0_0)),
                    },
                }
            }
        }
        impl PointsClient<tonic::transport::Channel> {
            /// Attempt to create a new client by connecting to a given endpoint.
            pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
            where
                D: std::convert::TryInto<tonic::transport::Endpoint>,
                D::Error: Into<StdError>,
            {
                let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
                Ok(Self::new(conn))
            }
        }
        impl<T> PointsClient<T>
        where
            T: tonic::client::GrpcService<tonic::body::BoxBody>,
            T::ResponseBody: Body + Send + Sync + 'static,
            T::Error: Into<StdError>,
            <T::ResponseBody as Body>::Error: Into<StdError> + Send,
        {
            pub fn new(inner: T) -> Self {
                let inner = tonic::client::Grpc::new(inner);
                Self { inner }
            }
            pub fn with_interceptor<F>(
                inner: T,
                interceptor: F,
            ) -> PointsClient<InterceptedService<T, F>>
            where
                F: FnMut(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
                T: tonic::codegen::Service<
                    http::Request<tonic::body::BoxBody>,
                    Response = http::Response<
                        <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                    >,
                >,
                <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                    Into<StdError> + Send + Sync,
            {
                PointsClient::new(InterceptedService::new(inner, interceptor))
            }
            /// Compress requests with `gzip`.
            ///
            /// This requires the server to support it otherwise it might respond with an
            /// error.
            pub fn send_gzip(mut self) -> Self {
                self.inner = self.inner.send_gzip();
                self
            }
            /// Enable decompressing responses with `gzip`.
            pub fn accept_gzip(mut self) -> Self {
                self.inner = self.inner.accept_gzip();
                self
            }
            pub async fn upsert(
                &mut self,
                request: impl tonic::IntoRequest<super::UpsertPoints>,
            ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status>
            {
                self.inner.ready().await.map_err(|e| {
                    tonic::Status::new(tonic::Code::Unknown, {
                        let res = ::alloc::fmt::format(::core::fmt::Arguments::new_v1(
                            &["Service was not ready: "],
                            &match (&e.into(),) {
                                (arg0,) => [::core::fmt::ArgumentV1::new(
                                    arg0,
                                    ::core::fmt::Display::fmt,
                                )],
                            },
                        ));
                        res
                    })
                })?;
                let codec = tonic::codec::ProstCodec::default();
                let path = http::uri::PathAndQuery::from_static("/qdrant.Points/Upsert");
                self.inner.unary(request.into_request(), path, codec).await
            }
        }
    }
    /// Generated server implementations.
    pub mod points_server {
        #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
        use tonic::codegen::*;
        ///Generated trait containing gRPC methods that should be implemented for use with PointsServer.
        pub trait Points: Send + Sync + 'static {
            #[must_use]
            #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
            fn upsert<'life0, 'async_trait>(
                &'life0 self,
                request: tonic::Request<super::UpsertPoints>,
            ) -> ::core::pin::Pin<
                Box<
                    dyn ::core::future::Future<
                            Output = Result<
                                tonic::Response<super::PointsOperationResponse>,
                                tonic::Status,
                            >,
                        > + ::core::marker::Send
                        + 'async_trait,
                >,
            >
            where
                'life0: 'async_trait,
                Self: 'async_trait;
        }
        pub struct PointsServer<T: Points> {
            inner: _Inner<T>,
            accept_compression_encodings: (),
            send_compression_encodings: (),
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl<T: ::core::fmt::Debug + Points> ::core::fmt::Debug for PointsServer<T> {
            fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                match *self {
                    PointsServer {
                        inner: ref __self_0_0,
                        accept_compression_encodings: ref __self_0_1,
                        send_compression_encodings: ref __self_0_2,
                    } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "PointsServer");
                        let _ = ::core::fmt::DebugStruct::field(
                            debug_trait_builder,
                            "inner",
                            &&(*__self_0_0),
                        );
                        let _ = ::core::fmt::DebugStruct::field(
                            debug_trait_builder,
                            "accept_compression_encodings",
                            &&(*__self_0_1),
                        );
                        let _ = ::core::fmt::DebugStruct::field(
                            debug_trait_builder,
                            "send_compression_encodings",
                            &&(*__self_0_2),
                        );
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        struct _Inner<T>(Arc<T>);
        impl<T: Points> PointsServer<T> {
            pub fn new(inner: T) -> Self {
                let inner = Arc::new(inner);
                let inner = _Inner(inner);
                Self {
                    inner,
                    accept_compression_encodings: Default::default(),
                    send_compression_encodings: Default::default(),
                }
            }
            pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
            where
                F: FnMut(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
            {
                InterceptedService::new(Self::new(inner), interceptor)
            }
        }
        impl<T, B> tonic::codegen::Service<http::Request<B>> for PointsServer<T>
        where
            T: Points,
            B: Body + Send + Sync + 'static,
            B::Error: Into<StdError> + Send + 'static,
        {
            type Response = http::Response<tonic::body::BoxBody>;
            type Error = Never;
            type Future = BoxFuture<Self::Response, Self::Error>;
            fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }
            fn call(&mut self, req: http::Request<B>) -> Self::Future {
                let inner = self.inner.clone();
                match req.uri().path() {
                    "/qdrant.Points/Upsert" => {
                        #[allow(non_camel_case_types)]
                        struct UpsertSvc<T: Points>(pub Arc<T>);
                        impl<T: Points> tonic::server::UnaryService<super::UpsertPoints> for UpsertSvc<T> {
                            type Response = super::PointsOperationResponse;
                            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                            fn call(
                                &mut self,
                                request: tonic::Request<super::UpsertPoints>,
                            ) -> Self::Future {
                                let inner = self.0.clone();
                                let fut = async move { (*inner).upsert(request).await };
                                Box::pin(fut)
                            }
                        }
                        let accept_compression_encodings = self.accept_compression_encodings;
                        let send_compression_encodings = self.send_compression_encodings;
                        let inner = self.inner.clone();
                        let fut = async move {
                            let inner = inner.0;
                            let method = UpsertSvc(inner);
                            let codec = tonic::codec::ProstCodec::default();
                            let mut grpc = tonic::server::Grpc::new(codec)
                                .apply_compression_config(
                                    accept_compression_encodings,
                                    send_compression_encodings,
                                );
                            let res = grpc.unary(method, req).await;
                            Ok(res)
                        };
                        Box::pin(fut)
                    }
                    _ => Box::pin(async move {
                        Ok(http::Response::builder()
                            .status(200)
                            .header("grpc-status", "12")
                            .header("content-type", "application/grpc")
                            .body(empty_body())
                            .unwrap())
                    }),
                }
            }
        }
        impl<T: Points> Clone for PointsServer<T> {
            fn clone(&self) -> Self {
                let inner = self.inner.clone();
                Self {
                    inner,
                    accept_compression_encodings: self.accept_compression_encodings,
                    send_compression_encodings: self.send_compression_encodings,
                }
            }
        }
        impl<T: Points> Clone for _Inner<T> {
            fn clone(&self) -> Self {
                Self(self.0.clone())
            }
        }
        impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_fmt(::core::fmt::Arguments::new_v1(
                    &[""],
                    &match (&self.0,) {
                        (arg0,) => [::core::fmt::ArgumentV1::new(arg0, ::core::fmt::Debug::fmt)],
                    },
                ))
            }
        }
        impl<T: Points> tonic::transport::NamedService for PointsServer<T> {
            const NAME: &'static str = "qdrant.Points";
        }
    }
    pub struct HealthCheckRequest {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for HealthCheckRequest {
        #[inline]
        fn clone(&self) -> HealthCheckRequest {
            match *self {
                HealthCheckRequest {} => HealthCheckRequest {},
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for HealthCheckRequest {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for HealthCheckRequest {
        #[inline]
        fn eq(&self, other: &HealthCheckRequest) -> bool {
            match *other {
                HealthCheckRequest {} => match *self {
                    HealthCheckRequest {} => true,
                },
            }
        }
    }
    impl ::prost::Message for HealthCheckRequest {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            match tag {
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0
        }
        fn clear(&mut self) {}
    }
    impl Default for HealthCheckRequest {
        fn default() -> Self {
            HealthCheckRequest {}
        }
    }
    impl ::core::fmt::Debug for HealthCheckRequest {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("HealthCheckRequest");
            builder.finish()
        }
    }
    pub struct HealthCheckReply {
        #[prost(string, tag = "1")]
        pub title: ::prost::alloc::string::String,
        #[prost(string, tag = "2")]
        pub version: ::prost::alloc::string::String,
    }
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::clone::Clone for HealthCheckReply {
        #[inline]
        fn clone(&self) -> HealthCheckReply {
            match *self {
                HealthCheckReply {
                    title: ref __self_0_0,
                    version: ref __self_0_1,
                } => HealthCheckReply {
                    title: ::core::clone::Clone::clone(&(*__self_0_0)),
                    version: ::core::clone::Clone::clone(&(*__self_0_1)),
                },
            }
        }
    }
    impl ::core::marker::StructuralPartialEq for HealthCheckReply {}
    #[automatically_derived]
    #[allow(unused_qualifications)]
    impl ::core::cmp::PartialEq for HealthCheckReply {
        #[inline]
        fn eq(&self, other: &HealthCheckReply) -> bool {
            match *other {
                HealthCheckReply {
                    title: ref __self_1_0,
                    version: ref __self_1_1,
                } => match *self {
                    HealthCheckReply {
                        title: ref __self_0_0,
                        version: ref __self_0_1,
                    } => (*__self_0_0) == (*__self_1_0) && (*__self_0_1) == (*__self_1_1),
                },
            }
        }
        #[inline]
        fn ne(&self, other: &HealthCheckReply) -> bool {
            match *other {
                HealthCheckReply {
                    title: ref __self_1_0,
                    version: ref __self_1_1,
                } => match *self {
                    HealthCheckReply {
                        title: ref __self_0_0,
                        version: ref __self_0_1,
                    } => (*__self_0_0) != (*__self_1_0) || (*__self_0_1) != (*__self_1_1),
                },
            }
        }
    }
    impl ::prost::Message for HealthCheckReply {
        #[allow(unused_variables)]
        fn encode_raw<B>(&self, buf: &mut B)
        where
            B: ::prost::bytes::BufMut,
        {
            if self.title != "" {
                ::prost::encoding::string::encode(1u32, &self.title, buf);
            }
            if self.version != "" {
                ::prost::encoding::string::encode(2u32, &self.version, buf);
            }
        }
        #[allow(unused_variables)]
        fn merge_field<B>(
            &mut self,
            tag: u32,
            wire_type: ::prost::encoding::WireType,
            buf: &mut B,
            ctx: ::prost::encoding::DecodeContext,
        ) -> ::core::result::Result<(), ::prost::DecodeError>
        where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "HealthCheckReply";
            match tag {
                1u32 => {
                    let mut value = &mut self.title;
                    ::prost::encoding::string::merge(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "title");
                            error
                        },
                    )
                }
                2u32 => {
                    let mut value = &mut self.version;
                    ::prost::encoding::string::merge(wire_type, value, buf, ctx).map_err(
                        |mut error| {
                            error.push(STRUCT_NAME, "version");
                            error
                        },
                    )
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }
        #[inline]
        fn encoded_len(&self) -> usize {
            0 + if self.title != "" {
                ::prost::encoding::string::encoded_len(1u32, &self.title)
            } else {
                0
            } + if self.version != "" {
                ::prost::encoding::string::encoded_len(2u32, &self.version)
            } else {
                0
            }
        }
        fn clear(&mut self) {
            self.title.clear();
            self.version.clear();
        }
    }
    impl Default for HealthCheckReply {
        fn default() -> Self {
            HealthCheckReply {
                title: ::prost::alloc::string::String::new(),
                version: ::prost::alloc::string::String::new(),
            }
        }
    }
    impl ::core::fmt::Debug for HealthCheckReply {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            let mut builder = f.debug_struct("HealthCheckReply");
            let builder = {
                let wrapper = {
                    fn ScalarWrapper<T>(v: T) -> T {
                        v
                    }
                    ScalarWrapper(&self.title)
                };
                builder.field("title", &wrapper)
            };
            let builder = {
                let wrapper = {
                    fn ScalarWrapper<T>(v: T) -> T {
                        v
                    }
                    ScalarWrapper(&self.version)
                };
                builder.field("version", &wrapper)
            };
            builder.finish()
        }
    }
    /// Generated client implementations.
    pub mod qdrant_client {
        #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
        use tonic::codegen::*;
        pub struct QdrantClient<T> {
            inner: tonic::client::Grpc<T>,
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl<T: ::core::fmt::Debug> ::core::fmt::Debug for QdrantClient<T> {
            fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                match *self {
                    QdrantClient {
                        inner: ref __self_0_0,
                    } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "QdrantClient");
                        let _ = ::core::fmt::DebugStruct::field(
                            debug_trait_builder,
                            "inner",
                            &&(*__self_0_0),
                        );
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl<T: ::core::clone::Clone> ::core::clone::Clone for QdrantClient<T> {
            #[inline]
            fn clone(&self) -> QdrantClient<T> {
                match *self {
                    QdrantClient {
                        inner: ref __self_0_0,
                    } => QdrantClient {
                        inner: ::core::clone::Clone::clone(&(*__self_0_0)),
                    },
                }
            }
        }
        impl QdrantClient<tonic::transport::Channel> {
            /// Attempt to create a new client by connecting to a given endpoint.
            pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
            where
                D: std::convert::TryInto<tonic::transport::Endpoint>,
                D::Error: Into<StdError>,
            {
                let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
                Ok(Self::new(conn))
            }
        }
        impl<T> QdrantClient<T>
        where
            T: tonic::client::GrpcService<tonic::body::BoxBody>,
            T::ResponseBody: Body + Send + Sync + 'static,
            T::Error: Into<StdError>,
            <T::ResponseBody as Body>::Error: Into<StdError> + Send,
        {
            pub fn new(inner: T) -> Self {
                let inner = tonic::client::Grpc::new(inner);
                Self { inner }
            }
            pub fn with_interceptor<F>(
                inner: T,
                interceptor: F,
            ) -> QdrantClient<InterceptedService<T, F>>
            where
                F: FnMut(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
                T: tonic::codegen::Service<
                    http::Request<tonic::body::BoxBody>,
                    Response = http::Response<
                        <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                    >,
                >,
                <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                    Into<StdError> + Send + Sync,
            {
                QdrantClient::new(InterceptedService::new(inner, interceptor))
            }
            /// Compress requests with `gzip`.
            ///
            /// This requires the server to support it otherwise it might respond with an
            /// error.
            pub fn send_gzip(mut self) -> Self {
                self.inner = self.inner.send_gzip();
                self
            }
            /// Enable decompressing responses with `gzip`.
            pub fn accept_gzip(mut self) -> Self {
                self.inner = self.inner.accept_gzip();
                self
            }
            pub async fn health_check(
                &mut self,
                request: impl tonic::IntoRequest<super::HealthCheckRequest>,
            ) -> Result<tonic::Response<super::HealthCheckReply>, tonic::Status> {
                self.inner.ready().await.map_err(|e| {
                    tonic::Status::new(tonic::Code::Unknown, {
                        let res = ::alloc::fmt::format(::core::fmt::Arguments::new_v1(
                            &["Service was not ready: "],
                            &match (&e.into(),) {
                                (arg0,) => [::core::fmt::ArgumentV1::new(
                                    arg0,
                                    ::core::fmt::Display::fmt,
                                )],
                            },
                        ));
                        res
                    })
                })?;
                let codec = tonic::codec::ProstCodec::default();
                let path = http::uri::PathAndQuery::from_static("/qdrant.Qdrant/HealthCheck");
                self.inner.unary(request.into_request(), path, codec).await
            }
        }
    }
    /// Generated server implementations.
    pub mod qdrant_server {
        #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
        use tonic::codegen::*;
        ///Generated trait containing gRPC methods that should be implemented for use with QdrantServer.
        pub trait Qdrant: Send + Sync + 'static {
            #[must_use]
            #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
            fn health_check<'life0, 'async_trait>(
                &'life0 self,
                request: tonic::Request<super::HealthCheckRequest>,
            ) -> ::core::pin::Pin<
                Box<
                    dyn ::core::future::Future<
                            Output = Result<
                                tonic::Response<super::HealthCheckReply>,
                                tonic::Status,
                            >,
                        > + ::core::marker::Send
                        + 'async_trait,
                >,
            >
            where
                'life0: 'async_trait,
                Self: 'async_trait;
        }
        pub struct QdrantServer<T: Qdrant> {
            inner: _Inner<T>,
            accept_compression_encodings: (),
            send_compression_encodings: (),
        }
        #[automatically_derived]
        #[allow(unused_qualifications)]
        impl<T: ::core::fmt::Debug + Qdrant> ::core::fmt::Debug for QdrantServer<T> {
            fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                match *self {
                    QdrantServer {
                        inner: ref __self_0_0,
                        accept_compression_encodings: ref __self_0_1,
                        send_compression_encodings: ref __self_0_2,
                    } => {
                        let debug_trait_builder =
                            &mut ::core::fmt::Formatter::debug_struct(f, "QdrantServer");
                        let _ = ::core::fmt::DebugStruct::field(
                            debug_trait_builder,
                            "inner",
                            &&(*__self_0_0),
                        );
                        let _ = ::core::fmt::DebugStruct::field(
                            debug_trait_builder,
                            "accept_compression_encodings",
                            &&(*__self_0_1),
                        );
                        let _ = ::core::fmt::DebugStruct::field(
                            debug_trait_builder,
                            "send_compression_encodings",
                            &&(*__self_0_2),
                        );
                        ::core::fmt::DebugStruct::finish(debug_trait_builder)
                    }
                }
            }
        }
        struct _Inner<T>(Arc<T>);
        impl<T: Qdrant> QdrantServer<T> {
            pub fn new(inner: T) -> Self {
                let inner = Arc::new(inner);
                let inner = _Inner(inner);
                Self {
                    inner,
                    accept_compression_encodings: Default::default(),
                    send_compression_encodings: Default::default(),
                }
            }
            pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
            where
                F: FnMut(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
            {
                InterceptedService::new(Self::new(inner), interceptor)
            }
        }
        impl<T, B> tonic::codegen::Service<http::Request<B>> for QdrantServer<T>
        where
            T: Qdrant,
            B: Body + Send + Sync + 'static,
            B::Error: Into<StdError> + Send + 'static,
        {
            type Response = http::Response<tonic::body::BoxBody>;
            type Error = Never;
            type Future = BoxFuture<Self::Response, Self::Error>;
            fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }
            fn call(&mut self, req: http::Request<B>) -> Self::Future {
                let inner = self.inner.clone();
                match req.uri().path() {
                    "/qdrant.Qdrant/HealthCheck" => {
                        #[allow(non_camel_case_types)]
                        struct HealthCheckSvc<T: Qdrant>(pub Arc<T>);
                        impl<T: Qdrant> tonic::server::UnaryService<super::HealthCheckRequest> for HealthCheckSvc<T> {
                            type Response = super::HealthCheckReply;
                            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                            fn call(
                                &mut self,
                                request: tonic::Request<super::HealthCheckRequest>,
                            ) -> Self::Future {
                                let inner = self.0.clone();
                                let fut = async move { (*inner).health_check(request).await };
                                Box::pin(fut)
                            }
                        }
                        let accept_compression_encodings = self.accept_compression_encodings;
                        let send_compression_encodings = self.send_compression_encodings;
                        let inner = self.inner.clone();
                        let fut = async move {
                            let inner = inner.0;
                            let method = HealthCheckSvc(inner);
                            let codec = tonic::codec::ProstCodec::default();
                            let mut grpc = tonic::server::Grpc::new(codec)
                                .apply_compression_config(
                                    accept_compression_encodings,
                                    send_compression_encodings,
                                );
                            let res = grpc.unary(method, req).await;
                            Ok(res)
                        };
                        Box::pin(fut)
                    }
                    _ => Box::pin(async move {
                        Ok(http::Response::builder()
                            .status(200)
                            .header("grpc-status", "12")
                            .header("content-type", "application/grpc")
                            .body(empty_body())
                            .unwrap())
                    }),
                }
            }
        }
        impl<T: Qdrant> Clone for QdrantServer<T> {
            fn clone(&self) -> Self {
                let inner = self.inner.clone();
                Self {
                    inner,
                    accept_compression_encodings: self.accept_compression_encodings,
                    send_compression_encodings: self.send_compression_encodings,
                }
            }
        }
        impl<T: Qdrant> Clone for _Inner<T> {
            fn clone(&self) -> Self {
                Self(self.0.clone())
            }
        }
        impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_fmt(::core::fmt::Arguments::new_v1(
                    &[""],
                    &match (&self.0,) {
                        (arg0,) => [::core::fmt::ArgumentV1::new(arg0, ::core::fmt::Debug::fmt)],
                    },
                ))
            }
        }
        impl<T: Qdrant> tonic::transport::NamedService for QdrantServer<T> {
            const NAME: &'static str = "qdrant.Qdrant";
        }
    }
}
