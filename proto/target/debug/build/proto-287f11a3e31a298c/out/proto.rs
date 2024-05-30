// This file is @generated by prost-build.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockIndex {
    #[prost(bytes = "vec", tag = "1")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint32, tag = "2")]
    pub offset: u32,
    #[prost(uint32, tag = "3")]
    pub len: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableIndex {
    #[prost(message, repeated, tag = "1")]
    pub blocks: ::prost::alloc::vec::Vec<BlockIndex>,
    #[prost(uint64, tag = "2")]
    pub bytes_len: u64,
    #[prost(uint64, tag = "3")]
    pub max_version: u64,
    #[prost(uint64, tag = "4")]
    pub key_count: u64,
}
