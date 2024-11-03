pub mod async_redis_client;
pub mod http_client;
pub mod nature_remo_client;
pub mod null_nature_remo_client;
pub mod null_redis_client;

cfg_if::cfg_if! {
    if #[cfg(not(test))] {
        pub use async_redis_client::AsyncRedisCrateClient;
        pub use http_client::ReqwestClient;
        pub use nature_remo_client::NatureRemoClient;
        pub use null_nature_remo_client::NullNatureRemoClient;
        pub use null_redis_client::NullRedisClient;
    } else if #[cfg(test)] {
        pub mod mocks;
        pub use mocks::MockAsyncRedisCrateClient as AsyncRedisCrateClient;
        pub use http_client::ReqwestClient;
        pub use mocks::MockNatureRemoClient as NatureRemoClient;
        pub use null_nature_remo_client::MockNullNatureRemoClient as NullNatureRemoClient;
        pub use mocks::MockNullRedisClient as NullRedisClient;
    }
}
