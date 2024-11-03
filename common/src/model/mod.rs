pub mod ambient_condition;
pub mod channel;
pub mod repository;

cfg_if::cfg_if! {
    if #[cfg(not(test))] {
        pub use ambient_condition::AmbientCondition;
    } else if #[cfg(test)] {
        pub mod mocks;
        pub use mocks::MockAmbientCondition as AmbientCondition;
    }
}
