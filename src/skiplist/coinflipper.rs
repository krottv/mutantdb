use std::collections::hash_map::{DefaultHasher, RandomState};
use std::hash::{BuildHasher, Hasher};

pub trait CoinFlipper {
    fn flip(&self) -> bool;
}

pub struct CoinFlipperHash {
    hasher: DefaultHasher,
}

impl CoinFlipperHash {
    pub fn new() -> CoinFlipperHash {
        return CoinFlipperHash { hasher: RandomState::new().build_hasher() };
    }
}

impl CoinFlipper for CoinFlipperHash {
    fn flip(&self) -> bool {
        let random_value = self.hasher.finish() as usize;
        return random_value < (usize::MAX / 2);
    }
}