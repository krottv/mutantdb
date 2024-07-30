use std::collections::hash_map::{DefaultHasher, RandomState};
use std::hash::{BuildHasher, Hasher};
use rand::random;

#[allow(dead_code)]

pub trait CoinFlipper: Send + Sync {
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

pub struct CoinFlipperRand {}

impl CoinFlipper for CoinFlipperRand {
    fn flip(&self) -> bool {
        let random_value: usize = random();
        return random_value < (usize::MAX / 2);
    }
}