pub struct Targets {
    pub base_level: usize,
    pub current_sizes: Vec<u64>,
    pub target_sizes: Vec<u64>
}

impl Targets {
    // compute based on bottom level
    // levels here start from 1
    // at most one level can have a positive target size below base_level_size
    pub fn compute(current_sizes: Vec<u64>, base_level_size: u64, level_size_multiplier: u32) -> Self {
        let mut target_sizes = Vec::new();
        for _i in 0..current_sizes.len() {
            target_sizes.push(0u64);
        }

        let mut base_level = 0usize;
        if current_sizes[current_sizes.len() - 1] < base_level_size {
            base_level = current_sizes.len() - 1;
            target_sizes[base_level] = base_level_size;
        } else {
            
            let mut cur_target_size = current_sizes[current_sizes.len() - 1];
            base_level = current_sizes.len() - 1;
            loop {
                if base_level == 0 {
                    break;
                }
                target_sizes[base_level] = cur_target_size;
                if cur_target_size < base_level_size {
                    break;
                }
                
                base_level -= 1;
                cur_target_size /= level_size_multiplier as u64;
            }
        }

        Targets {
            base_level,
            current_sizes,
            target_sizes
        }
    }
}

// 0 0 0 0 0 200MB
// 0 0 0 0 30MB 300MB
// 0 0 30MB 300MB 3GB 30GB

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_lsm_state() {
        let current_sizes = vec![0, 0, 0, 0, 0, 0];
        let base_level_size = 200;
        let level_size_multiplier = 10;
        let targets = Targets::compute(current_sizes, base_level_size, level_size_multiplier);
        assert_eq!(targets.base_level, 5);
        assert_eq!(targets.target_sizes, vec![0, 0, 0, 0, 0, 200]);
    }

    #[test]
    fn test_bottom_level_300mb() {
        let current_sizes = vec![0, 0, 0, 0, 0, 300];
        let base_level_size = 200;
        let level_size_multiplier = 10;
        let targets = Targets::compute(current_sizes, base_level_size, level_size_multiplier);
        assert_eq!(targets.base_level, 4);
        assert_eq!(targets.target_sizes, vec![0, 0, 0, 0, 30, 300]);
    }

    #[test]
    fn test_last_level_30gb() {
        let current_sizes = vec![0, 0, 0, 0, 0, 30000];
        let base_level_size = 200;
        let level_size_multiplier = 10;
        let targets = Targets::compute(current_sizes, base_level_size, level_size_multiplier);
        assert_eq!(targets.base_level, 2);
        assert_eq!(targets.target_sizes, vec![0, 0, 30, 300, 3000, 30000]);
    }
}