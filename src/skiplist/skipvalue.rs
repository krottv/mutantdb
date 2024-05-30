pub enum SkipData<T> {
    // leftmost nodes are dummy
    Dummy(),
    // bottom nodes (except leftmost)
    Owned(Box<T>, *mut T),
    Pointer(*mut T),
}


impl<T> SkipData<T> {
    pub fn is_none(&self) -> bool {
        match self {
            SkipData::Dummy() => {
                return true;
            }
            _ => {
                return false;
            }
        }
    }

    pub fn get_pointer(&self) -> *mut T {
        match self {
            SkipData::Dummy() => {
                panic!("cannot unwrap dummy node");
            }
            SkipData::Owned(_, pointer) => {
                return *pointer;
            }
            SkipData::Pointer(pointer) => {
                return *pointer;
            }
        }
    }

    pub fn get_ref(&self) -> &T {
        match self {
            SkipData::Dummy() => {
                panic!("cannot unwrap dummy node");
            }
            SkipData::Owned(data, _pointer) => {
                return data.as_ref();
            }
            SkipData::Pointer(data) => {
                unsafe {
                    return &**data;
                }
            }
        }
    }
}