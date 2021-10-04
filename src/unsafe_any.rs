use std::{
    mem::{self, ManuallyDrop, MaybeUninit},
    ptr::{self, NonNull},
};

const MAX_SIZE: usize = mem::size_of::<NonNull<()>>();

#[repr(transparent)]
pub struct UnsafeAny {
    bytes: [MaybeUninit<u8>; MAX_SIZE],
}

impl UnsafeAny {
    const fn is_inline<T>() -> bool {
        mem::size_of::<T>() <= MAX_SIZE
    }

    #[inline(always)]
    pub fn new<T>(val: T) -> Result<Self, T> {
        if Self::is_inline::<T>() {
            Ok(unsafe { Self::new_unchecked(val) })
        } else {
            Err(val)
        }
    }

    #[inline(always)]
    pub unsafe fn new_unchecked<T>(val: T) -> Self {
        let val = ManuallyDrop::new(val);

        Self {
            bytes: mem::transmute_copy(&*val),
        }
    }

    pub const fn uninit() -> Self {
        Self {
            bytes: [
                MaybeUninit::uninit(),
                MaybeUninit::uninit(),
                MaybeUninit::uninit(),
                MaybeUninit::uninit(),
                MaybeUninit::uninit(),
                MaybeUninit::uninit(),
                MaybeUninit::uninit(),
                MaybeUninit::uninit(),
            ],
        }
    }

    pub unsafe fn into<T>(self) -> T {
        let bytes = &self.bytes as *const [_; MAX_SIZE];
        if Self::is_inline::<T>() {
            if bytes.align_offset(mem::align_of::<T>()) == 0 {
                ptr::read(bytes as *const T)
            } else {
                ptr::read_unaligned(bytes as *const T)
            }
        } else {
            let ptr = if bytes.align_offset(mem::align_of::<NonNull<T>>()) == 0 {
                ptr::read(bytes as *const NonNull<T>)
            } else {
                ptr::read_unaligned(bytes as *const NonNull<T>)
            };

            ptr::read(ptr.as_ptr())
        }
    }

    pub fn make_iter<I>(iter: I) -> impl Iterator<Item = UnsafeAny>
    where
        I: IntoIterator,
    {
        let iter = iter.into_iter();

        let mut buf: Vec<ManuallyDrop<I::Item>> = if Self::is_inline::<I::Item>() {
            Vec::new()
        } else {
            let (min, max) = iter.size_hint();

            Vec::with_capacity(max.unwrap_or(min))
        };

        iter.map(move |val| {
            Self::new(val).unwrap_or_else(|val| {
                // TODO: Handle types that lie about iterator length.
                assert!(buf.len() < buf.capacity());
                buf.push(ManuallyDrop::new(val));

                Self::new(NonNull::from(&buf[buf.len() - 1])).unwrap()
            })
        })
    }
}
