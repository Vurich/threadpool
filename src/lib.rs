use crossbeam_deque::{Injector, Stealer, Worker};
use parking_lot::Once;
use std::{
    any::Any,
    cell::UnsafeCell,
    marker,
    mem::{self, ManuallyDrop, MaybeUninit},
    ops::{ControlFlow, Deref},
    panic,
    pin::Pin,
    ptr::{self, NonNull},
    sync::atomic::{AtomicUsize, Ordering},
    thread::{self, JoinHandle},
};

mod unsafe_any;

use unsafe_any::UnsafeAny;

pub trait Executor {
    fn for_each<'data, Data, I, Callback>(&self, iter: I, callback: Callback)
    where
        Data: Send + 'data,
        I: IntoIterator<Item = Data>,
        Callback: Fn(Data) + Sync + 'data;
}

impl<C, Ptr, const COUNT: usize> Executor for Pin<Ptr>
where
    C: ?Sized + ThreadConfig,
    Ptr: Deref<Target = ThreadPool<C, COUNT>>,
{
    fn for_each<'data, Data, I, Callback>(&self, iter: I, callback: Callback)
    where
        Data: Send + 'data,
        I: IntoIterator<Item = Data>,
        Callback: Fn(Data) + Sync + 'data,
    {
        ThreadPool::for_each(self.as_ref(), iter, callback);
    }
}

fn shutdown(_ptr: UnsafeAny) -> ControlFlow<()> {
    ControlFlow::Break(())
}

const SHUTDOWN: Job = Job {
    data: UnsafeAny::uninit(),
    callback: unsafe {
        NonNull::new_unchecked(
            &shutdown as &JobCallback<'static> as *const JobCallback<'static>
                as *mut JobCallback<'static>,
        )
    },
};

type JobCallback<'lifetime> = dyn Fn(UnsafeAny) -> ControlFlow<()> + Sync + 'lifetime;

struct Job {
    data: UnsafeAny,
    callback: NonNull<JobCallback<'static>>,
}

unsafe impl Send for Job {}

pub trait ThreadConfig {
    fn spawn(&mut self, thread: Thread) -> thread::JoinHandle<()>;
}

impl<F> ThreadConfig for F
where
    F: FnMut(Thread) -> thread::JoinHandle<()>,
{
    fn spawn(&mut self, thread: Thread) -> thread::JoinHandle<()> {
        self(thread)
    }
}

pub struct Thread {
    id: usize,
    worker: Worker<Job>,
    stealers: &'static [Stealer<Job>],
    injector: &'static Injector<Job>,
}

impl Thread {
    pub fn index(&self) -> usize {
        self.id
    }

    pub fn run(self) {
        'worker: loop {
            match do_work_with_worker(&self.worker, &self.stealers, &self.injector) {
                ControlFlow::Continue(()) => {}
                ControlFlow::Break(()) => {
                    self.injector.push(SHUTDOWN);
                    break 'worker;
                }
            }
        }
    }
}

#[derive(Default)]
struct Config<Namer, Size> {
    namer: Namer,
    size: Size,
}

impl<Namer, Size, Name, StackSize> ThreadConfig for Config<Namer, Size>
where
    Namer: FnMut(usize) -> Name,
    Size: FnMut(usize) -> StackSize,
    Name: Into<Option<String>>,
    StackSize: Into<Option<usize>>,
{
    fn spawn(&mut self, thread: Thread) -> thread::JoinHandle<()> {
        let mut b = thread::Builder::new();

        if let Some(name) = (self.namer)(thread.index()).into() {
            b = b.name(name);
        }

        if let Some(size) = (self.size)(thread.index()).into() {
            b = b.stack_size(size);
        }

        b.spawn(move || thread.run())
            .expect("Invalid thread config")
    }
}

impl ThreadConfig for Config<(), ()> {
    fn spawn(&mut self, thread: Thread) -> thread::JoinHandle<()> {
        thread::spawn(move || thread.run())
    }
}

impl<Namer, Name> ThreadConfig for Config<Namer, ()>
where
    Namer: FnMut(usize) -> Name,
    Name: Into<Option<String>>,
{
    fn spawn(&mut self, thread: Thread) -> thread::JoinHandle<()> {
        let mut b = thread::Builder::new();

        if let Some(name) = (self.namer)(thread.index()).into() {
            b = b.name(name);
        }

        b.spawn(move || thread.run())
            .expect("Invalid thread config")
    }
}

impl<Size, StackSize> ThreadConfig for Config<(), Size>
where
    Size: FnMut(usize) -> StackSize,
    StackSize: Into<Option<usize>>,
{
    fn spawn(&mut self, thread: Thread) -> thread::JoinHandle<()> {
        let mut b = thread::Builder::new();

        if let Some(size) = (self.size)(thread.index()).into() {
            b = b.stack_size(size);
        }

        b.spawn(move || thread.run())
            .expect("Invalid thread config")
    }
}

pub struct ThreadPool<C: ?Sized, const COUNT: usize> {
    injector: Injector<Job>,
    init: Once,
    _marker: marker::PhantomPinned,
    // We wrap everything whose mutable access must be synchronised with the
    // `Once` inside a single `UnsafeCell`.
    inner: UnsafeCell<ThreadStartupInfo<C, COUNT>>,
}

struct ThreadStartupInfo<C: ?Sized, const COUNT: usize> {
    stealers: MaybeUninit<[Stealer<Job>; COUNT]>,
    handles: [MaybeUninit<JoinHandle<()>>; COUNT],
    config: C,
}

// The `UnsafeCell`s are only written to when synchronised with `Once`, so
// we can impl `Send + Sync`.
unsafe impl<Config: ?Sized + Send, const COUNT: usize> Send for ThreadPool<Config, COUNT> {}
unsafe impl<Config: ?Sized + Sync, const COUNT: usize> Sync for ThreadPool<Config, COUNT> {}

fn do_work_with_worker(
    worker: &Worker<Job>,
    mut stealers: &[Stealer<Job>],
    injector: &Injector<Job>,
) -> ControlFlow<()> {
    use crossbeam_deque::Steal;

    // Execute jobs
    if let Some(job) = worker.pop() {
        unsafe { job.callback.as_ref()(job.data) }
    } else {
        // Get new jobs
        'steal_jobs: loop {
            match injector.steal_batch(worker) {
                Steal::Success(()) => break 'steal_jobs,
                Steal::Retry => continue 'steal_jobs,
                Steal::Empty => {
                    while let Some((first, rest)) = stealers.split_first() {
                        match first.steal_batch(worker) {
                            Steal::Success(()) => break 'steal_jobs,
                            Steal::Retry => {}
                            Steal::Empty => {
                                stealers = rest;
                            }
                        }
                    }
                }
            }
        }

        ControlFlow::Continue(())
    }
}

fn start_thread<C: ?Sized + ThreadConfig>(
    id: usize,
    config: &mut C,
    worker: Worker<Job>,
    stealers: &'static [Stealer<Job>],
    injector: &'static Injector<Job>,
) -> JoinHandle<()> {
    config.spawn(Thread {
        id,
        worker,
        stealers,
        injector,
    })
}

impl ThreadPool<Config<(), ()>, 0> {
    // We can't make an uninitialized array of a size dependent on a generic parameter
    // in stable Rust right now, so using `mem::uninitialized` is a workaround
    #[allow(deprecated)]
    // This lint fires incorrectly on `new` functions with const generic params but no
    // type generic params.
    #[allow(clippy::new_without_default)]
    pub fn new<const COUNT: usize>() -> ThreadPool<Config<(), ()>, COUNT> {
        let injector = Injector::new();

        ThreadPool {
            injector,
            init: Once::new(),
            inner: UnsafeCell::new(ThreadStartupInfo {
                stealers: MaybeUninit::uninit(),
                handles: unsafe { mem::uninitialized() },
                config: Config::default(),
            }),
            _marker: marker::PhantomPinned,
        }
    }
}

impl<Namer, Size, const COUNT: usize> ThreadPool<Config<Namer, Size>, COUNT> {
    /// Supply a function which can be used to get the name of created threads
    pub fn with_name<F, O>(self, namer: F) -> ThreadPool<Config<F, Size>, COUNT>
    where
        F: FnMut(usize) -> O,
        O: Into<Option<String>>,
    {
        // Safety: since we take owned self, we know that this cannot be run from multiple threads.
        //         It is also impossible to call this after the call to `start_threads` since you
        //         cannot get a mutable reference out of a `Pin`, but we assert anyway.

        use parking_lot::OnceState;

        assert_eq!(self.init.state(), OnceState::New);

        let this = ManuallyDrop::new(self);

        unsafe {
            let ThreadStartupInfo {
                config: Config { size, namer: _ },
                stealers,
                handles,
            } = ptr::read(&this.inner).into_inner();
            let inner = UnsafeCell::new(ThreadStartupInfo {
                config: Config { namer, size },
                stealers,
                handles,
            });
            let injector = ptr::read(&this.injector);
            let init = ptr::read(&this.init);

            ThreadPool {
                injector,
                init,
                inner,
                _marker: marker::PhantomPinned,
            }
        }
    }

    /// Supply a function which can be used to get the name of created threads
    pub fn with_stack_size<F, O>(self, size: F) -> ThreadPool<Config<Namer, F>, COUNT>
    where
        F: FnMut(usize) -> O,
        O: Into<Option<usize>>,
    {
        // Safety: since we take owned self, we know that this cannot be run from multiple threads.
        //         It is also impossible to call this after the call to `start_threads` since you
        //         cannot get a mutable reference out of a `Pin`, but we assert anyway.

        use parking_lot::OnceState;

        assert_eq!(self.init.state(), OnceState::New);

        let this = ManuallyDrop::new(self);

        unsafe {
            let ThreadStartupInfo {
                config: Config { namer, size: _ },
                stealers,
                handles,
            } = ptr::read(&this.inner).into_inner();
            let inner = UnsafeCell::new(ThreadStartupInfo {
                config: Config { namer, size },
                stealers,
                handles,
            });
            let injector = ptr::read(&this.injector);
            let init = ptr::read(&this.init);

            ThreadPool {
                injector,
                init,
                inner,
                _marker: marker::PhantomPinned,
            }
        }
    }

    /// Supply a function which can be used to get the name of created threads
    pub fn with_thread_config<F, O>(self, config: F) -> ThreadPool<F, COUNT>
    where
        F: FnMut(Thread) -> thread::JoinHandle<()>,
    {
        // Safety: since we take owned self, we know that this cannot be run from multiple threads.
        //         It is also impossible to call this after the call to `start_threads` since you
        //         cannot get an owned `T: !Unpin` out of a `Pin<T>`, but we assert anyway.

        use parking_lot::OnceState;

        assert_eq!(self.init.state(), OnceState::New);

        let this = ManuallyDrop::new(self);

        unsafe {
            let ThreadStartupInfo {
                config: _,
                stealers,
                handles,
            } = ptr::read(&this.inner).into_inner();
            let inner = UnsafeCell::new(ThreadStartupInfo {
                config,
                stealers,
                handles,
            });
            let injector = ptr::read(&this.injector);
            let init = ptr::read(&this.init);

            ThreadPool {
                injector,
                init,
                inner,
                _marker: marker::PhantomPinned,
            }
        }
    }
}

impl<C: ?Sized + ThreadConfig, const COUNT: usize> ThreadPool<C, COUNT> {
    /// Safety: This function must only be called init across the entire lifetime of `self`.
    // We can't make an uninitialized array of a size dependent on a generic parameter
    // in stable Rust right now, so using `mem::uninitialized` is a workaround
    #[allow(deprecated)]
    unsafe fn start_threads(self: Pin<&Self>) {
        let mut workers: [MaybeUninit<Worker<Job>>; COUNT] = mem::uninitialized();

        for worker in &mut workers {
            // We use a LIFO queue so shutdown events are processed as fast as possible.
            worker.write(Worker::new_lifo());
        }

        let workers = workers.map(|worker| unsafe { worker.assume_init() });

        let mut stealers: [MaybeUninit<Stealer<Job>>; COUNT] = mem::uninitialized();

        for (stealer, worker) in stealers.iter_mut().zip(workers.iter()) {
            stealer.write(worker.stealer());
        }

        let stealers = stealers.map(|stealer| unsafe { stealer.assume_init() });

        // This is valid because the invariants of this function ensure that it can only be
        // called a single time globally, synchronised using a `Once`.
        let inner = &mut *self.inner.get();
        let handles: &mut [MaybeUninit<JoinHandle<()>>] = &mut inner.handles[..];

        inner.stealers.write(stealers);

        let injector =
            mem::transmute::<&Injector<Job>, &'static Injector<Job>>(&self.as_ref().injector);
        let stealers = mem::transmute::<&[Stealer<Job>], &'static [Stealer<Job>]>(
            &(*self.as_ref().inner.get()).stealers.assume_init_ref()[..],
        );

        for (i, (worker, handle)) in IntoIterator::into_iter(workers).zip(handles).enumerate() {
            handle.write(start_thread(
                i,
                &mut inner.config,
                worker,
                stealers,
                injector,
            ));
        }
    }

    /// Safety: Can only be called when `self.inner` is initialised
    unsafe fn for_each_unchecked<'data, Data, I, Callback>(
        self: Pin<&Self>,
        iter: I,
        callback: Callback,
    ) where
        Data: Send + 'data,
        I: IntoIterator<Item = Data>,
        Callback: Fn(Data) + Sync + 'data,
    {
        use parking_lot::{Condvar, Mutex};

        let mut iter = ManuallyDrop::new(UnsafeAny::make_iter(iter));

        let remaining_jobs = AtomicUsize::new(0);
        let finished = Condvar::new();
        let err: Mutex<Option<Box<dyn Any + Send + 'static>>> = Mutex::new(None);

        let (remaining_jobs, finished, err) = (&remaining_jobs, &finished, &err);

        let wrapped_callback = move |void_ptr: UnsafeAny| {
            let data = unsafe { void_ptr.into::<Data>() };

            match panic::catch_unwind(panic::AssertUnwindSafe(|| callback(data))) {
                Err(e) => *err.lock() = Some(e),
                Ok(()) => {}
            }

            if remaining_jobs.fetch_sub(1, Ordering::AcqRel) == 1 {
                // This ensures that we only notify once the main thread
                // is actually waiting.
                let _ = err.lock();
                finished.notify_one();
            }

            ControlFlow::Continue(())
        };

        // Take the lock before pushing any jobs to avoid race condition where condvar is
        // notified before waiting.
        let mut err = err.lock();

        let callback_ptr: NonNull<JobCallback<'_>> = NonNull::from(&wrapped_callback);
        // Safety: transmuting trait objects is OK because we're only transmuting lifetimes
        let static_callback_ptr: NonNull<JobCallback<'static>> = mem::transmute(callback_ptr);

        let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            for data in iter.by_ref() {
                remaining_jobs.fetch_add(1, Ordering::AcqRel);
                self.injector.push(Job {
                    data,
                    callback: static_callback_ptr,
                });
            }
        }));

        while remaining_jobs.load(Ordering::Acquire) > 0 {
            finished.wait(&mut err);
        }

        match result {
            Ok(()) => {}
            Err(e) => {
                *err = Some(e);
            }
        }

        if let Some(err) = err.take() {
            panic::resume_unwind(err);
        }

        ManuallyDrop::drop(&mut iter);
    }

    pub fn for_each<'data, Data, I, Callback>(self: Pin<&Self>, iter: I, callback: Callback)
    where
        Data: Send + 'data,
        I: IntoIterator<Item = Data>,
        Callback: Fn(Data) + Sync + 'data,
    {
        unsafe {
            self.init.call_once(|| self.start_threads());

            self.for_each_unchecked(iter, callback)
        }
    }
}

impl<C: ?Sized, const COUNT: usize> Drop for ThreadPool<C, COUNT> {
    fn drop(&mut self) {
        use parking_lot::OnceState;

        let inner_needs_drop = match self.init.state() {
            // If the init is poisoned we may leak some data, but that's
            // better than causing unsoundness.
            OnceState::New => false,
            OnceState::Done => true,
            OnceState::InProgress => {
                unreachable!("Somehow we dropped a threadpool while still running setup code. This should never happen.");
            }
            OnceState::Poisoned => {
                panic!("Thread setup panicked");
            }
        };

        self.injector.push(SHUTDOWN);

        if inner_needs_drop {
            unsafe {
                let handles = ptr::read(&(*self.inner.get()).handles);

                for handle in handles {
                    let _ = handle.assume_init().join();
                }

                let stealers = (*self.inner.get()).stealers.assume_init_mut();
                ptr::drop_in_place(stealers);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Executor, ThreadPool};

    #[test]
    fn runs_each_job_once() {
        use parking_lot::Mutex;

        let pool = Box::pin(ThreadPool::new::<4>());
        let vals = [
            Mutex::new(0),
            Mutex::new(1),
            Mutex::new(2),
            Mutex::new(3),
            Mutex::new(4),
            Mutex::new(5),
            Mutex::new(6),
            Mutex::new(7),
            Mutex::new(8),
            Mutex::new(9),
            Mutex::new(10),
        ];

        pool.for_each(&vals, |val| {
            let mut val = val.lock();

            *val = 10 - *val;
        });

        assert_eq!(vals.map(|v| *v.lock()), [10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]);
    }

    #[test]
    fn runs_each_job_once_mut() {
        let pool = Box::pin(ThreadPool::new::<4>());
        let mut vals = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        pool.for_each(&mut vals, |val| {
            *val = 10 - *val;
        });

        assert_eq!(vals, [10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]);
    }

    #[test]
    fn works_on_panic() {
        let pool = Box::pin(ThreadPool::new::<4>());
        let vals = [()];

        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| pool.for_each(
                &vals,
                |()| {
                    panic!();
                }
            )))
            .is_err()
        );
    }

    #[test]
    fn really_big_array() {
        const COUNT: usize = (std::u16::MAX as usize) * 8;

        let pool = Box::pin(ThreadPool::new::<4>());
        let mut vals = (0..COUNT).collect::<Box<[_]>>();

        pool.for_each(&mut vals[..], |i| *i = (COUNT - 1) - *i);

        let mut expected = (0..COUNT).collect::<Box<[_]>>();
        expected.reverse();

        assert_eq!(vals, expected);
    }

    #[test]
    fn big_values() {
        const COUNT: usize = std::u8::MAX as usize;

        let pool = Box::pin(ThreadPool::new::<4>());
        let mut vals = (0..COUNT).map(|i| [i; COUNT]).collect::<Box<[_]>>();

        pool.for_each(&mut vals[..], |vals| {
            for i in vals {
                *i = (COUNT - 1) - *i;
            }
        });

        let mut expected = (0..COUNT).map(|i| [i; COUNT]).collect::<Box<[_]>>();
        expected.reverse();

        assert_eq!(vals, expected);
    }
}
