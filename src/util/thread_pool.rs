use std::sync::{Arc, Mutex};
use std::thread;

pub struct SimpleThreadPool {
    workers: Vec<Worker>,
    tasks: Arc<Mutex<Vec<Box<dyn FnOnce() + Send + 'static>>>>,
}

struct Worker {
    handle: Option<thread::JoinHandle<()>>,
}

impl SimpleThreadPool {
    pub fn new(size: usize) -> Self {
        let tasks = Arc::new(Mutex::new(vec![]));
        let mut workers = Vec::with_capacity(size);

        for _ in 0..size {
            let tasks_clone = Arc::clone(&tasks);
            workers.push(Worker {
                handle: Some(thread::spawn(move || loop {
                    let task: Option<Box<dyn FnOnce() + Send>> = {
                        let mut tasks = tasks_clone.lock().unwrap();
                        tasks.pop()
                    };

                    if let Some(task) = task {
                        task();
                    } else {
                        break;
                    }
                })),
            });
        }

        SimpleThreadPool { workers, tasks }
    }

    pub fn execute<F>(&self, task: F)
        where
            F: FnOnce() + Send + 'static,
    {
        let mut tasks = self.tasks.lock().unwrap();
        tasks.push(Box::new(task));
    }

    pub fn join(self) {
        for worker in self.workers {
            if let Some(handle) = worker.handle {
                handle.join().unwrap();
            }
        }
    }
}