use std::{collections::HashMap, sync::Arc};

use super::{SchedulerFactory, SchedulerStrategy};

#[derive(Default)]
pub struct SchedulerRegistry {
    factories: HashMap<String, Arc<dyn SchedulerFactory>>,
}

impl SchedulerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<F>(&mut self, factory: F)
    where
        F: SchedulerFactory + 'static,
    {
        self.factories
            .insert(factory.name().to_string(), Arc::new(factory));
    }

    pub fn build(&self, name: &str) -> Option<Box<dyn SchedulerStrategy>> {
        self.factories.get(name).map(|factory| factory.build())
    }

    pub fn available(&self) -> Vec<String> {
        let mut names = self.factories.keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }
}
