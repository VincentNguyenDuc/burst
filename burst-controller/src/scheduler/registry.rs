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

#[cfg(test)]
mod tests {
    use super::SchedulerRegistry;
    use crate::scheduler::{FifoFactory, SchedulerFactory};

    struct AlphaFactory;

    impl SchedulerFactory for AlphaFactory {
        fn name(&self) -> &'static str {
            "alpha"
        }

        fn build(&self) -> Box<dyn crate::scheduler::SchedulerStrategy> {
            FifoFactory.build()
        }
    }

    #[test]
    fn build_returns_registered_strategy() {
        let mut registry = SchedulerRegistry::new();
        registry.register(FifoFactory);

        let strategy = registry.build("fifo");

        assert!(strategy.is_some());
        assert_eq!(strategy.expect("strategy missing").name(), "fifo");
    }

    #[test]
    fn available_is_sorted() {
        let mut registry = SchedulerRegistry::new();
        registry.register(FifoFactory);
        registry.register(AlphaFactory);

        let names = registry.available();

        assert_eq!(names, vec!["alpha".to_string(), "fifo".to_string()]);
    }
}
