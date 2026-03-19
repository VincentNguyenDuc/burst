use std::{collections::HashMap, sync::Arc};

use super::{RouterFactory, RouterStrategy};

#[derive(Default)]
pub struct RouterRegistry {
    factories: HashMap<String, Arc<dyn RouterFactory>>,
}

impl RouterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<F>(&mut self, factory: F)
    where
        F: RouterFactory + 'static,
    {
        self.factories
            .insert(factory.name().to_string(), Arc::new(factory));
    }

    pub fn build(&self, name: &str) -> Option<Box<dyn RouterStrategy>> {
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
    use super::RouterRegistry;
    use crate::router::{RoundRobinFactory, RouterFactory};

    struct AlphaFactory;

    impl RouterFactory for AlphaFactory {
        fn name(&self) -> &'static str {
            "alpha"
        }

        fn build(&self) -> Box<dyn crate::router::RouterStrategy> {
            RoundRobinFactory.build()
        }
    }

    #[test]
    fn build_returns_registered_strategy() {
        let mut registry = RouterRegistry::new();
        registry.register(RoundRobinFactory);

        let strategy = registry.build("roundrobin");

        assert!(strategy.is_some());
        assert_eq!(strategy.expect("strategy missing").name(), "roundrobin");
    }

    #[test]
    fn available_is_sorted() {
        let mut registry = RouterRegistry::new();
        registry.register(RoundRobinFactory);
        registry.register(AlphaFactory);

        let names = registry.available();

        assert_eq!(names, vec!["alpha".to_string(), "roundrobin".to_string()]);
    }
}
