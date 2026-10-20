use forest_protocol::{
    AccessPolicy, AsSchema, AsValue, CallContext, InterfaceDeclaration, Invocation, Token, Value,
};
use std::{collections::BTreeMap, future::Future, pin::Pin, rc::Rc};

pub type HandlerFuture = Pin<Box<dyn Future<Output = Value>>>;
type Handler<M> = Box<dyn Fn(Rc<M>, CallContext, Value) -> HandlerFuture>;
struct Route<M> {
    declaration: InterfaceDeclaration,
    handler: Handler<M>,
}

/// A typed local dispatch table. The SDK erases a concrete handler only after
/// deriving its exact input and Result output schemas at registration.
pub struct Handlers<M> {
    routes: BTreeMap<Token, Route<M>>,
}
impl<M: 'static> Default for Handlers<M> {
    fn default() -> Self {
        Self::new()
    }
}
impl<M: 'static> Handlers<M> {
    pub fn new() -> Self {
        Self {
            routes: BTreeMap::new(),
        }
    }
    pub fn with<I, T, E, F, Fut>(
        mut self,
        path: Token,
        reentrant: bool,
        access: AccessPolicy,
        handler: F,
    ) -> Self
    where
        I: AsSchema + AsValue,
        T: AsSchema + AsValue,
        E: AsSchema + AsValue,
        F: Fn(Rc<M>, CallContext, I) -> Fut + 'static,
        Fut: Future<Output = Result<T, E>> + 'static,
    {
        let declaration = InterfaceDeclaration {
            path: path.clone(),
            input: I::as_schema(),
            output: <Result<T, E>>::as_schema(),
            reentrant,
            access,
        };
        let handler = Box::new(move |module, context, input| -> HandlerFuture {
            let input = I::from_value(input).expect("validated handler input");
            let future = handler(module, context, input);
            Box::pin(async move { future.await.as_value() })
        });
        assert!(
            self.routes
                .insert(
                    path,
                    Route {
                        declaration,
                        handler
                    }
                )
                .is_none(),
            "duplicate handler path"
        );
        self
    }
    pub fn declarations(&self) -> Vec<InterfaceDeclaration> {
        self.routes
            .values()
            .map(|route| route.declaration.clone())
            .collect()
    }
    pub(crate) fn invoke(&self, module: Rc<M>, invocation: Invocation) -> HandlerFuture {
        let route = self
            .routes
            .get(&invocation.interface)
            .expect("manifest and SDK handlers disagree");
        (route.handler)(module, invocation.context, invocation.input)
    }
}
