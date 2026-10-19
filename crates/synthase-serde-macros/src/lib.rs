use proc_macro::TokenStream;
mod schema;
mod value;

/// Derive the `AsSchema` trait for a struct or enum.
/// This macro generates code to produce a schema representation of the type,
/// handling recursive types by using a cache based on `TypeId`.
#[proc_macro_derive(Schema)]
pub fn derive_schema(input: TokenStream) -> TokenStream {
    schema::derive_schema(input)
}

/// Derive the `AsValue` trait for a struct or enum.
/// This macro generates code to convert the type to and from a generic `Value` representation,
/// handling recursive types appropriately.
#[proc_macro_derive(Value)]
pub fn derive_value(input: TokenStream) -> TokenStream {
    value::derive_value(input)
}

#[doc(hidden)]
#[proc_macro]
pub fn __impl_as_schema_for_tuples(input: TokenStream) -> TokenStream {
    schema::impl_as_schema_for_tuples(input)
}

#[doc(hidden)]
#[proc_macro]
pub fn __impl_as_value_for_tuples(input: TokenStream) -> TokenStream {
    value::impl_as_value_for_tuples(input)
}
