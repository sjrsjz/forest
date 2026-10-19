use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, Index, parse_macro_input};

pub fn derive_value(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let mut where_clause = where_clause
        .cloned()
        .unwrap_or_else(|| syn::parse_quote! { where });
    for param in input.generics.type_params() {
        let ident = &param.ident;
        where_clause
            .predicates
            .push(syn::parse_quote! { #ident: ::forest_serde::adt::AsValue });
    }

    let serialize_body = match &input.data {
        Data::Struct(s) => generate_struct_serialize_body(&s.fields),
        Data::Enum(e) => generate_enum_serialize_body(e),
        Data::Union(_) => {
            return syn::Error::new_spanned(
                input,
                "Unions are not supported and cannot derive AsValue",
            )
            .to_compile_error()
            .into();
        }
    };

    let deserialize_body = match &input.data {
        Data::Struct(s) => generate_struct_deserialize_body(&s.fields),
        Data::Enum(e) => generate_enum_deserialize_body(e),
        Data::Union(_) => unreachable!(),
    };

    let expanded = quote! {
        impl #impl_generics ::forest_serde::adt::AsValue for #name #ty_generics #where_clause {
            fn as_value(&self) -> ::forest_serde::adt::Value {
                #serialize_body
            }

            fn from_value(value: ::forest_serde::adt::Value) -> Result<Self, ::forest_serde::adt::DecodeError> {
                #deserialize_body
            }
        }
    };

    TokenStream::from(expanded)
}

fn generate_struct_serialize_body(fields: &Fields) -> TokenStream2 {
    match fields {
        Fields::Named(fields) => {
            let field_inserters = fields.named.iter().map(|f| {
                let field_name = f.ident.as_ref().unwrap().to_string();
                let field_ident = f.ident.as_ref().unwrap();
                let field_ty = &f.ty;
                quote! {
                    map.insert(#field_name.to_string(), <#field_ty as ::forest_serde::adt::AsValue>::as_value(&self.#field_ident));
                }
            });
            quote! {
                {
                    let mut map = ::std::collections::BTreeMap::new();
                    #(#field_inserters)*
                    ::forest_serde::adt::Value::Record(map)
                }
            }
        }
        Fields::Unnamed(fields) => {
            let field_serializers = fields.unnamed.iter().enumerate().map(|(i, f)| {
                let index = Index::from(i);
                let field_ty = &f.ty;
                quote! { <#field_ty as ::forest_serde::adt::AsValue>::as_value(&self.#index) }
            });
            quote! {
                ::forest_serde::adt::Value::Tuple(vec![#(#field_serializers),*])
            }
        }
        Fields::Unit => {
            quote! { ::forest_serde::adt::Value::Record(::std::collections::BTreeMap::new()) }
        }
    }
}

fn generate_enum_serialize_body(e: &syn::DataEnum) -> TokenStream2 {
    if e.variants.is_empty() {
        return quote! { match *self {} };
    }
    let match_arms = e.variants.iter().map(|v| {
        let variant_ident = &v.ident;
        let variant_name_str = v.ident.to_string();
        match &v.fields {
            Fields::Named(fields) => {
                let field_idents: Vec<_> = fields.named.iter().map(|f| f.ident.as_ref().unwrap()).collect();
                let field_inserters = fields.named.iter().zip(field_idents.iter()).map(|(f, ident)| {
                    let ident_str = ident.to_string();
                    let field_ty = &f.ty;
                    quote! {
                        map.insert(#ident_str.to_string(), <#field_ty as ::forest_serde::adt::AsValue>::as_value(#ident));
                    }
                });
                quote! {
                    Self::#variant_ident { #(#field_idents),* } => {
                        let mut map = ::std::collections::BTreeMap::new();
                        #(#field_inserters)*
                        ::forest_serde::adt::Value::Variant(
                            #variant_name_str.to_string(),
                            ::forest_serde::adt::VariantPayloadValue::Named(map)
                        )
                    }
                }
            }
            Fields::Unnamed(fields) => {
                let field_bindings: Vec<_> = (0..fields.unnamed.len()).map(|i| format_ident!("p{}", i)).collect();
                let field_serializers = fields.unnamed.iter().zip(field_bindings.iter()).map(|(f, binding)| {
                    let field_ty = &f.ty;
                    quote! { <#field_ty as ::forest_serde::adt::AsValue>::as_value(#binding) }
                });
                quote! {
                    Self::#variant_ident(#(#field_bindings),*) => {
                        ::forest_serde::adt::Value::Variant(
                            #variant_name_str.to_string(),
                            ::forest_serde::adt::VariantPayloadValue::Unnamed(vec![#(#field_serializers),*])
                        )
                    }
                }
            }
            Fields::Unit => {
                quote! {
                    Self::#variant_ident => {
                        ::forest_serde::adt::Value::Variant(
                            #variant_name_str.to_string(),
                            ::forest_serde::adt::VariantPayloadValue::Unnamed(vec![])
                        )
                    }
                }
            }
        }
    });

    quote! { match self { #(#match_arms),* } }
}

fn generate_struct_deserialize_body(fields: &Fields) -> TokenStream2 {
    match fields {
        Fields::Named(fields) => {
            if fields.named.is_empty() {
                return quote! {
                    match value {
                        ::forest_serde::adt::Value::Record(_) | ::forest_serde::adt::Value::Tuple(_) => Ok(Self {}),
                        _ => Err(::forest_serde::adt::DecodeError::expected(::forest_serde::adt::ValueKind::Record, &value)),
                    }
                };
            }
            let field_extractors = fields.named.iter().map(|f| {
                let field_name_str = f.ident.as_ref().unwrap().to_string();
                let field_ident = f.ident.as_ref().unwrap();
                let field_ty = &f.ty;
                quote! {
                    let #field_ident = {
                        let val = map.remove(#field_name_str)
                            .ok_or_else(|| ::forest_serde::adt::DecodeError::MissingField { field: #field_name_str.into() })?;
                        <#field_ty as ::forest_serde::adt::AsValue>::from_value(val)
                            .map_err(|error| error.at_field(#field_name_str))?
                    };
                }
            });
            let field_idents = fields.named.iter().map(|f| f.ident.as_ref().unwrap());
            quote! {
                match value {
                    ::forest_serde::adt::Value::Record(mut map) => {
                        #(#field_extractors)*
                        Ok(Self { #(#field_idents),* })
                    }
                    _ => Err(::forest_serde::adt::DecodeError::expected(::forest_serde::adt::ValueKind::Record, &value)),
                }
            }
        }
        Fields::Unnamed(fields) => {
            let field_count = fields.unnamed.len();
            if field_count == 0 {
                return quote! {
                    match value {
                        ::forest_serde::adt::Value::Tuple(_) | ::forest_serde::adt::Value::Record(_) => Ok(Self()),
                        _ => Err(::forest_serde::adt::DecodeError::expected(::forest_serde::adt::ValueKind::Tuple, &value)),
                    }
                };
            }
            let field_deserializers = fields.unnamed.iter().enumerate().map(|(index, f)| {
                let field_ty = &f.ty;
                quote! { <#field_ty as ::forest_serde::adt::AsValue>::from_value(values.remove(0)).map_err(|error| error.at_index(#index))? }
            });
            quote! {
                match value {
                    ::forest_serde::adt::Value::Tuple(mut values) => {
                        if values.len() < #field_count {
                            return Err(::forest_serde::adt::DecodeError::TooFewElements { minimum: #field_count, actual: values.len() });
                        }
                        Ok(Self(#(#field_deserializers,)*))
                    }
                    _ => Err(::forest_serde::adt::DecodeError::expected(::forest_serde::adt::ValueKind::Tuple, &value)),
                }
            }
        }
        Fields::Unit => {
            quote! {
                match value {
                    ::forest_serde::adt::Value::Record(_) | ::forest_serde::adt::Value::Tuple(_) => Ok(Self),
                    _ => Err(::forest_serde::adt::DecodeError::expected(::forest_serde::adt::ValueKind::Record, &value)),
                }
            }
        }
    }
}

fn generate_enum_deserialize_body(e: &syn::DataEnum) -> TokenStream2 {
    let tag_match_arms = e.variants.iter().map(|v| {
        let variant_ident = &v.ident;
        let variant_name_str = v.ident.to_string();
        let inner_deserializer = match &v.fields {
            Fields::Unit => {
                quote! {
                    Ok(Self::#variant_ident)
                }
            }
            Fields::Named(fields) => {
                if fields.named.is_empty() {
                    return quote! { #variant_name_str => { Ok(Self::#variant_ident {}) } };
                }
                let field_extractors = fields.named.iter().map(|f| {
                    let field_name_str = f.ident.as_ref().unwrap().to_string();
                    let field_ident = f.ident.as_ref().unwrap();
                    let field_ty = &f.ty;
                    quote! {
                        let #field_ident = {
                            let val = map.remove(#field_name_str)
                                .ok_or_else(|| ::forest_serde::adt::DecodeError::MissingField { field: #field_name_str.into() })?;
                            <#field_ty as ::forest_serde::adt::AsValue>::from_value(val)
                                .map_err(|error| error.at_field(#field_name_str))?
                        };
                    }
                });
                let field_idents = fields.named.iter().map(|f| f.ident.as_ref().unwrap());
                quote! {
                    match payload {
                        ::forest_serde::adt::VariantPayloadValue::Named(mut map) => {
                            #(#field_extractors)*
                            Ok(Self::#variant_ident { #(#field_idents),* })
                        }
                        _ => Err(::forest_serde::adt::DecodeError::PayloadMismatch {
                            expected: ::forest_serde::adt::PayloadKind::Named,
                            actual: payload.kind(),
                        }),
                    }
                }
            }
            Fields::Unnamed(fields) => {
                let field_count = fields.unnamed.len();
                if field_count == 0 {
                    return quote! { #variant_name_str => { Ok(Self::#variant_ident()) } };
                }
                let field_deserializers = fields.unnamed.iter().enumerate().map(|(index, f)| {
                    let field_ty = &f.ty;
                    quote! { <#field_ty as ::forest_serde::adt::AsValue>::from_value(values.remove(0)).map_err(|error| error.at_index(#index))? }
                });
                quote! {
                    match payload {
                        ::forest_serde::adt::VariantPayloadValue::Unnamed(mut values) => {
                            if values.len() < #field_count {
                                return Err(::forest_serde::adt::DecodeError::TooFewElements { minimum: #field_count, actual: values.len() });
                            }
                            Ok(Self::#variant_ident(#(#field_deserializers,)*))
                        }
                        _ => Err(::forest_serde::adt::DecodeError::PayloadMismatch {
                            expected: ::forest_serde::adt::PayloadKind::Unnamed,
                            actual: payload.kind(),
                        }),
                    }
                }
            }
        };

        quote! { #variant_name_str => { #inner_deserializer } }
    });

    quote! {
        match value {
            ::forest_serde::adt::Value::Variant(tag, payload) => {
                match tag.as_str() {
                    #(#tag_match_arms),*
                    _ => Err(::forest_serde::adt::DecodeError::UnknownVariant { tag }),
                }
            }
            _ => Err(::forest_serde::adt::DecodeError::expected(::forest_serde::adt::ValueKind::Variant, &value)),
        }
    }
}

pub(crate) fn impl_as_value_for_tuples(input: TokenStream) -> TokenStream {
    let lit_int = match syn::parse::<syn::LitInt>(input) {
        Ok(li) => li,
        Err(e) => return e.to_compile_error().into(),
    };
    let max_arity = match lit_int.base10_parse::<usize>() {
        Ok(n) => n,
        Err(e) => return e.to_compile_error().into(),
    };

    let mut all_impls = TokenStream2::new();
    for i in 1..=max_arity {
        let generics: Vec<_> = (0..i).map(|j| format_ident!("T{}", j)).collect();
        let trait_bounds = generics
            .iter()
            .map(|g| quote! { #g: crate::adt::AsValue + 'static });
        let tuple_type = quote! { ( #(#generics,)* ) };
        let to_value_fields = generics.iter().enumerate().map(|(j, g)| {
            let index = syn::Index::from(j);
            quote! { <#g as crate::adt::AsValue>::as_value(&self.#index) }
        });
        let from_value_fields = generics.iter().enumerate().map(|(index, g)| {
            quote! { <#g as crate::adt::AsValue>::from_value(values.remove(0)).map_err(|error| error.at_index(#index))? }
        });

        let impl_block = quote! {
            #[allow(non_snake_case)]
            impl<#(#trait_bounds),*> crate::adt::AsValue for #tuple_type {
                fn as_value(&self) -> crate::adt::Value {
                    crate::adt::Value::Tuple(vec![#(#to_value_fields),*])
                }
                fn from_value(value: crate::adt::Value) -> Result<Self, crate::adt::DecodeError> {
                    match value {
                        crate::adt::Value::Tuple(mut values) => {
                            if values.len() < #i {
                                return Err(crate::adt::DecodeError::TooFewElements { minimum: #i, actual: values.len() });
                            }
                            Ok(( #(#from_value_fields,)* ))
                        }
                        _ => Err(crate::adt::DecodeError::expected(crate::adt::ValueKind::Tuple, &value)),
                    }
                }
            }
        };
        all_impls.extend(impl_block);
    }
    all_impls.into()
}
