use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, parse_macro_input};

pub(crate) fn derive_schema(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let mut generics = input.generics.clone();
    for param in generics.type_params_mut() {
        param
            .bounds
            .push(syn::parse_quote!(::synthase_serde::adt::AsSchema));
    }
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let schema_body = match &input.data {
        Data::Struct(s) => generate_struct_body_schema(&s.fields),
        Data::Enum(e) => generate_enum_body_schema(e),
        Data::Union(_) => {
            return syn::Error::new_spanned(
                input,
                "Unions are not supported and cannot derive AsSchema",
            )
            .to_compile_error()
            .into();
        }
    };

    let expanded = quote! {
        impl #impl_generics ::synthase_serde::adt::AsSchema for #name #ty_generics #where_clause {
            fn register_dependencies_and_self(
                cache: &mut ::std::collections::HashMap<::std::any::TypeId, ::synthase_serde::adt::GlobalSchemaEnum>,
                rec_detector: &mut Vec<::std::any::TypeId>
            ) -> ::synthase_serde::adt::GlobalSchemaEnum {
                if rec_detector.contains(&::std::any::TypeId::of::<Self>()) {
                    return ::synthase_serde::adt::GlobalSchemaEnum::Ref(::std::any::TypeId::of::<Self>());
                }
                rec_detector.push(::std::any::TypeId::of::<Self>());
                let schema_definition = #schema_body;
                rec_detector.pop();
                ::synthase_serde::adt::GlobalSchemaEnum::alloc::<Self>(schema_definition.clone(), cache);
                schema_definition
            }
        }
    };

    TokenStream::from(expanded)
}

fn generate_struct_body_schema(fields: &Fields) -> TokenStream2 {
    match fields {
        Fields::Named(fields) => {
            let field_initializers = fields.named.iter().map(|f| {
                let field_name = f.ident.as_ref().unwrap().to_string();
                let field_ty = &f.ty;
                quote! {
                    (
                        #field_name.to_string(),
                        <#field_ty as ::synthase_serde::adt::AsSchema>::register_dependencies_and_self(cache, rec_detector)
                    )
                }
            });
            quote! {
                ::synthase_serde::adt::GlobalSchemaEnum::Record(
                    [#(#field_initializers),*].into_iter().collect()
                )
            }
        }
        Fields::Unnamed(fields) => {
            let inner_schemas = fields.unnamed.iter().map(|f| {
                let ty = &f.ty;
                quote! {
                    <#ty as ::synthase_serde::adt::AsSchema>::register_dependencies_and_self(cache, rec_detector)
                }
            });
            quote! {
                ::synthase_serde::adt::GlobalSchemaEnum::Tuple(vec![#(#inner_schemas),*])
            }
        }
        Fields::Unit => {
            quote! {
                ::synthase_serde::adt::GlobalSchemaEnum::Record(::std::collections::BTreeMap::new())
            }
        }
    }
}

fn generate_enum_body_schema(e: &syn::DataEnum) -> TokenStream2 {
    let variant_initializers = e.variants.iter().map(|v| {
        let variant_name = v.ident.to_string();
        let payload_schema = match &v.fields {
            Fields::Unit => {
                quote! { ::synthase_serde::adt::VariantPayloadGlobal::Unnamed(vec![]) }
            }
            Fields::Unnamed(fields) => {
                let inner_schemas = fields.unnamed.iter().map(|f| {
                    let ty = &f.ty;
                    quote! { <#ty as ::synthase_serde::adt::AsSchema>::register_dependencies_and_self(cache, rec_detector) }
                });
                quote! {
                    ::synthase_serde::adt::VariantPayloadGlobal::Unnamed(vec![#(#inner_schemas),*])
                }
            }
            Fields::Named(fields) => {
                let field_initializers = fields.named.iter().map(|f| {
                    let field_name = f.ident.as_ref().unwrap().to_string();
                    let field_ty = &f.ty;
                    quote! {
                        (
                            #field_name.to_string(),
                            <#field_ty as ::synthase_serde::adt::AsSchema>::register_dependencies_and_self(cache, rec_detector)
                        )
                    }
                });
                quote! {
                    ::synthase_serde::adt::VariantPayloadGlobal::Named(
                        [#(#field_initializers),*].into_iter().collect()
                    )
                }
            }
        };
        quote! { (#variant_name.to_string(), #payload_schema) }
    });

    quote! {
        ::synthase_serde::adt::GlobalSchemaEnum::Variant(
            [#(#variant_initializers),*].into_iter().collect()
        )
    }
}

pub(crate) fn impl_as_schema_for_tuples(input: TokenStream) -> TokenStream {
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
            .map(|g| quote! { #g: crate::adt::AsSchema + 'static });
        let tuple_type = quote! { ( #(#generics,)* ) };
        let register_deps = generics.iter().map(|g| {
            quote! { <#g as crate::adt::AsSchema>::register_dependencies_and_self(cache, rec_detector); }
        });
        let inner_refs = generics.iter().map(|g| {
            quote! { crate::adt::GlobalSchemaEnum::Ref(::std::any::TypeId::of::<#g>()) }
        });

        let impl_block = quote! {
            #[allow(non_snake_case)]
            impl<#(#trait_bounds),*> crate::adt::AsSchema for #tuple_type {
                fn register_dependencies_and_self(
                    cache: &mut ::std::collections::HashMap<::std::any::TypeId, crate::adt::GlobalSchemaEnum>,
                    rec_detector: &mut Vec<::std::any::TypeId>,
                ) -> crate::adt::GlobalSchemaEnum {
                    if rec_detector.contains(&::std::any::TypeId::of::<Self>()) {
                        return crate::adt::GlobalSchemaEnum::Ref(::std::any::TypeId::of::<Self>());
                    }
                    rec_detector.push(::std::any::TypeId::of::<Self>());
                    #(#register_deps)*
                    let inner_schemas = vec![#(#inner_refs),*];
                    rec_detector.pop();
                    let schema = crate::adt::GlobalSchemaEnum::Tuple(inner_schemas);
                    crate::adt::GlobalSchemaEnum::alloc::<Self>(schema.clone(), cache);
                    schema
                }
            }
        };
        all_impls.extend(impl_block);
    }
    all_impls.into()
}
