# Bundled MiTeX scope

MiTeX's Rust converter (crate 0.2.4) emits names from its Typst scope, including
`mitexsqrt`, matrix helpers and text/math operators. The parser alone does not
define these names.

These files are from the published Typst packages:

- https://packages.typst.org/preview/mitex-0.2.6.tar.gz

`prelude.typ` is unchanged. `standard.typ` has its prelude import removed: the
two files are concatenated into generated math documents in dependency order.
Scope 0.2.6 supports current Typst (0.2.4 still referenced removed math symbols).
No package download or nested Wasm plugin is needed at
runtime. The upstream MiTeX scope is passed to Typst `eval` exactly as in MiTeX's
`mimath` implementation. Only converted math is evaluated; Markdown text and code
are passed as string data.

MiTeX is Apache-2.0 (`LICENSE`).
