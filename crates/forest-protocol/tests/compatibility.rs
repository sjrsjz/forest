use forest_protocol::{AsSchema, InterfaceDeclaration, manifest::callers};

fn interface<I: AsSchema, T: AsSchema, E: AsSchema>() -> InterfaceDeclaration {
    InterfaceDeclaration {
        path: "/example".parse().unwrap(),
        input: I::as_schema(),
        output: <Result<T, E>>::as_schema(),
        reentrant: false,
        access: callers(vec![]),
    }
}

#[test]
fn function_subtyping_is_contravariant_in_input_and_covariant_in_both_result_branches() {
    let requirement = interface::<i32, i64, i64>();
    assert!(interface::<i64, i32, i32>().satisfies(&requirement.input, &requirement.output));
    let widened_input = interface::<i64, i64, i64>();
    assert!(!requirement.satisfies(&widened_input.input, &widened_input.output));
    let narrowed_output = interface::<i32, i32, i32>();
    assert!(
        !interface::<i64, i64, i32>().satisfies(&narrowed_output.input, &narrowed_output.output)
    );
    assert!(
        !interface::<i64, i32, i64>().satisfies(&narrowed_output.input, &narrowed_output.output)
    );

    let prefix = interface::<(i32, String), (i64,), String>();
    assert!(interface::<(i64,), (i32, String), String>().satisfies(&prefix.input, &prefix.output));
}
