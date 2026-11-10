use forest_protocol::{AsSchema, InterfaceDeclaration, manifest::callers};

#[test]
fn framework_errors_keep_their_variants_and_token_payloads_as_forest_values() {
    use forest_protocol::{AsValue, DispatchError, service::ServiceError};
    for error in [
        DispatchError::ObjectNotFound("/module/missing".parse().unwrap()),
        DispatchError::InterfaceNotFound("/some/interface".parse().unwrap()),
        DispatchError::Unavailable("/module/stopped".parse().unwrap()),
        DispatchError::Denied,
        DispatchError::LifecycleRestricted,
        DispatchError::QueueFull,
        DispatchError::InputContract,
        DispatchError::OutputContract,
        DispatchError::ExecutionFailed("runtime diagnostic".into()),
        DispatchError::Interrupted,
        DispatchError::HostStopped,
        DispatchError::FrameTooLarge,
    ] {
        let value = error.as_value();
        assert!(value.check(&DispatchError::as_schema(), false));
        assert_eq!(DispatchError::from_value(value).unwrap(), error);
    }
    for error in [
        ServiceError::InvalidInput("bad argument".into()),
        ServiceError::NotFound,
        ServiceError::InUse,
        ServiceError::Transport("transport diagnostic".into()),
        ServiceError::Storage("storage diagnostic".into()),
    ] {
        let value = error.as_value();
        assert!(value.check(&ServiceError::as_schema(), false));
        assert_eq!(ServiceError::from_value(value).unwrap(), error);
    }
    assert!(forest_protocol::Token::from_value("relative/path".to_owned().as_value()).is_err());
}

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
