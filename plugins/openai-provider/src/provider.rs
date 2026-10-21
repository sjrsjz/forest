//! Concrete protocol conversion. No context, clipping, tool execution or storage.
use crate::types::*;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

#[derive(Serialize)]
struct Body<'a> {
    model: &'a str,
    messages: &'a [Message],
    stream: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<WireTool<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<WireToolChoice<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_completion_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_effort: Option<&'a str>,
}

#[derive(Serialize)]
struct WireTool<'a> {
    r#type: &'static str,
    function: WireFunction<'a>,
}

#[derive(Serialize)]
struct WireFunction<'a> {
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<&'a str>,
    parameters: Box<RawValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    strict: Option<bool>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum WireToolChoice<'a> {
    Mode(&'static str),
    Function {
        r#type: &'static str,
        function: FunctionName<'a>,
    },
}

#[derive(Serialize)]
struct FunctionName<'a> {
    name: &'a str,
}

pub(crate) fn encode(settings: &Settings, request: &Request) -> Result<Vec<u8>, Error> {
    let tools = request
        .tools
        .iter()
        .map(|tool| {
            let parameters =
                RawValue::from_string(tool.parameters_json.clone()).map_err(|error| {
                    Error::ToolSchema {
                        name: tool.name.clone(),
                        reason: error.to_string(),
                    }
                })?;
            Ok(WireTool {
                r#type: "function",
                function: WireFunction {
                    name: &tool.name,
                    description: tool.description.as_deref(),
                    parameters,
                    strict: tool.strict,
                },
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let tool_choice = request.tool_choice.as_ref().map(|choice| match choice {
        ToolChoice::Auto => WireToolChoice::Mode("auto"),
        ToolChoice::None => WireToolChoice::Mode("none"),
        ToolChoice::Required => WireToolChoice::Mode("required"),
        ToolChoice::Function(name) => WireToolChoice::Function {
            r#type: "function",
            function: FunctionName { name },
        },
    });
    let (max_completion_tokens, max_tokens) = match settings.output_limit {
        OutputLimit::ProviderDefault => (None, None),
        OutputLimit::CompletionTokens(value) => (Some(value), None),
        OutputLimit::Tokens(value) => (None, Some(value)),
    };
    let body = Body {
        model: &settings.model,
        messages: &request.messages,
        stream: false,
        tools,
        tool_choice,
        max_completion_tokens,
        max_tokens,
        reasoning_effort: settings.reasoning_effort.as_deref(),
    };
    Ok(serde_json::to_vec(&body).expect("typed protocol fields are JSON serializable"))
}

#[derive(Deserialize)]
struct WireCompletion {
    id: String,
    model: String,
    created: i64,
    choices: Vec<WireChoice>,
    usage: Option<Usage>,
}

#[derive(Deserialize)]
struct WireChoice {
    index: i64,
    message: Message,
    finish_reason: FinishReason,
}

pub(crate) fn decode(bytes: &[u8]) -> Result<Completion, Error> {
    let response: WireCompletion =
        serde_json::from_slice(bytes).map_err(|error| Error::Response(error.to_string()))?;
    let choices = response
        .choices
        .into_iter()
        .map(|choice| {
            let Message::Assistant(message) = choice.message else {
                return Err(Error::Response(
                    "completion choice must contain an assistant message".into(),
                ));
            };
            Ok(Choice {
                index: choice.index,
                message,
                finish_reason: choice.finish_reason,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Ok(Completion {
        id: response.id,
        model: response.model,
        created: response.created,
        choices,
        usage: response.usage,
    })
}
