#![cfg(feature = "http")]

use forest_onebot::{
    CallError, Client, Outcome,
    action::*,
    http::{HttpError, HttpTransport},
    message::{Message, Segment},
    response::SentMessage,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

fn direct(endpoint: reqwest::Url, token: Option<&str>) -> HttpTransport {
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()
        .unwrap();
    HttpTransport::with_client(endpoint, token, client).unwrap()
}

async fn request(stream: &mut TcpStream) -> (String, Vec<u8>) {
    let mut bytes = Vec::new();
    let mut chunk = [0; 1024];
    let (header_end, content_length) = loop {
        let count = stream.read(&mut chunk).await.unwrap();
        assert_ne!(count, 0, "connection closed before request headers");
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            let headers = std::str::from_utf8(&bytes[..end]).unwrap();
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            break (end + 4, length);
        }
    };
    while bytes.len() < header_end + content_length {
        let count = stream.read(&mut chunk).await.unwrap();
        assert_ne!(count, 0, "connection closed before request body");
        bytes.extend_from_slice(&chunk[..count]);
    }
    (
        String::from_utf8(bytes[..header_end].to_vec()).unwrap(),
        bytes[header_end..].to_vec(),
    )
}

async fn respond(stream: &mut TcpStream, status: &str, body: &[u8]) {
    let headers = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(headers.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    stream.shutdown().await.unwrap();
}

#[tokio::test]
async fn json_post_uses_prefix_and_bearer_and_returns_typed_receipt() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/prefix/", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let client = Client::new(direct(endpoint, Some("local-test-token")));
    let action = SendGroupMsg {
        group_id: 12,
        message: Message::Segments(vec![Segment::text("[CQ:at,qq=all]&")]),
        auto_escape: None,
    };
    let server = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let (headers, body) = request(&mut stream).await;
        assert!(headers.starts_with("POST /prefix/send_group_msg HTTP/1.1\r\n"));
        let headers = headers.to_ascii_lowercase();
        assert!(headers.contains("content-type: application/json\r\n"));
        assert!(headers.contains("authorization: bearer local-test-token\r\n"));
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            body,
            serde_json::json!({"group_id":12,"message":[{"type":"text","data":{"text":"[CQ:at,qq=all]&"}}]})
        );
        respond(
            &mut stream,
            "200 OK",
            br#"{"status":"ok","retcode":0,"data":{"message_id":-17}}"#,
        )
        .await;
    };
    let (result, ()) = tokio::join!(client.call(&action), server);
    assert_eq!(
        result.unwrap(),
        Outcome::Completed(SentMessage { message_id: -17 })
    );
}

#[tokio::test]
async fn concurrent_requests_are_not_serialized_or_mixed_by_return_order() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let client = Client::new(direct(endpoint, None));
    let server = async {
        let (mut first, _) = listener.accept().await.unwrap();
        let first_headers = request(&mut first).await.0;
        // First response is withheld until the second request has arrived.
        let (mut second, _) = listener.accept().await.unwrap();
        let second_headers = request(&mut second).await.0;
        assert_ne!(first_headers.lines().next(), second_headers.lines().next());
        for (stream, headers) in [(&mut second, second_headers), (&mut first, first_headers)] {
            let body: &[u8] = if headers.starts_with("POST /get_login_info ") {
                br#"{"status":"ok","retcode":0,"data":{"user_id":34,"nickname":"example"}}"#
            } else {
                assert!(headers.starts_with("POST /get_group_list "));
                br#"{"status":"ok","retcode":0,"data":[{"group_id":12,"group_name":"example","member_count":1,"max_member_count":2}]}"#
            };
            respond(stream, "200 OK", body).await;
        }
    };
    let (login, groups, ()) = tokio::join!(
        client.call(&GetLoginInfo {}),
        client.call(&GetGroupList {}),
        server
    );
    assert!(matches!(login.unwrap(), Outcome::Completed(info) if info.user_id == 34));
    assert!(matches!(groups.unwrap(), Outcome::Completed(groups) if groups[0].group_id == 12));
}

#[tokio::test]
async fn http_decode_and_connection_errors_return_to_the_caller() {
    for (status, body) in [
        ("403 Forbidden", b"denied".as_slice()),
        (
            "302 Found\r\nLocation: http://127.0.0.1:1/unused",
            b"redirect",
        ),
        ("200 OK", b"invalid-json"),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let client = Client::new(direct(endpoint, None));
        let server = async {
            let (mut stream, _) = listener.accept().await.unwrap();
            request(&mut stream).await;
            respond(&mut stream, status, body).await;
        };
        let (result, ()) = tokio::join!(client.call(&GetLoginInfo {}), server);
        if status.starts_with("200") {
            assert!(matches!(result, Err(CallError::Decode(_))));
        } else {
            let Err(CallError::Transport(HttpError::Status {
                status: code,
                body: actual,
            })) = result
            else {
                panic!("expected HTTP status error")
            };
            assert_eq!(
                code.as_u16(),
                if status.starts_with("403") { 403 } else { 302 }
            );
            assert_eq!(actual, body);
        }
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let client = Client::new(direct(endpoint, None));
    let server = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        request(&mut stream).await;
    };
    let (result, ()) = tokio::join!(client.call(&GetLoginInfo {}), server);
    assert!(matches!(
        result,
        Err(CallError::Transport(HttpError::Request(_)))
    ));
}
