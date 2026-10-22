use crate::App;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{
        Sse,
        sse::{Event, KeepAlive},
    },
};
use futures::Stream;
use std::convert::Infallible;

pub(crate) async fn stream(
    State(app): State<App>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    let reader = app
        .inspector
        .logs()
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let cursor = headers
        .get("last-event-id")
        .map(|value| {
            let (journal, sequence) = value
                .to_str()
                .map_err(|_| StatusCode::BAD_REQUEST)?
                .rsplit_once(':')
                .ok_or(StatusCode::BAD_REQUEST)?;
            let sequence = sequence
                .parse::<u64>()
                .map_err(|_| StatusCode::BAD_REQUEST)?;
            Ok::<_, StatusCode>((journal == reader.id()).then_some(sequence))
        })
        .transpose()?
        .flatten();
    let updates = reader.subscribe();
    let stream = futures::stream::unfold(
        (reader, updates, app.closing, cursor),
        |(reader, mut updates, mut closing, mut cursor)| async move {
            loop {
                updates.borrow_and_update();
                let batch = reader.after(cursor);
                if batch.reset || !batch.entries.is_empty() {
                    cursor = Some(batch.cursor);
                    let event = Event::default()
                        .event("logs")
                        .id(format!("{}:{}", reader.id(), batch.cursor))
                        .json_data(batch)
                        .expect("serializable log batch");
                    return Some((Ok(event), (reader, updates, closing, cursor)));
                }
                tokio::select! {
                    changed = updates.changed() => { changed.ok()?; }
                    _ = closing.changed() => return None,
                }
            }
        },
    );
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
