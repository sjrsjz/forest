use forest_onebot::{
    Client, Outcome,
    action::{GetGroupList, GetLoginInfo},
    http::HttpTransport,
};

/// Explicit opt-in, read-only connectivity example. No default remote endpoint.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("ONEBOT_HTTP_URL")?.parse()?;
    let token = std::env::var("ONEBOT_ACCESS_TOKEN").ok();
    let client = Client::new(HttpTransport::new(endpoint, token.as_deref())?);
    match client.call(&GetLoginInfo {}).await? {
        Outcome::Completed(info) => println!("Bot: {} ({})", info.nickname, info.user_id),
        Outcome::Submitted => println!("Login query submitted; final result unknown"),
    }
    match client.call(&GetGroupList {}).await? {
        Outcome::Completed(groups) => println!("Joined groups: {}", groups.len()),
        Outcome::Submitted => println!("Group query submitted; final result unknown"),
    }
    Ok(())
}
