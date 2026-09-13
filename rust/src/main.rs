#[path = "./cockatiel_lib.rs"]
mod cockatiel_lib;
use cockatiel_lib::proto::container::Payload;
use cockatiel_lib::CockatielClient;

#[tokio::main]
async fn main() {
    // Connects and auto-generates config if missing
    let mut client = CockatielClient::connect("cockatiel-config.json")
        .await
        .expect("Fatal: Could not connect to Engine");

    // Event Loop
    while let Some(container) = client.receive().await {
        match container.payload {
            Some(Payload::MessagePreProcess(msg)) => {
                println!("Received message payload: {:?}", msg);

                // Example Send:
                // client.send(Payload::MessageInProcess(...)).await.unwrap();
            }
            // The compiler ensures you safely ignore unhandled events
            _ => {}
        }
    }
}
