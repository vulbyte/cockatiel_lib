#[path = "cockatiel_lib.rs"]
mod cockatiel_lib;

use cockatiel_lib::proto::container::Payload;
use cockatiel_lib::CockatielClient;

#[tokio::main]
async fn main() {
    let mut client = CockatielClient::connect("cockatiel-config.json")
        .await
        .expect("Fatal: Could not connect to Engine");

    // Accessing config to silence the unused field warning
    println!(
        "Module active: [{}] on position {}",
        client.config.module_name, client.config.position
    );

    while let Some(container) = client.receive().await {
        match container.payload {
            Some(Payload::MessagePreProcess(msg)) => {
                println!("Received message payload: {:?}", msg);

                // Example call to exercise the send method
                // client.send(Payload::MessageInProcess(..)).await.unwrap();
            }
            _ => {}
        }
    }
}
