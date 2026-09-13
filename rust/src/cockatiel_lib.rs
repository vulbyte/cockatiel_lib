use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::time::Duration;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

// Re-export your generated Protobuf module here so devs don't have to compile it themselves
// pub mod proto { include!(concat!(env!("OUT_DIR"), "/cockatiel_protobuf.v1.rs")); }

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CockatielConfig {
    #[serde(default = "default_ip")]
    pub ip: String,
    pub port: u16,
    pub pin: i32,
    pub module_name: String,
    pub position: i32, // Maps to ProcessPosition enum
    #[serde(default = "default_priority")]
    pub priority: u32,
}

// Ergonomic defaults for Serde
fn default_ip() -> String {
    "localhost".to_string()
}
fn default_priority() -> u32 {
    100
}

impl CockatielConfig {
    /// Loads from JSON, or creates it if it doesn't exist.
    pub fn load_or_create<P: AsRef<Path>>(path: P) -> Self {
        if let Ok(file_content) = fs::read_to_string(&path) {
            if let Ok(config) = serde_json::from_str(&file_content) {
                return config;
            }
        }

        // Default template if missing
        let default_config = Self {
            ip: default_ip(),
            port: 8080,
            pin: 0000,
            module_name: "unnamed_module".to_string(),
            position: 1,
            priority: default_priority(),
        };

        let _ = fs::write(path, serde_json::to_string_pretty(&default_config).unwrap());
        default_config
    }
}

pub struct CockatielClient {
    // Underlying WebSocket stream
    stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    pub config: CockatielConfig,
}

impl CockatielClient {
    /// Ergonomic builder accepting anything that turns into a String for config paths
    pub async fn connect(config_path: impl Into<String>) -> Result<Self, String> {
        let config_path = config_path.into();
        let config = CockatielConfig::load_or_create(&config_path);

        let ws_url = format!("wss://{}:{}", config.ip, config.port);
        let max_attempts = 60; // 5 minutes / 5 seconds
        let mut attempt = 0;

        loop {
            attempt += 1;
            println!(
                "Connecting to {} (Attempt {}/{})",
                ws_url, attempt, max_attempts
            );

            match connect_async(&ws_url).await {
                Ok((ws_stream, _)) => {
                    println!("Successfully connected to Cockatiel Engine.");

                    let mut client = Self {
                        stream: ws_stream,
                        config: config.clone(),
                    };

                    // TODO: Send Initial ConnectionRequest Protobuf here
                    // client.send(Payload::ConnectionRequest(...)).await;

                    return Ok(client);
                }
                Err(e) => {
                    if attempt >= max_attempts {
                        return Err(format!("Failed to connect after 5 minutes: {}", e));
                    }
                    eprintln!("Connection failed. Retrying in 5 seconds...");
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    /// Accepts the generated Enum `Payload` and handles the envelope automatically
    pub async fn send(&mut self, payload: proto::container::Payload) -> Result<(), String> {
        let container = proto::Container {
            version: 1,
            auth_token: "TEMP_TOKEN".to_string(), // Injected automatically
            module_name: self.config.module_name.clone(),
            module_instance_uuid7: "YOUR_UUID_HERE".to_string(), // Injected automatically
            payload: Some(payload),
        };

        let mut buf = Vec::new();
        container.encode(&mut buf).map_err(|e| e.to_string())?;

        self.stream
            .send(Message::Binary(buf))
            .await
            .map_err(|e| e.to_string())
    }

    /// Yields the next valid Container from the engine
    pub async fn receive(&mut self) -> Option<proto::Container> {
        while let Some(msg) = self.stream.next().await {
            if let Ok(Message::Binary(bin)) = msg {
                if let Ok(container) = proto::Container::decode(&*bin) {
                    return Some(container);
                }
            }
        }
        None // Triggers if connection drops
    }
}
