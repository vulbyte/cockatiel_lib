use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use prost::Message as _;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::{tungstenite::Message as WsMessage, MaybeTlsStream, WebSocketStream};

/// Generated protobuf types (from cockatiel_protobuf.proto).
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/cockatiel_protobuf.v1.rs"));
}

// NOTE: `pb::Err` shadows `std::result::Result::Err` if you glob-import
// `pb::*`. Prefer qualifying it as `pb::Err` (as this crate does throughout)
// rather than `use cockatiel_client::pb::*;`.

const PROTOCOL_VERSION: i32 = 1;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("websocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("invalid engine url: {0}")]
    InvalidUrl(String),
    #[error("engine rejected the connection request (pin/priority mismatch, or engine full)")]
    ConnectionRejected,
    #[error("engine sent an unexpected message during handshake")]
    UnexpectedHandshakeResponse,
    #[error("connection closed before a response arrived")]
    ConnectionClosed,
    #[error("failed to decode a message from the engine: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("not connected to the engine anymore")]
    Disconnected,
}

/// Arguments for [`connect_to_engine`].
///
/// `pin`, `process_position`, `priority`, and `module_instance_uuid7` map directly
/// onto `ConnectionRequest`. `url` and `module_name` are additionally required
/// here: `url` is the engine's initial listening address (the engine hands back
/// a dedicated port after accepting the request, and this client transparently
/// reconnects to it), and `module_name` is a required field on every `Container`
/// the engine expects.
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    /// e.g. "ws://127.0.0.1:9000" — the engine's initial connection endpoint.
    pub url: String,
    /// Required. Must match the engine's configured pin or the request is refused.
    pub pin: i32,
    /// Required. Used for display / internal resource management on the engine side.
    pub module_name: String,
    /// Optional. "preprocess" | "inprocess" | "postprocess" | "connection".
    pub process_position: Option<String>,
    /// Optional. Lower number = higher priority. Defaults to 0.
    pub priority: Option<i32>,
    /// Optional. uuid7. Leave blank to let the engine assign one.
    pub module_instance_uuid7: Option<String>,
}

/// A live, post-handshake connection to the engine.
pub struct EngineConnection {
    pub listen: Listen,
    pub send: SendApi,
    module_instance_uuid7: Arc<Mutex<String>>,
    auth_token: Arc<Mutex<String>>,
    outbound_tx: mpsc::UnboundedSender<pb::Container>,
}

impl EngineConnection {
    /// The module_instance_uuid7 this connection ended up with (the engine may
    /// have reassigned it if the one you requested collided with another module).
    pub fn module_instance_uuid7(&self) -> String {
        self.module_instance_uuid7.lock().unwrap().clone()
    }

    /// Update the auth token used on future outgoing messages, e.g. after
    /// receiving an `AuthNew` from the engine.
    pub fn set_auth_token(&self, token: impl Into<String>) {
        *self.auth_token.lock().unwrap() = token.into();
    }

    /// Send a `Shutdown` message with the given reason, then close the socket.
    pub async fn disconnect(&self, reason: impl Into<String>) -> Result<(), EngineError> {
        self.send.shutdown(pb::Shutdown {
            reason: reason.into(),
        })?;
        // Give the writer task a beat to flush before we drop the sender.
        tokio::task::yield_now().await;
        Ok(())
    }
}

macro_rules! define_listen_and_send {
    ( $( $field:ident : $Variant:ident => $Msg:ty ),+ $(,)? ) => {
        /// Register callbacks for incoming messages, either narrowly by type
        /// (`listen.log(...)`) or as a catch-all (`listen.all(...)`).
        pub struct Listen {
            registry: Arc<HandlerRegistry>,
        }

        impl Listen {
            /// Fires for every incoming message, regardless of type.
            pub fn all<F>(&self, f: F)
            where
                F: Fn(pb::container::Payload) + Send + Sync + 'static,
            {
                self.registry.all.lock().unwrap().push(Box::new(f));
            }

            $(
                #[doc = concat!("Fires only for incoming `", stringify!($Msg), "` messages.")]
                pub fn $field<F>(&self, f: F)
                where
                    F: Fn($Msg) + Send + Sync + 'static,
                {
                    self.registry.$field.lock().unwrap().push(Box::new(f));
                }
            )+
        }

        #[allow(dead_code)]
        struct HandlerRegistry {
            all: Mutex<Vec<Box<dyn Fn(pb::container::Payload) + Send + Sync>>>,
            $( $field: Mutex<Vec<Box<dyn Fn($Msg) + Send + Sync>>>, )+
        }

        impl HandlerRegistry {
            fn new() -> Self {
                Self {
                    all: Mutex::new(Vec::new()),
                    $( $field: Mutex::new(Vec::new()), )+
                }
            }

            /// Runs every matching handler for one incoming `Container` payload.
            fn dispatch(&self, payload: pb::container::Payload) {
                for f in self.all.lock().unwrap().iter() {
                    f(payload.clone());
                }
                match payload {
                    $(
                        pb::container::Payload::$Variant(inner) => {
                            for f in self.$field.lock().unwrap().iter() {
                                f(inner.clone());
                            }
                        }
                    )+
                }
            }
        }

        /// Send outbound messages to the engine, one method per message type
        /// (`send.log(...)`, `send.timeline_event(...)`, etc). Each call wraps
        /// the payload in a `Container` with the connection's current auth
        /// token / module identity and enqueues it on the writer task.
        pub struct SendApi {
            tx: mpsc::UnboundedSender<pb::Container>,
            module_name: String,
            module_instance_uuid7: Arc<Mutex<String>>,
            auth_token: Arc<Mutex<String>>,
        }

        impl SendApi {
            $(
                #[doc = concat!("Send a `", stringify!($Msg), "` to the engine.")]
                pub fn $field(&self, data: $Msg) -> Result<(), EngineError> {
                    let container = pb::Container {
                        version: PROTOCOL_VERSION,
                        auth_token: self.auth_token.lock().unwrap().clone(),
                        module_name: self.module_name.clone(),
                        module_instance_uuid7: self.module_instance_uuid7.lock().unwrap().clone(),
                        payload: Some(pb::container::Payload::$Variant(data)),
                    };
                    self.tx.send(container).map_err(|_| EngineError::Disconnected)
                }
            )+
        }
    };
}

// One entry per Container.payload oneof field. `$field` is what you'll call
// on `.listen` / `.send` (matches the .proto oneof field name in snake_case).
// `$Variant` is prost's generated enum variant name, derived from that field
// name — NOT always the same as the message type. `$Msg` is the actual
// message type. Double-check `$Variant` against the generated code in
// OUT_DIR/cockatiel_protobuf.v1.rs if you add/rename fields in the .proto;
// prost's naming here is mechanical but easy to get wrong by hand.
define_listen_and_send! {
    connection_request: ConnectionRequest => pb::ConnectionRequest,
    connection_request_return: ConnectionRequestReturn => pb::ConnectionRequestReturn,
    auth_verify: AuthVerify => pb::AuthVerify,
    auth_new: AuthNew => pb::AuthNew,
    command_payload: CommandPayload => pb::Command,
    commands_payload: CommandsPayload => pb::Commands,
    message_pre_process: MessagePreProcess => pb::MessagePreProcess,
    message_in_process: MessageInProcess => pb::MessageInProcess,
    message_post_process: MessagePostProcess => pb::MessagePostProcess,
    timeline_event: TimelineEvent => pb::TimelineEvent,
    user_data: UserData => pb::UserData,
    shutdown: Shutdown => pb::Shutdown,
    log: Log => pb::Log,
    err: Err => pb::Err,
    // See the ADDED comment in the .proto: this field didn't exist in the
    // original oneof, so it could never be sent/received before.
    send_to_platforms: SendToPlatforms => pb::SendToPlatfroms,
}

/// Connect to the engine, perform the two-phase handshake (initial port ->
/// `ConnectionRequest` -> dedicated port), and return a live connection.
pub async fn connect_to_engine(opts: ConnectOptions) -> Result<EngineConnection, EngineError> {
    let (mut ws, _) = tokio_tungstenite::connect_async(&opts.url).await?;

    let requested_uuid = opts.module_instance_uuid7.clone().unwrap_or_default();
    let request = pb::ConnectionRequest {
        pin: opts.pin,
        process_position: opts.process_position.clone().unwrap_or_default(),
        priority: opts.priority.unwrap_or(0),
        module_instance_uuid7: requested_uuid.clone(),
    };
    let handshake = pb::Container {
        version: PROTOCOL_VERSION,
        auth_token: String::new(),
        module_name: opts.module_name.clone(),
        module_instance_uuid7: requested_uuid.clone(),
        payload: Some(pb::container::Payload::ConnectionRequest(request)),
    };
    send_container(&mut ws, &handshake).await?;

    let response = recv_container(&mut ws).await?;
    let (new_port, resolved_uuid) = match response.payload {
        Some(pb::container::Payload::ConnectionRequestReturn(r)) => {
            if r.new_port == 0 {
                return Err(EngineError::ConnectionRejected);
            }
            let uuid = if r.module_instance_uuid7.is_empty() {
                requested_uuid
            } else {
                r.module_instance_uuid7
            };
            (r.new_port, uuid)
        }
        _ => return Err(EngineError::UnexpectedHandshakeResponse),
    };

    // Per the engine's flow, the original port connection is dropped once a
    // dedicated port is handed out; reconnect there for everything else.
    let _ = ws.close(None).await;
    let dedicated_url = with_port(&opts.url, new_port)?;
    let (ws2, _) = tokio_tungstenite::connect_async(&dedicated_url).await?;

    let module_instance_uuid7 = Arc::new(Mutex::new(resolved_uuid));
    let auth_token = Arc::new(Mutex::new(String::new()));
    let registry = Arc::new(HandlerRegistry::new());
    let (outbound_tx, outbound_rx) = mpsc::unbounded_channel::<pb::Container>();

    spawn_io_tasks(ws2, registry.clone(), outbound_rx);

    let listen = Listen {
        registry: registry.clone(),
    };
    let send = SendApi {
        tx: outbound_tx.clone(),
        module_name: opts.module_name,
        module_instance_uuid7: module_instance_uuid7.clone(),
        auth_token: auth_token.clone(),
    };

    Ok(EngineConnection {
        listen,
        send,
        module_instance_uuid7,
        auth_token,
        outbound_tx,
    })
}

/// Splits the dedicated websocket into a reader task (decodes incoming
/// `Container`s and dispatches them to the `HandlerRegistry`) and a writer
/// task (drains outbound `Container`s onto the socket).
fn spawn_io_tasks(
    ws: WsStream,
    registry: Arc<HandlerRegistry>,
    mut outbound_rx: mpsc::UnboundedReceiver<pb::Container>,
) {
    let (mut write, mut read) = ws.split();

    tokio::spawn(async move {
        while let Some(msg) = outbound_rx.recv().await {
            let mut buf = Vec::new();
            if msg.encode(&mut buf).is_err() {
                continue;
            }
            if write.send(WsMessage::Binary(buf)).await.is_err() {
                break;
            }
        }
    });

    tokio::spawn(async move {
        while let Some(frame) = read.next().await {
            let frame = match frame {
                Ok(f) => f,
                Err(_) => break,
            };
            let bytes = match frame {
                WsMessage::Binary(b) => b,
                WsMessage::Close(_) => break,
                _ => continue, // ignore text/ping/pong frames
            };
            if let Ok(container) = pb::Container::decode(bytes.as_slice()) {
                if let Some(payload) = container.payload {
                    registry.dispatch(payload);
                }
            }
        }
    });
}

async fn send_container(ws: &mut WsStream, container: &pb::Container) -> Result<(), EngineError> {
    let mut buf = Vec::new();
    container
        .encode(&mut buf)
        .expect("encoding a well-formed Container cannot fail");
    ws.send(WsMessage::Binary(buf)).await?;
    Ok(())
}

async fn recv_container(ws: &mut WsStream) -> Result<pb::Container, EngineError> {
    while let Some(frame) = ws.next().await {
        let frame = frame?;
        match frame {
            WsMessage::Binary(bytes) => return Ok(pb::Container::decode(bytes.as_slice())?),
            WsMessage::Close(_) => return Err(EngineError::ConnectionClosed),
            _ => continue,
        }
    }
    Err(EngineError::ConnectionClosed)
}

/// Rewrites the port on a ws(s):// URL, keeping scheme/host/path intact.
fn with_port(base_url: &str, port: i32) -> Result<String, EngineError> {
    let mut url = url::Url::parse(base_url)
        .map_err(|e| EngineError::InvalidUrl(format!("{base_url}: {e}")))?;
    url.set_port(Some(port as u16))
        .map_err(|_| EngineError::InvalidUrl(format!("could not set port {port} on {base_url}")))?;
    Ok(url.to_string())
}
