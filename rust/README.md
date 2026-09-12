# Cockatiel Client SDK (Rust) - Micro-Tutorial

---

## Step 1: Initialize Your Rust Project
Open your terminal, create a new project if you haven't already, and navigate into it (skip this if it's already done): 
```bash
cargo new my_cockatiel_module
cd my_cockatiel_module

```

## Step 2: Add Dependencies via Terminal
install the required packages:
```bash
cargo add tokio --features full
cargo add tokio-tungstenite --features native-tls
cargo add futures-util prost thiserror url
cargo add --build prost-build
```

## Step 3: Place Your Files
You have total flexibility over where you place your files. Organize them however you prefer in your workspace:
1. Place your `cockatiel_protobuf.proto` and `lib-cockatiel.rs` files anywhere you like within your project.

2. Create a `build.rs` file to compile the Protocol Buffer definitions during build time.
* **Default Location:** place it directly in your root directory (the highest level of your project, right next to `Cargo.toml`).
* **Custom Location:** If you prefer `build.rs` to live in a custom folder (e.g., inside a `scripts/` or `build/` directory), you must tell Cargo where to find it by adding a `build` field under `[package]` in your `Cargo.toml`:
```toml
[package]
...
build = "path/to/your/custom/build.rs"
```

3. Configure your `build.rs` file to point to wherever you decided to store your `cockatiel_protobuf.proto` file:
```rust
// build.rs
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Provide the relative path to your .proto file and its parent search directory
    prost_build::compile_protos(
        &["path/to/your/custom/folder/cockatiel_protobuf.proto"], // path to the proto file
        &["path/to/your/custom/folder"]                          // include search path
    )?;
    Ok(())
}

```

## Step 4: Write Your Module Code
Open your main application file (e.g., `src/main.rs`), point the `#[path = "..."]` attribute to wherever you decided to store your `lib-cockatiel.rs` file, and implement your integration logic:
```rust
// Bring the wrapper script into scope. 
// Adjust this relative path to match wherever you chose to place lib-cockatiel.rs
#[path = "./lib-cockatiel.rs"] 
mod lib_cockatiel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Connect to the engine[cite: 2]
    let engine = lib_cockatiel::connect_to_engine(lib_cockatiel::ConnectOptions {
        url: "ws://127.0.0.1:9000".into(),  
        pin: 1234,
        module_name: "discord-adapter".into(),
        process_position: Some("connection".into()),
        priority: None, 
        module_instance_uuid7: None,
    }).await?;

    // 2. (Optional) Register your module's command capabilities with the engine
    let my_flag = lib_cockatiel::pb::Flag {
        flag_name: "help".into(),
        flag_description: "Displays help info".into(),
        limiting_type: "any".into(), 
        min_val: 0.0,
        max_val: 0.0,
        options: vec![],
    };

    let my_command = lib_cockatiel::pb::Command {
        command_name: "text to speech".into(), // DO NOT ABBREVIATE
        command_flag: "tts".into(),            // DO NOT include "!"
        command_description: "Takes a message and converts it to audio".into(),
        command_flags: vec![my_flag],
    };

    let commands_payload = lib_cockatiel::pb::Commands {
        commands: vec![my_command],
    };

    // Send capabilities package to the engine[cite: 2]
    engine.send.commands_payload(commands_payload)?;

    // 3. Bind listeners to handle incoming messages from the engine[cite: 2]
    engine.listen.all(|payload| { 
        // Fires for every incoming message container payload[cite: 2]
    });

    engine.listen.send_to_platforms(|msg: lib_cockatiel::pb::SendToPlatforms| { 
        // Handle outgoing platform message requests[cite: 1, 2]
    });

    engine.listen.timeline_event(|evt: lib_cockatiel::pb::TimelineEvent| { 
        // Handle timeline updates[cite: 1, 2]
    });

    // 4. Send messages to the engine using the unified send API[cite: 2]
    engine.send.log(lib_cockatiel::pb::Log { 
        log: "hello from module".into(), 
        blob: String::new() 
    })?;

    // 5. Disconnect cleanly when done[cite: 2]
    // IMPORTANT: If you don't send this, the engine will assume your module crashed 
    // and might attempt to restart it, leading to multiple running processes.
    engine.disconnect("shutting down").await?;[cite: 2]

    Ok(())
}
```
