#!/usr/bin/env bash
set -e

echo "Setting up Cockatiel module structure..."

# Create directories
mkdir -p proto
mkdir -p src

# Create build.rs if it doesn't exist
if [ ! -f "build.rs" ]; then
    cat << 'EOF' > build.rs
fn main() {
    prost_build::compile_protos(&["proto/cockatiel.proto"], &["proto/"]).unwrap();
}
EOF
    echo "Created build.rs"
else
    echo "build.rs already exists, skipping."
fi

echo "Setup complete! Drop your 'cockatiel.proto' into the 'proto/' folder and 'cockatiel_lib.rs' into 'src/'."
