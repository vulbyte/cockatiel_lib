#!/usr/bin/env bash
#
# Regenerates java/Cockatiel.java — the single-file Java client for the
# Cockatiel engine.
#
#   1. compiles the vendored cockatiel_protobuf.proto with protoc --java_out
#   2. remaps the generated package (cockatiel_protobuf.v1) -> cockatiel
#   3. renames the generated holder class CockatielProtobuf -> Cockatiel so the
#      single public top-level class matches the file name (one public class
#      per .java file is a hard Java rule)
#   4. hoists the handwritten client's imports to the top of the file and
#      splices codegen/CockatielClient.java into the holder (mirroring the C#
#      namespace layout: everything hangs off the single `Cockatiel` type).
#
# Requires: protoc on PATH (set PROTOC to override). Edit
# codegen/CockatielClient.java to tweak the client — this script is only
# needed after the .proto changes.
set -euo pipefail

LIB_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
JAVA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROTO="$LIB_ROOT/cockatiel_protobuf.proto"
PARTIAL="$JAVA_DIR/codegen/CockatielClient.java"
OUT="$JAVA_DIR/Cockatiel.java"
PROTOC="${PROTOC:-/usr/local/bin/protoc}"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

"$PROTOC" -I "$LIB_ROOT" --java_out="$TMP" "$PROTO"

GEN="$TMP/cockatiel_protobuf/v1/CockatielProtobuf.java"
if [ ! -f "$GEN" ]; then
    echo "error: unexpected protoc output" >&2
    exit 1
fi

# 1. Remap package + fully-qualified references.
sed 's/cockatiel_protobuf\.v1/cockatiel/g' "$GEN" > "$TMP/remapped.java"

# 2. Rename the holder class so the public top-level type matches Cockatiel.java.
sed 's/CockatielProtobuf/Cockatiel/g' "$TMP/remapped.java" > "$TMP/renamed.java"

# The generated holder must end with exactly one bare '}' (the class close) as
# the final line; the client is spliced in before it.
if [ "$(tail -n 1 "$TMP/renamed.java")" != "}" ]; then
    echo "error: unexpected protoc output footer" >&2
    exit 1
fi

# 3. Hoist the client's imports above the generated types (Java requires all
#    imports before the first type declaration).
grep -E '^import ' "$PARTIAL" > "$TMP/client-imports.java" || true
grep -vE '^import ' "$PARTIAL" > "$TMP/client-body.java"

# 4. Insert the imports right after `package cockatiel;`, then drop the final
#    '}' so the client can be nested inside the holder.
sed -e '/^package cockatiel;$/r '"$TMP/client-imports.java" "$TMP/renamed.java" \
    | sed '$d' > "$TMP/generated-head.java"

{
    cat "$TMP/generated-head.java"
    echo
    echo "  // ─────────────────────────────────────────────────────────────"
    echo "  //  HANDWRITTEN CLIENT (codegen/CockatielClient.java)"
    echo "  // ─────────────────────────────────────────────────────────────"
    cat "$TMP/client-body.java"
    echo
    echo "}"
} > "$OUT"

echo "Wrote $OUT ($(wc -l < "$OUT" | tr -d ' ') lines)"