#!/usr/bin/env bash
#
# Regenerates Cockatiel/Cockatiel.cs — the single-file C# client for the
# Cockatiel engine.
#
#   1. compiles the vendored cockatiel_protobuf.proto with protoc --csharp_out
#   2. remaps the generated namespace (CockatielProtobuf.V1) -> Cockatiel
#   3. splices in the handwritten client (codegen/CockatielClient.partial.cs)
#      and re-closes the namespace, producing one self-contained source file.
#
# Requires: protoc on PATH. Edit Cockatiel.cs directly to tweak the client —
# this script is only needed after the .proto changes.
set -euo pipefail

LIB_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DOTNET_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROTO="$LIB_ROOT/cockatiel_protobuf.proto"
PARTIAL="$DOTNET_DIR/codegen/CockatielClient.partial.cs"
OUT="$DOTNET_DIR/Cockatiel/Cockatiel.cs"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

protoc -I "$LIB_ROOT" --csharp_out="$TMP" "$PROTO"

GEN="$TMP/CockatielProtobuf.cs"

# Sanity: the generated footer must be exactly namespace-close + blank +
# '#endregion Designer generated code' (one bare '}' and one closing endregion).
if [ "$(grep -c '^}$' "$GEN")" -ne 1 ] \
    || [ "$(grep -c '^#endregion Designer generated code$' "$GEN")" -ne 1 ] \
    || [ "$(tail -n 1 "$GEN")" != "#endregion Designer generated code" ]; then
    echo "error: unexpected protoc output footer" >&2
    exit 1
fi

# Strip the namespace-closing '}', the trailing blank line, and the trailing
# '#endregion Designer generated code'; the client partial is spliced in before
# we re-emit all three. Also silence nullable-analysis warnings that the
# generated descriptor code trips over (they are re-enabled before the client).
sed 's/CockatielProtobuf\.V1/Cockatiel/g' "$GEN" \
    | sed 's/#pragma warning disable 1591, 0612, 3021, 8981/#pragma warning disable 1591, 0612, 3021, 8981, 8600, 8601, 8602, 8603, 8604, 8618, 8625, 8632, 8765, 8767/' \
    | sed '$d' | sed '$d' | sed '$d' > "$TMP/generated.cs"

# C# requires using directives before any namespace members, so the client's
# usings are hoisted to the very top of the file and removed from the splice.
grep -E '^using ' "$PARTIAL" > "$TMP/client-usings.cs"
grep -vE '^using ' "$PARTIAL" > "$TMP/client-body.cs"

{
    cat "$TMP/client-usings.cs"
    echo
    cat "$TMP/generated.cs"
    echo
    echo "// ───────────────────────────────────────────────────────────────────"
    echo "//  HANDWRITTEN CLIENT (codegen/CockatielClient.partial.cs)"
    echo "// ───────────────────────────────────────────────────────────────────"
    echo "#pragma warning restore 1591, 0612, 3021, 8981, 8600, 8601, 8602, 8603, 8604, 8618, 8625, 8632, 8765, 8767"
    cat "$TMP/client-body.cs"
    echo
    echo "}"
    echo
    echo "#endregion Designer generated code"
} > "$OUT"

echo "Wrote $OUT ($(wc -l < "$OUT" | tr -d ' ') lines)"