#!/usr/bin/env bash
# Generates the Java stubs in java/hopper-proto from proto/ with
# buf.gen.java.yaml. It fetches protoc and protoc-gen-grpc-java at pinned
# versions from Maven Central into java/.tools (git-ignored) and checks their
# SHA-256 before using them. With --check it regenerates into a scratch tree
# and fails if the committed stubs differ.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

PROTOC_VERSION=4.36.2
GRPC_JAVA_VERSION=1.85.1
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64)
    os=osx-aarch_64
    protoc_sha=765ccb18be73e565bfc3e2ad94e85039ca28f928b3e62b487e51b7b8fdf1dcfb
    grpc_sha=e5dc43dc4c2a31123b6edb62283e98d721edd94d216d946da564ae64e96fcd3b
    ;;
  Linux-x86_64)
    # Checked 2026-11-24 and again in Phase 3c Task 18 against Maven Central's
    # .sha1 files (bb3742d0c403ab2c01a307adc1a33460ad8c1ba4 and
    # 19389e4a786d57d4f73ba8af5887ca406aff3c3e). CI runs this branch.
    os=linux-x86_64
    protoc_sha=f6ee22d65361338459208da8bff801c9ca689741cba9187b34990dd45198f864
    grpc_sha=e0701e7a47cc8e486c62a4abedecdecc2ae9a8cadd66a690f5a35f56b1d4a540
    ;;
  *)
    echo "java-gen.sh: no pinned checksums for $(uname -s)-$(uname -m); add them from Maven Central's .sha1 files" >&2
    exit 2
    ;;
esac

tools=java/.tools
mkdir -p "$tools"
fetch() { # url dest sha256
  if [ -x "$2" ] && echo "$3  $2" | shasum -a 256 -c --status; then
    return
  fi
  curl -fsSL --max-time 120 -o "$2.part" "$1"
  if ! echo "$3  $2.part" | shasum -a 256 -c --status; then
    rm -f "$2.part"
    echo "java-gen.sh: checksum mismatch for $1" >&2
    exit 1
  fi
  mv "$2.part" "$2"
  chmod +x "$2"
}
fetch "https://repo1.maven.org/maven2/com/google/protobuf/protoc/$PROTOC_VERSION/protoc-$PROTOC_VERSION-$os.exe" \
  "$tools/protoc-$PROTOC_VERSION" "$protoc_sha"
fetch "https://repo1.maven.org/maven2/io/grpc/protoc-gen-grpc-java/$GRPC_JAVA_VERSION/protoc-gen-grpc-java-$GRPC_JAVA_VERSION-$os.exe" \
  "$tools/protoc-gen-grpc-java-$GRPC_JAVA_VERSION" "$grpc_sha"

out=java/hopper-proto/src/main/java/io/github/rpasco787/hopper/proto/v1
if [ "${1:-}" = --check ]; then
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT
  buf generate --template buf.gen.java.yaml -o "$tmp"
  if ! diff -r "$tmp/$out" "$out" >/dev/null; then
    echo "java-gen.sh: the committed stubs are stale; run scripts/java-gen.sh" >&2
    diff -r "$tmp/$out" "$out" | head -20 >&2
    exit 1
  fi
  echo "java stubs are current"
  exit 0
fi
rm -rf "$out"
buf generate --template buf.gen.java.yaml
echo "generated $(ls "$out" | wc -l | tr -d ' ') files in $out"
