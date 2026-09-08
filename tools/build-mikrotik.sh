#!/usr/bin/env bash
# Build bseries as a tar that RouterOS can import with /container/add file=.
#
#   ./tools/build-mikrotik.sh                    # arm64, the common case
#   ./tools/build-mikrotik.sh linux/arm          # 32 bit arm models
#   ./tools/build-mikrotik.sh linux/amd64        # x86 (CHR, CCR20xx)
#
# Check the target with `/system resource print` on the router and read
# architecture-name: arm64 here means linux/arm64, arm means linux/arm.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLATFORM="${1:-linux/arm64}"
ARCH="${PLATFORM##*/}"
OUT="${OUT:-$HERE/bseries-$ARCH.tar}"

# --provenance and --sbom default to on with buildx, and each adds an
# attestation manifest to the archive. RouterOS cannot match those to a
# platform, and the converter below refuses them too.
OCI="$(mktemp -t bseries-oci.XXXXXX.tar)"
trap 'rm -f "$OCI"' EXIT

docker buildx build \
    --platform "$PLATFORM" \
    --provenance=false \
    --sbom=false \
    -f "$HERE/Dockerfile.mikrotik" \
    -t "bseries:$ARCH" \
    --output "type=docker,dest=$OCI" \
    "$HERE"

# Docker writes an OCI layout whatever the output type says, because the
# containerd image store is the default and it has no v1 writer left. RouterOS
# reads only the old layout, and fails on the modern one with "could not load
# next layer" after the container has already been created.
python3 "$HERE/tools/oci-to-docker-archive.py" "$OCI" "$OUT" "bseries:$ARCH"

echo
echo "wrote $OUT ($(du -h "$OUT" | cut -f1))"
echo
echo "Upload it and provision the router:"
echo "  scp $OUT <router>:"
echo "  # then apply mikrotik.example.rsc, editing the names at the top first"
