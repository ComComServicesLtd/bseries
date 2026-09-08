#!/usr/bin/env python3
"""Repack an OCI image archive into the legacy Docker v1 layout.

RouterOS reads only the old format: a directory per layer holding an
uncompressed layer.tar. Docker has not written that format since the containerd
image store became the default -- buildx emits an OCI layout whose manifest.json
points at blobs/sha256/..., and every layer is gzipped. RouterOS gets as far as
reading the manifest and then fails with "could not load next layer".

Nothing here is bseries specific; it converts any single platform OCI archive.

    oci-to-docker-archive.py <in.tar> <out.tar> [repo:tag]
"""

import gzip
import hashlib
import json
import os
import shutil
import sys
import tarfile
import tempfile

LEGACY_VERSION = "1.0"


def load_manifest(src):
    """Pick the image manifest out of an OCI index, rejecting multi-platform."""
    index = json.load(open(os.path.join(src, "index.json")))
    manifests = [m for m in index["manifests"]
                 if "vnd.oci.image.index" not in m.get("mediaType", "")]
    if len(manifests) != 1:
        sys.exit("expected exactly one image manifest, found %d -- build with a "
                 "single --platform and --provenance=false --sbom=false"
                 % len(manifests))
    return json.load(open(blob(src, manifests[0]["digest"]))), manifests[0]


def blob(src, digest):
    return os.path.join(src, "blobs", "sha256", digest.split(":", 1)[1])


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    in_tar, out_tar = sys.argv[1], sys.argv[2]
    tag = sys.argv[3] if len(sys.argv) > 3 else None

    work = tempfile.mkdtemp(prefix="oci2docker.")
    try:
        src = os.path.join(work, "src")
        dst = os.path.join(work, "dst")
        os.makedirs(src)
        os.makedirs(dst)

        with tarfile.open(in_tar) as t:
            # filter= is 3.12 and later, and becomes the default in 3.14.
            try:
                t.extractall(src, filter="data")
            except TypeError:
                t.extractall(src)

        manifest, _ = load_manifest(src)
        config_digest = manifest["config"]["digest"]
        config = json.load(open(blob(src, config_digest)))

        config_name = config_digest.split(":", 1)[1] + ".json"
        shutil.copyfile(blob(src, config_digest), os.path.join(dst, config_name))

        # The layer id is the digest of the *uncompressed* layer, which is also
        # what the config already records as its diff_id -- so the two agree
        # without the config needing to be rewritten.
        layer_paths = []
        parent = None
        for entry in manifest["layers"]:
            compressed = blob(src, entry["digest"])
            raw = os.path.join(work, "layer.tar")

            opener = gzip.open if entry["mediaType"].endswith("gzip") else open
            digest = hashlib.sha256()
            with opener(compressed, "rb") as fin, open(raw, "wb") as fout:
                while True:
                    chunk = fin.read(1 << 20)
                    if not chunk:
                        break
                    digest.update(chunk)
                    fout.write(chunk)

            layer_id = digest.hexdigest()
            layer_dir = os.path.join(dst, layer_id)
            os.makedirs(layer_dir, exist_ok=True)
            os.replace(raw, os.path.join(layer_dir, "layer.tar"))

            with open(os.path.join(layer_dir, "VERSION"), "w") as f:
                f.write(LEGACY_VERSION)
            # Docker ignores this for anything but the top layer; it is written
            # because the format requires the file to exist.
            with open(os.path.join(layer_dir, "json"), "w") as f:
                json.dump({"id": layer_id, "parent": parent}, f)

            layer_paths.append(layer_id + "/layer.tar")
            parent = layer_id

        repo_tags = [tag] if tag else []
        with open(os.path.join(dst, "manifest.json"), "w") as f:
            json.dump([{"Config": config_name,
                        "RepoTags": repo_tags,
                        "Layers": layer_paths}], f)

        if tag and ":" in tag:
            name, version = tag.rsplit(":", 1)
            with open(os.path.join(dst, "repositories"), "w") as f:
                json.dump({name: {version: parent}}, f)

        with tarfile.open(out_tar, "w") as t:
            for entry in sorted(os.listdir(dst)):
                t.add(os.path.join(dst, entry), arcname=entry)
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
