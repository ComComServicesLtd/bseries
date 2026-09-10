# Shipping a container to MikroTik RouterOS

Everything below was learned building and deploying this database to a hAP ax²
running RouterOS 7.24.2. Each item is here because it cost time, and most of them
fail in a way that points at the wrong thing.

Read the failure signatures table at the end first if something is already broken.


## 1. Prepare the device, once

**RouterOS 7.20 or later**, because multiple veth per container landed there.

```
/system package update check-for-updates
/system package update install
```

**Install the container package.** `container-<arch>.npk` from the Extra packages
archive on mikrotik.com/download, matching `architecture-name` below. Upload it
and reboot.

**Enable container device mode.** This one needs *physical* access:

```
/system/device-mode/update container=yes
```

then press the reset button or power-cycle within the timeout. There is no way
round this remotely; plan for it if the device is not on your desk.

**Confirm the target's architecture** — everything else depends on it:

```
/system resource print
```

`architecture-name` is `arm64` on recent models and `arm` on older 32 bit ones,
which map to `linux/arm64` and `linux/arm`. Note `free-hdd-space` at the same
time; see §6.


## 2. Development access

### SSH

Create a key-only user restricted to your lab subnet. Upload the public key to
the router first (Winbox > Files > drag and drop), then:

```
/user add name=claudedev group=full address=172.16.220.0/24 comment="dev access"
/user ssh-keys import public-key-file=mikrotik_dev.pub user=claudedev
```

**RouterOS ships an older SSH server**, so be explicit about algorithms or the
handshake fails in a way that looks like a rejected key:

```bash
ssh -i dev/keys/mikrotik_dev \
    -o IdentitiesOnly=yes \
    -o PubkeyAcceptedKeyTypes=+ssh-rsa \
    -o HostKeyAlgorithms=+ssh-rsa \
    claudedev@172.16.220.131 '/system resource print'
```

Wrap that in a script. Typing it by hand every time is how you end up debugging
the wrong layer.

**`scp` needs `-O`** (legacy protocol) against RouterOS. Without it the transfer
appears to hang or fails with an unhelpful protocol error.

A passwordless full-access user is acceptable on an isolated lab bench **only**.
Re-bootstrap before anything leaves it.

### Reaching the container's API

A container gets its own address on a bridge, and that is what *other containers*
talk to. To reach it from your workstation you need a dst-nat rule:

```
/ip firewall nat add chain=dstnat action=dst-nat protocol=tcp \
    dst-address=172.16.220.131 dst-port=8086 \
    to-addresses=172.20.0.3 to-ports=8086 comment="dev: bseries"
```

Test both paths — they fail independently:

```bash
curl http://172.16.220.131:8086/v1/health          # via dst-nat, from your desk
```
```
/tool/fetch url="http://172.20.0.3:8086/v1/health" output=user
```

The second runs *on the router* and is the one that matters for container to
container traffic. A sibling container talking to yours uses the veth address,
not the router's.

### Files on the device

There is no shell, but directories can still be created:

```
/file/add name=bseries-data/profiles type=directory
```

`/file/print where name~"..."` lists them. Uploaded files land in the filesystem
root by default.


## 3. Build the image

- [ ] **Static binary, `scratch` base.** No libc to ship, nothing to patch, and
      the whole image is the binary. Build on **alpine/musl** rather than a glibc
      base: glibc's `getaddrinfo` wants its shared libraries back at run time even
      inside a static binary, and musl's does not.

- [ ] **Run as root.** This is the opposite of normal advice and it is not
      optional: RouterOS mounts a directory out of its own flash, owned by root,
      and offers neither a shell nor `chown`. A non-root process cannot write its
      own data directory. Root is confined to the container's namespace.

- [ ] **Suppress build attestations.** `--provenance=false --sbom=false`.
      RouterOS cannot match an attestation manifest to a platform.

- [ ] **Repack to the legacy Docker layout.** This is the big one — see §4.

- [ ] **`HEALTHCHECK` is honoured.** It surfaces as `H` in `/container/print` and
      drives `stop-on-unhealthy`. Worth setting. Have the binary probe itself so
      the image needs no HTTP client.

- [ ] **`VOLUME` is ignored.** `/container/mounts` is the only source of truth.
      Leaving `VOLUME` in suggests a guarantee the router is not making.

- [ ] **Cross-building needs an emulator on the host.** Registration does **not**
      survive a reboot:

      ```bash
      docker run --privileged --rm tonistiigi/binfmt --install arm64   # until reboot
      sudo apt install qemu-user-static                                # at every boot
      ```

      Preflight it, or you will debug a "broken Dockerfile" that is nothing of the
      sort.


## 4. The image format trap

**RouterOS reads only the legacy Docker v1 tar layout**: a directory per layer
containing an uncompressed `layer.tar`, plus a `manifest.json` pointing at them.

Docker has not written that since the containerd image store became the default.
`docker save` and `--output type=docker` both emit an **OCI layout** — a
`blobs/sha256/` tree with **gzipped** layers and an `oci-layout` marker. It looks
like a tar of an image and RouterOS cannot read it.

The failure is late and says nothing useful: `/container/add` **succeeds**, and
the container then appears flagged `F` with:

```
download/extract error: could not load next layer
```

Repack before uploading. The conversion is mechanical — decompress each layer,
write it as `<digest>/layer.tar` with `VERSION` and `json` beside it, and rebuild
`manifest.json` — and `tools/oci-to-docker-archive.py` in this repo does it.

Sanity check before you upload:

```bash
tar tf bseries-arm64.tar | head
#  good:  <hex>/layer.tar, <hex>.json, manifest.json, repositories
#  bad:   blobs/sha256/..., oci-layout, index.json
```


## 5. Provision it

```
/interface/veth/add name=bseries address=172.20.0.3/24 gateway=172.20.0.1
/interface/bridge/port/add bridge=containers interface=bseries

/container/mounts/add list=bseries-data src=/bseries-data dst=/data mode=rw
/container/envs/add list=bseries key=BSERIES_FLUSH_INTERVAL value=3600

/container/add file=bseries-arm64.tar interface=bseries \
    root-dir=/containers/bseries \
    envlist=bseries mountlists=bseries-data \
    start-on-boot=yes logging=yes
```

- [ ] **Two parameter names are easy to get wrong**, and both fail with a bare
      `bad parameter` and a column number:
      - mounts are declared with **`list=`**, not `name=`
      - and attached with **`mountlists=`**, not `mounts=`

      RouterOS reports the column at the *end* of the offending token, which makes
      it look like the *next* parameter is at fault.

- [ ] **The container's name comes from the image tag**, not from anything you
      pass to `/container/add`. Tag deliberately.

- [ ] **A veth stays down until a container binds it.** An inactive bridge port
      (`I`) before first start is expected, not a fault.

- [ ] **Listen on `0.0.0.0`, not loopback.** Inside a container the loopback is
      reachable only from that container, so a service bound to it looks like it
      is ignoring you.

- [ ] **Persist through the mount.** `/container/mounts` puts data on the router's
      flash where it survives `/container/remove` and every image upgrade. The
      container's own layer does not — that is the whole difference between an
      upgrade and a data loss.


## 6. Budget for the hardware

- [ ] **Flash is small.** A hAP ax² has 128 MB total. Two 15 MB series plus images
      left ~25 MB free here.

- [ ] **Any in-place file rewrite needs the file's size again**, for the temp copy
      before the atomic rename. A 15 MB file needs 15 MB free. Check
      `free-hdd-space` before a migration, and do them one at a time.

- [ ] **Flash has a limited erase budget.** Every flush is a write. Keep flush
      intervals long — an hour, not a second — and never leave a debugging value
      like `flush_interval=1` in place.

- [ ] **`stop_grace_period` matters.** Shutdown flushes buffers. `SIGTERM` is
      handled; `kill -9` loses whatever was buffered.

- [ ] **Logs go to stderr, unbuffered.** `printf` to stdout is block-buffered when
      stdout is a pipe, which is exactly what container logging gives it — a
      warning then sits in the buffer until the process exits, and a hard kill
      loses it entirely.


## 7. Deploy without shipping the wrong thing

- [ ] **Delete the output artifact before building.** A failed build otherwise
      leaves the previous tar looking current, and that is the quiet way to upload
      a stale binary and report success.

- [ ] **Never filter the build log down to its success line.** `set -euo pipefail`
      does not save you: piping to `grep` makes *grep's* exit status the
      pipeline's, so a hard failure prints nothing and reads as fine.

- [ ] **Flush before stopping the container.** Buffered points are not on disk.

- [ ] **A redeploy breaks whatever is talking to the container.** A sibling prober
      here never reconnected on its own after the service went away, and sat
      holding points silently. Check dependents *after* every redeploy, not just
      the service you replaced.

- [ ] **Verify the running build, not the deploy script's exit code.** Ask the
      service for something only the new version can answer. A health check passes
      just as happily against the old binary.


## Failure signatures

| What you see | What it actually is |
|---|---|
| `F  download/extract error: could not load next layer` | OCI tar; repack to the legacy layout (§4) |
| `exec /bin/sh: exec format error` during build | no emulator registered on the *host*, not a bad Dockerfile |
| `bad parameter` with a column number | `mounts=` should be `mountlists=`, or `name=` should be `list=` |
| Container runs, API unreachable from the router | bound to loopback instead of `0.0.0.0` |
| API reachable from the router, not from your desk | missing dst-nat rule |
| SSH key rejected | RouterOS's older SSH; add `PubkeyAcceptedKeyTypes=+ssh-rsa` |
| `scp` hangs or fails oddly | needs `-O` |
| Data gone after an image upgrade | it was in the container layer, not on a mount |
| Container is `H` but a sibling is not talking to it | the sibling did not reconnect; restart it |
