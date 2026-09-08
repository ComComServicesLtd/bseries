# bseries on RouterOS. A starting point rather than something to apply unchanged.
#
# Build and upload the image first:
#
#   ./tools/build-mikrotik.sh                 # writes bseries-arm64.tar
#   scp -O bseries-arm64.tar <router>:
#
# Then edit the values below and paste this in, or upload it and run
# `/import mikrotik.example.rsc`.
#
# Needs the container package, and container support enabled:
#
#   /system/device-mode/update container=yes
#
# which asks for a physical confirmation (press the reset button, or power
# cycle) and is a one time step per router.

:local ctrName    "bseries"
:local ctrTar     "bseries-arm64.tar"
:local ctrBridge  "containers"          ;# an existing bridge for container veths
:local ctrAddr    "172.20.0.3/24"       ;# free address on that bridge
:local ctrGw      "172.20.0.1"          ;# the router's address on it
:local ctrDir     "/containers/bseries" ;# where the image is unpacked
:local dataDir    "/bseries-data"       ;# database files, on the router's flash
:local routerAddr "172.16.220.131"      ;# address the API is reached on
:local apiPort    "8086"

# --- network -----------------------------------------------------------------
# The veth is one end of a pair; the container gets the other. It stays down
# until a container is bound to it, so an inactive bridge port here is expected.

/interface/veth/add name=$ctrName address=$ctrAddr gateway=$ctrGw \
    comment="bseries tsdb"

/interface/bridge/port/add bridge=$ctrBridge interface=$ctrName \
    comment="bseries tsdb"

# Egress, so the container can reach a time source or a remote it pushes to.
# Skip it if a masquerade rule already covers this subnet.
# /ip/firewall/nat/add chain=srcnat action=masquerade \
#     src-address=172.20.0.0/24 out-interface=ether1

# --- storage -----------------------------------------------------------------
# Mounted rather than left in the container's own layer, so the database
# outlives /container/remove and an image upgrade. RouterOS creates the source
# directory on first start.
#
# The mount is also why the image runs as root: RouterOS owns this directory as
# root and offers no chown, so a non-root container could not write its own
# database. See the USER note in Dockerfile.mikrotik.

/container/mounts/add list=bseries-data src=$dataDir dst="/data" mode=rw

# --- settings ----------------------------------------------------------------
# Every setting the configuration file understands has a BSERIES_ twin, so
# nothing needs baking into the image. The image already sets BSERIES_LISTEN,
# BSERIES_PORT and BSERIES_DATA_DIRECTORY.
#
# An hour between flushes suits flash storage: RouterOS routers write to eMMC
# with a limited erase budget, and every flush is a write.

/container/envs/add list=bseries key=BSERIES_FLUSH_INTERVAL   value="3600"
/container/envs/add list=bseries key=BSERIES_WRITE_AHEAD_SIZE value="1024"
/container/envs/add list=bseries key=BSERIES_SERIES_MAX_IDLE  value="900"

# --- the container -----------------------------------------------------------
# mountlists and envlist are the parameter names RouterOS takes here; `mounts`
# is rejected. The container's name is taken from the image tag, so this adds
# one called bseries-arm64 rather than $ctrName.

/container/add file=$ctrTar interface=$ctrName root-dir=$ctrDir \
    envlist=bseries mountlists=bseries-data \
    start-on-boot=yes logging=yes comment="bseries tsdb"

:delay 5s
/container/start [find where root-dir=$ctrDir]

# --- reaching it -------------------------------------------------------------
# The container is on a private bridge subnet, so the API needs forwarding to be
# reachable from elsewhere. There is no TLS and no authentication until the
# first key is minted, so forward it only where that is acceptable, and prefer
# an address list or an in-interface match over the whole world.

/ip/firewall/nat/add chain=dstnat action=dst-nat protocol=tcp \
    dst-address=$routerAddr dst-port=$apiPort \
    to-addresses=[:pick $ctrAddr 0 [:find $ctrAddr "/"]] to-ports=$apiPort \
    comment="bseries tsdb"

# --- first key ---------------------------------------------------------------
# On a first run the database has no write key and prints a one time bootstrap
# token to the log. Read it with:
#
#   /log/print where topics~"container"
#
# then mint the first key from a host that can reach the API:
#
#   curl -XPOST -H 'X-API-Key: <token>' \
#     'http://172.16.220.131:8086/v1/auth/keys?role=write&name=admin'
#
# The token works only for that, and only until a write key exists.
