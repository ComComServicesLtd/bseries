# bseries as a container.
#
# The project takes no dependencies, so the runtime image can be scratch: one
# static binary and an empty data directory, nothing else to patch or audit.
# Building on alpine rather than a glibc image is deliberate — glibc's
# getaddrinfo wants its shared libraries back at run time even in a static
# binary, and musl's does not.

FROM alpine:3.20 AS build

RUN apk add --no-cache g++ make

WORKDIR /src
COPY . .

RUN make clean && make static && strip bseriesd

# Prepared here because scratch has no shell to mkdir with, and the ownership
# has to match the numeric user the final stage runs as.
RUN mkdir -p /prepared/data && chown -R 10001:10001 /prepared/data


FROM scratch

COPY --from=build /src/bseriesd /bseriesd
COPY --from=build --chown=10001:10001 /prepared/data /data

# Numeric because scratch has no /etc/passwd to resolve a name against.
USER 10001:10001

# 0.0.0.0 rather than the loopback default: inside a container the loopback is
# only reachable from the container itself, so the shipped default would look
# like the server was ignoring you.
ENV BSERIES_LISTEN=0.0.0.0 \
    BSERIES_PORT=8086 \
    BSERIES_DATA_DIRECTORY=/data

EXPOSE 8086
VOLUME ["/data"]

# The binary checks itself, so the image does not have to carry an HTTP client
# purely to be health checked.
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD ["/bseriesd", "--health"]

ENTRYPOINT ["/bseriesd"]
