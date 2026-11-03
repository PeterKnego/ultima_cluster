# The injector image: the compiled program and the CLI that attaches it. It runs privileged in the target
# container's network namespace — docker run --rm --privileged --network container:<name> faulteron:local faulteron ...
FROM debian:bookworm-slim AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang libbpf-dev linux-libc-dev make \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Makefile ./
COPY bpf/ bpf/
RUN make

# make install's layout: the CLI finds the program in ../lib/faulteron beside it.
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends bpftool iproute2 jq \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/build/faulteron.bpf.o /usr/local/lib/faulteron/
COPY bin/faulteron /usr/local/bin/
