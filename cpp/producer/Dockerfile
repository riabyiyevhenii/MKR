# syntax=docker/dockerfile:1
FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake g++ make librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY cpp/producer/CMakeLists.txt cpp/producer/main.cpp ./
RUN cmake -DCMAKE_BUILD_TYPE=Release . && make -j"$(nproc)"

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        librdkafka1 librdkafka++1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /build/producer .
ENTRYPOINT ["./producer"]
