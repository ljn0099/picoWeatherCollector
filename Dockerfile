# Stage 1 builder
FROM docker.io/library/alpine:3.22 AS builder

RUN apk add --no-cache \
    build-base \
    cmake \
    pkgconf \
    protobuf-dev \
    protobuf-c-dev \
    protobuf-c-compiler \
    libpq-dev \
    libsodium-dev \
    mosquitto-dev

WORKDIR /picoWeatherCollector

COPY src ./src
COPY CMakeLists.txt .

RUN mkdir build && cd build && cmake .. && make -j$(nproc)

# Stage 2 runtime
FROM docker.io/library/eclipse-mosquitto:2-openssl

RUN apk add --no-cache \
    libpq \
    libsodium \
    protobuf-c

RUN mkdir -p /usr/lib/mosquitto/plugins/
COPY --from=builder /picoWeatherCollector/build/picoWeatherCollector.so /usr/lib/mosquitto/plugins/
RUN chown mosquitto:mosquitto /usr/lib/mosquitto/plugins/picoWeatherCollector.so

COPY mosquitto.conf /mosquitto/config/mosquitto.conf

COPY picoWeatherCollector.sh /picoWeatherCollector.sh
RUN chmod +x /picoWeatherCollector.sh

CMD ["/picoWeatherCollector.sh"]
