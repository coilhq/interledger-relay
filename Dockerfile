FROM rust:1.43-slim
WORKDIR /app

COPY . ./

RUN apt-get update
# These packages are required by rust-openssl (via hyper_tls).
RUN apt-get install -y --no-install-recommends pkgconf libssl-dev
RUN cargo build --release

CMD [ "/app/target/release/ilprelay" ]
