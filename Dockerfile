FROM rust:1.33-slim
WORKDIR /app

COPY . ./

RUN apt-get update
RUN apt-get install -y --no-install-recommends pkgconf libssl-dev
RUN cargo build --release

CMD [ "/app/target/release/ilprelay" ]
