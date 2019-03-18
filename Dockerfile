FROM rust:1.33-slim
WORKDIR /app

COPY . ./

RUN cargo build --release

CMD [ "/app/target/release/ilprelay" ]
