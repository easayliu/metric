FROM rust:1-slim AS builder
WORKDIR /src

# 先只用一个空壳 crate 把依赖编出来，单独占一层。只要 Cargo.toml / Cargo.lock 没动，
# 这层就能命中 buildcache，改 src 不用重编一遍所有依赖。
# 注意这一层是 registry 缓存必须配 mode=max 的原因：它在中间阶段里，
# 默认的 min 模式只导出最终阶段的层，压根不会把它带上。
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo 'fn main() {}' > src/main.rs \
    && touch src/lib.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src ./src
# cargo 按 mtime 判断新旧，COPY 进来的文件可能比空壳产物还旧，
# 不 touch 一遍它会以为已经编好了，直接把空壳二进制交出去。
RUN find src -name '*.rs' -exec touch {} + \
    && cargo build --release --locked --bin metricpipe

FROM debian:stable-slim
# reqwest 用 rustls，只需要根证书
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/metricpipe /usr/local/bin/metricpipe
EXPOSE 4317 4318
ENTRYPOINT ["metricpipe"]
CMD ["/etc/metricpipe/metricpipe.yaml"]
