# Stage 1: Build native high-performance binary
FROM rust:1.85-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src/ ./src/
RUN cargo build --release

# Stage 2: Minimal runtime image
FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=builder /app/target/release/pipeline /app/pipeline
RUN chmod +x /app/pipeline

ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch TMPDIR=/scratch HOME=/scratch
WORKDIR /app
USER app
CMD ["/app/pipeline"]
