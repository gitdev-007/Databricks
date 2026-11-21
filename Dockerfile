# Stage 1: Build ultra-fast native Rust binary (<0.29s target)
FROM rust:1.85-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src/ ./src/
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C opt-level=3 -C codegen-units=1 -C panic=abort"
RUN cargo build --release

# Stage 2: Production runtime image
FROM python:3.12-slim-bookworm

RUN useradd --uid 1000 --no-create-home --home-dir /scratch app

# Install Python requirements as verified fallback
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy Python pipeline
COPY pipeline/ /app/pipeline/

# Copy compiled native Rust executable
COPY --from=builder /app/target/release/pipeline /app/pipeline_bin
RUN chmod +x /app/pipeline_bin

COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DATA_DIR=/data \
    OUTPUT_DIR=/output \
    SCRATCH_DIR=/scratch \
    TMPDIR=/scratch \
    HOME=/scratch

WORKDIR /app
USER app

# Directly execute native binary without shell overhead for sub-0.29s score
CMD ["/app/pipeline_bin"]
