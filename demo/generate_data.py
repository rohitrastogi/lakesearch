"""Generate realistic event Parquet data for the LakeSearch demo.

Creates demo/data/events.parquet with ~10K rows of synthetic log events.
Dependencies: pyarrow
"""

import random
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq

SERVICES = [
    "api-gateway",
    "auth-service",
    "payment-service",
    "user-service",
    "notification-service",
    "storage-service",
]

# (template, is_error) — templates use {service} and {detail} placeholders
TEMPLATES = [
    # Errors / failures
    ("connection timeout after {ms}ms to upstream {service}", True),
    ("connection refused by {service} on port 443", True),
    ("error processing request: {detail}", True),
    ("failed to authenticate token: {detail}", True),
    ("database query timeout after {ms}ms: {detail}", True),
    ("out of memory: heap allocation failed in {service}", True),
    ("TLS handshake error: certificate expired for {service}", True),
    ("request failed with status 500: internal server error", True),
    ("request failed with status 502: bad gateway from {service}", True),
    ("request failed with status 503: service unavailable", True),
    ("disk write error: no space left on device", True),
    ("connection reset by peer during data transfer", True),
    # Warnings / slow
    ("slow query detected: {ms}ms for SELECT on users table", False),
    ("high memory usage warning: {pct}% heap utilization", False),
    ("request latency spike: p99 at {ms}ms for /api/v1/users", False),
    ("retry attempt 3 of 5 for {service} health check", False),
    ("connection pool exhausted, waiting for available connection", False),
    ("rate limit approaching: {pct}% of quota consumed", False),
    # Success / normal
    ("request completed successfully in {ms}ms", False),
    ("health check passed for {service}", False),
    ("cache hit for session token lookup", False),
    ("processed batch of {count} events in {ms}ms", False),
    ("user login successful from {ip}", False),
    ("payment processed: transaction {txn} completed", False),
    ("notification delivered via email to {count} recipients", False),
    ("file uploaded successfully: {size}KB in {ms}ms", False),
    ("database connection established in {ms}ms", False),
    ("background job completed: cleanup of expired sessions", False),
]

ERROR_DETAILS = [
    "null pointer exception in handler chain",
    "invalid JSON payload in request body",
    "foreign key constraint violation on orders table",
    "maximum retry count exceeded",
    "circuit breaker open for downstream service",
    "request payload exceeds 10MB limit",
    "invalid UTF-8 sequence in header value",
    "deadlock detected in transaction",
]

# Services weighted toward more error-heavy traffic
SERVICE_ERROR_WEIGHTS = {
    "api-gateway": 0.15,
    "auth-service": 0.20,
    "payment-service": 0.25,
    "user-service": 0.10,
    "notification-service": 0.08,
    "storage-service": 0.18,
}

NUM_ROWS = 10_000
PAGE_SIZE = 500
SEED = 42

# Precompute template lists to avoid rebuilding on every row
ERROR_TEMPLATES = [(t, e) for t, e in TEMPLATES if e]
NON_ERROR_TEMPLATES = [(t, e) for t, e in TEMPLATES if not e]


def generate_row(rng: random.Random, base_time: datetime) -> tuple:
    service = rng.choice(SERVICES)
    error_rate = SERVICE_ERROR_WEIGHTS[service]

    if rng.random() < error_rate:
        template, is_error = rng.choice(ERROR_TEMPLATES)
    else:
        template, is_error = rng.choice(NON_ERROR_TEMPLATES)

    # Response time: errors are slower
    if is_error:
        response_time = int(rng.gauss(800, 400))
        response_time = max(100, min(response_time, 5000))
    else:
        response_time = int(rng.gauss(150, 80))
        response_time = max(5, min(response_time, 2000))

    # Fill template placeholders. {service} in templates refers to an
    # upstream dependency (not necessarily this row's own service).
    description = template.format(
        service=rng.choice(SERVICES),
        detail=rng.choice(ERROR_DETAILS),
        ms=response_time,
        pct=rng.randint(70, 99),
        count=rng.randint(10, 5000),
        ip=f"10.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(1,254)}",
        txn=f"txn_{rng.randint(100000, 999999)}",
        size=rng.randint(1, 50000),
    )

    # Timestamp: random within last 24 hours
    offset_secs = rng.randint(0, 86400)
    timestamp = base_time - timedelta(seconds=offset_secs)

    return timestamp, service, description, response_time


def main():
    import os

    out_dir = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "events.parquet")

    rng = random.Random(SEED)
    base_time = datetime.now(tz=timezone.utc)

    timestamps = []
    services = []
    descriptions = []
    response_times = []

    for _ in range(NUM_ROWS):
        ts, svc, desc, rt = generate_row(rng, base_time)
        timestamps.append(ts)
        services.append(svc)
        descriptions.append(desc)
        response_times.append(rt)

    table = pa.table(
        {
            "timestamp": pa.array(timestamps, type=pa.timestamp("us", tz="UTC")),
            "service": pa.array(services, type=pa.string()),
            "description": pa.array(descriptions, type=pa.string()),
            "response_time_ms": pa.array(response_times, type=pa.int32()),
        }
    )

    pq.write_table(
        table,
        out_path,
        row_group_size=PAGE_SIZE,
        data_page_size=PAGE_SIZE * 50,  # small pages for pruning demo
        write_statistics=True,
        write_page_index=True,
    )

    print(f"Wrote {NUM_ROWS} rows to {out_path}")
    print(f"  Row groups: {NUM_ROWS // PAGE_SIZE}")
    print(f"  Columns: timestamp, service, description, response_time_ms")

    # Quick sanity check
    meta = pq.read_metadata(out_path)
    print(f"  File size: {os.path.getsize(out_path) / 1024:.0f} KB")
    print(f"  Row groups in file: {meta.num_row_groups}")


if __name__ == "__main__":
    main()
