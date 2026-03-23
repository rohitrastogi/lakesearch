"""DuckDB + LakeSearch Arrow Flight demo.

Shows how the same Parquet files serve both columnar analytics (DuckDB)
and full-text search (LakeSearch via Arrow Flight).

Prerequisites:
  - demo/data/events.parquet exists (run generate_data.py first)
  - LakeSearch query server running with demo/config.yaml
  - pyarrow, duckdb installed
"""

import json
import os
import sys

import duckdb
import pyarrow.flight as flight

PARQUET_PATH = os.path.join(os.path.dirname(__file__), "data", "events.parquet")
FLIGHT_URI = "grpc://localhost:8081"


def make_ticket(table: str, column: str, match: str, select: list[str]) -> flight.Ticket:
    payload = {
        "table": table,
        "column": column,
        "match": match,
        "select": select,
    }
    return flight.Ticket(json.dumps(payload).encode())


def main():
    if not os.path.exists(PARQUET_PATH):
        print(f"ERROR: {PARQUET_PATH} not found. Run generate_data.py first.")
        sys.exit(1)

    conn = duckdb.connect()
    client = flight.connect(FLIGHT_URI)

    print("=" * 70)
    print("Query 1: Pure OLAP — DuckDB scans Parquet directly")
    print("  p99 response time by service")
    print("=" * 70)
    conn.sql(f"""
        SELECT
            service,
            approx_quantile(response_time_ms, 0.99) AS p99_ms,
            count(*) AS total_events
        FROM read_parquet('{PARQUET_PATH}')
        GROUP BY service
        ORDER BY p99_ms DESC
    """).show()

    print()
    print("=" * 70)
    print("Query 2: Full-text search via Flight — 'connection timeout' errors")
    print("  LakeSearch prunes pages, DuckDB aggregates the results")
    print("=" * 70)
    ticket = make_ticket(
        "events", "description", "connection timeout",
        ["service", "response_time_ms", "timestamp"],
    )
    timeout_hits = client.do_get(ticket).to_reader()
    conn.sql("""
        SELECT
            service,
            count(*) AS timeout_errors,
            round(avg(response_time_ms), 0) AS avg_latency_ms,
            max(response_time_ms) AS max_latency_ms
        FROM timeout_hits
        GROUP BY service
        ORDER BY timeout_errors DESC
    """).show()

    print()
    print("=" * 70)
    print("Query 3: Full-text search — all 'error' events by service")
    print("  Demonstrates broader text search + OLAP aggregation")
    print("=" * 70)
    ticket = make_ticket(
        "events", "description", "error",
        ["service", "response_time_ms", "timestamp"],
    )
    error_hits = client.do_get(ticket).to_reader()
    conn.sql("""
        SELECT
            service,
            count(*) AS errors,
            round(avg(response_time_ms), 0) AS avg_latency_ms,
            max(response_time_ms) AS max_latency_ms
        FROM error_hits
        GROUP BY service
        ORDER BY errors DESC
    """).show()

    print()
    print("=" * 70)
    print("Query 4: Text search results joined with Parquet analytics")
    print("  Flight results as a dimension table, Parquet as the fact table")
    print("=" * 70)
    ticket = make_ticket(
        "events", "description", "timeout",
        ["service", "response_time_ms"],
    )
    timeout_results = client.do_get(ticket).to_reader()
    conn.register("timeout_events", timeout_results)

    conn.sql(f"""
        WITH all_events AS (
            SELECT service, count(*) AS total
            FROM read_parquet('{PARQUET_PATH}')
            GROUP BY service
        ),
        timeout_agg AS (
            SELECT service, count(*) AS timeouts, avg(response_time_ms) AS avg_rt
            FROM timeout_events
            GROUP BY service
        )
        SELECT
            a.service,
            a.total AS total_events,
            coalesce(t.timeouts, 0) AS timeout_events,
            round(100.0 * coalesce(t.timeouts, 0) / a.total, 2) AS timeout_pct,
            round(coalesce(t.avg_rt, 0), 0) AS avg_timeout_latency_ms
        FROM all_events a
        LEFT JOIN timeout_agg t ON a.service = t.service
        ORDER BY timeout_pct DESC
    """).show()

    print()
    print("Done! Same Parquet files served both columnar analytics and full-text search.")


if __name__ == "__main__":
    main()
