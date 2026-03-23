"""
migrate_to_postgres.py
----------------------
One-shot migration from local CSV lake → PostgreSQL.
Run this when you're ready to move off flat files.

Prerequisites:
    pip install psycopg2-binary sqlalchemy

Usage:
    # Migrate everything
    python scripts/migrate/migrate_to_postgres.py --dsn "postgresql://user:pass@host:5432/dbname"

    # Migrate specific tables only
    python scripts/migrate/migrate_to_postgres.py --dsn "..." --tables price_daily funding_8h

    # Dry run (shows what would happen, writes nothing)
    python scripts/migrate/migrate_to_postgres.py --dsn "..." --dry-run

Schema strategy:
    - Each CSV table → one Postgres table (same name)
    - Composite primary keys defined in TABLE_KEYS
    - Timestamps stored as TIMESTAMPTZ
    - Numeric columns auto-detected by pandas/SQLAlchemy
    - The asset_registry table is also created from the latest registry CSV
    - All tables include source + ingested_at audit columns
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.utils.registry import Registry
from scripts.utils.storage import TABLE_PATHS, TABLE_KEYS


# ── Postgres type hints ───────────────────────────────────────────────────────
# sqlalchemy will infer most types; we only override the ones that need precision
COLUMN_TYPE_OVERRIDES: dict[str, str] = {
    "timestamp":      "TIMESTAMPTZ",
    "snapshot_date":  "DATE",
    "ingested_at":    "TIMESTAMPTZ",
    "price_usd":      "DOUBLE PRECISION",
    "market_cap_usd": "DOUBLE PRECISION",
    "volume_usd":     "DOUBLE PRECISION",
    "funding_rate":   "DOUBLE PRECISION",
    "open_interest":  "DOUBLE PRECISION",
    "iv":             "DOUBLE PRECISION",
    "delta":          "DOUBLE PRECISION",
    "strike":         "DOUBLE PRECISION",
}


def get_create_table_sql(table_name: str, df: pd.DataFrame) -> str:
    """Generate a CREATE TABLE IF NOT EXISTS statement from a DataFrame."""
    col_defs = []
    for col in df.columns:
        if col in COLUMN_TYPE_OVERRIDES:
            pg_type = COLUMN_TYPE_OVERRIDES[col]
        elif pd.api.types.is_integer_dtype(df[col]):
            pg_type = "BIGINT"
        elif pd.api.types.is_float_dtype(df[col]):
            pg_type = "DOUBLE PRECISION"
        else:
            pg_type = "TEXT"
        col_defs.append(f"    {col} {pg_type}")

    keys = TABLE_KEYS.get(table_name, [])
    valid_keys = [k for k in keys if k in df.columns]
    if valid_keys:
        col_defs.append(f"    PRIMARY KEY ({', '.join(valid_keys)})")

    return (
        f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        + ",\n".join(col_defs)
        + "\n);"
    )


def migrate_table(conn, table_name: str, path: Path, dry_run: bool = False):
    """Read a CSV and upsert into Postgres."""
    if not path.exists():
        print(f"  [skip] {table_name}: file not found")
        return 0

    df = pd.read_csv(path)
    if df.empty:
        print(f"  [skip] {table_name}: empty file")
        return 0

    create_sql = get_create_table_sql(table_name, df)

    if dry_run:
        print(f"\n  [dry-run] {table_name}: {len(df)} rows")
        print(f"  SQL:\n{create_sql}\n")
        return len(df)

    cursor = conn.cursor()
    cursor.execute(create_sql)
    conn.commit()

    # Build upsert SQL
    cols = list(df.columns)
    keys = [k for k in TABLE_KEYS.get(table_name, []) if k in cols]
    non_keys = [c for c in cols if c not in keys]

    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(cols)

    if keys and non_keys:
        conflict_clause = f"ON CONFLICT ({', '.join(keys)}) DO UPDATE SET " + ", ".join(
            f"{c} = EXCLUDED.{c}" for c in non_keys
        )
    else:
        conflict_clause = "ON CONFLICT DO NOTHING"

    upsert_sql = (
        f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders}) "
        f"{conflict_clause}"
    )

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cursor.executemany(upsert_sql, rows)
    conn.commit()
    cursor.close()

    print(f"  [ok] {table_name}: {len(df)} rows upserted")
    return len(df)


def migrate_registry(conn, dry_run: bool = False):
    """Migrate the asset registry as its own table: asset_registry."""
    reg = Registry()
    df = reg.universe()

    create_sql = (
        "CREATE TABLE IF NOT EXISTS asset_registry (\n"
        "    symbol           TEXT PRIMARY KEY,\n"
        "    market_cap       DOUBLE PRECISION,\n"
        "    sector           TEXT,\n"
        "    use_case         TEXT,\n"
        "    sub_use_case     TEXT,\n"
        "    tech_stack       TEXT,\n"
        "    sub_tech_stack   TEXT,\n"
        "    tertiary_tech    TEXT,\n"
        "    ecosystem        TEXT,\n"
        "    region           TEXT,\n"
        "    custodies        TEXT,\n"
        "    coingecko_id     TEXT UNIQUE,\n"
        "    coingecko_symbol TEXT,\n"
        "    coingecko_name   TEXT,\n"
        "    cmc_id           BIGINT,\n"
        "    cmc_symbol       TEXT,\n"
        "    cmc_name         TEXT,\n"
        "    cmc_slug         TEXT,\n"
        "    coinmetrics_id   TEXT,\n"
        "    coinmetrics_name TEXT,\n"
        "    registry_version DATE\n"
        ");"
    )

    df["registry_version"] = reg.version_date

    if dry_run:
        print(f"\n  [dry-run] asset_registry: {len(df)} rows")
        print(f"  SQL:\n{create_sql}\n")
        return

    cursor = conn.cursor()
    cursor.execute(create_sql)
    conn.commit()

    cols = list(df.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(cols)
    non_keys = [c for c in cols if c != "symbol"]
    conflict_clause = "ON CONFLICT (symbol) DO UPDATE SET " + ", ".join(
        f"{c} = EXCLUDED.{c}" for c in non_keys
    )
    upsert_sql = f"INSERT INTO asset_registry ({col_list}) VALUES ({placeholders}) {conflict_clause}"

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cursor.executemany(upsert_sql, rows)
    conn.commit()
    cursor.close()

    print(f"  [ok] asset_registry: {len(df)} rows upserted")


# ── CLI ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Migrate CSV data lake to PostgreSQL")
    parser.add_argument("--dsn",      required=True, help="PostgreSQL DSN string")
    parser.add_argument("--tables",   nargs="*",     help="Specific tables to migrate (default: all)")
    parser.add_argument("--dry-run",  action="store_true")
    args = parser.parse_args()

    tables_to_migrate = args.tables or list(TABLE_PATHS.keys())

    if args.dry_run:
        print("[migrate] DRY RUN — no data will be written\n")
        conn = None
    else:
        import psycopg2
        conn = psycopg2.connect(args.dsn)
        print(f"[migrate] Connected to Postgres\n")

    # Migrate asset registry first (other tables reference coingecko_id)
    print("── asset_registry ──────────────────────────────")
    migrate_registry(conn, dry_run=args.dry_run)

    # Migrate data tables
    for table in tables_to_migrate:
        print(f"── {table} ──────────────────────────────")
        path = TABLE_PATHS.get(table)
        if path is None:
            print(f"  [skip] unknown table: {table}")
            continue
        migrate_table(conn, table, path, dry_run=args.dry_run)

    if conn:
        conn.close()
    print("\n[migrate] Done.")


if __name__ == "__main__":
    main()
