"""
update_registry.py
------------------
Drop a new monthly asset export into /registry/ and run this script.
It validates the new file, archives the old one, and prints a diff
of added / removed / changed assets.

Usage:
    python scripts/utils/update_registry.py --new registry/assets-export-2026-03-20.csv
"""

from __future__ import annotations

import argparse
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.utils.registry import Registry, REGISTRY_DIR, REGISTRY_FILENAME_PATTERN


def diff_registries(old_df: pd.DataFrame, new_df: pd.DataFrame) -> None:
    """Print a human-readable diff between two registry DataFrames."""
    old_syms = set(old_df["symbol"])
    new_syms = set(new_df["symbol"])

    added   = new_syms - old_syms
    removed = old_syms - new_syms
    common  = old_syms & new_syms

    print(f"\n{'─'*50}")
    print(f"  Registry diff")
    print(f"{'─'*50}")
    print(f"  Total assets  : {len(old_syms)} → {len(new_syms)}")
    print(f"  Added         : {len(added)}")
    print(f"  Removed       : {len(removed)}")

    if added:
        print(f"\n  ++ Added assets:")
        for s in sorted(added):
            row = new_df[new_df["symbol"] == s].iloc[0]
            print(f"     {s:12s}  {row.get('sector', '')} / {row.get('coingecko_id', '')}")

    if removed:
        print(f"\n  -- Removed assets:")
        for s in sorted(removed):
            row = old_df[old_df["symbol"] == s].iloc[0]
            print(f"     {s:12s}  {row.get('sector', '')} / {row.get('coingecko_id', '')}")

    # Check for sector/coingecko_id changes in common assets
    changes = []
    old_idx = old_df.set_index("symbol")
    new_idx = new_df.set_index("symbol")
    for sym in common:
        for col in ["sector", "coingecko_id", "ecosystem", "tech_stack"]:
            if col not in old_idx.columns or col not in new_idx.columns:
                continue
            ov = old_idx.at[sym, col]
            nv = new_idx.at[sym, col]
            if str(ov) != str(nv):
                changes.append((sym, col, ov, nv))

    if changes:
        print(f"\n  ~~ Changed fields ({len(changes)}):")
        for sym, col, ov, nv in changes[:20]:  # cap at 20
            print(f"     {sym:12s}  {col}: '{ov}' → '{nv}'")
        if len(changes) > 20:
            print(f"     ... and {len(changes) - 20} more")

    print(f"{'─'*50}\n")


def run(new_path_str: str):
    new_path = Path(new_path_str).resolve()

    # Validate filename pattern
    if not REGISTRY_FILENAME_PATTERN.match(new_path.name):
        raise ValueError(
            f"Filename '{new_path.name}' does not match expected pattern: "
            "assets-export-YYYY-MM-DD*.csv"
        )

    # Validate the new file loads cleanly
    try:
        new_df = pd.read_csv(new_path)
        print(f"[update_registry] New file: {new_path.name} ({len(new_df)} assets)")
    except Exception as e:
        raise ValueError(f"Could not read new registry file: {e}")

    required_cols = ["Symbol", "CoinGecko ID"]
    missing = [c for c in required_cols if c not in new_df.columns]
    if missing:
        raise ValueError(f"New registry missing required columns: {missing}")

    # Load current registry for diff
    try:
        old_reg = Registry()
        old_df  = old_reg.universe()
        old_path = old_reg.path
    except FileNotFoundError:
        print("[update_registry] No existing registry — this is the first load.")
        old_df   = pd.DataFrame()
        old_path = None

    # Diff
    if not old_df.empty:
        # Rename new_df columns to canonical for diff
        from scripts.utils.registry import Registry as R
        inv_map = {v: k for k, v in R.COLUMN_MAP.items()}
        new_df_canonical = new_df.rename(columns=inv_map)
        diff_registries(old_df, new_df_canonical)

    # Archive old registry
    if old_path and old_path.exists():
        archive_dir = REGISTRY_DIR / "_archive"
        archive_dir.mkdir(exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        archive_name = f"archived_{ts}_{old_path.name}"
        shutil.move(str(old_path), str(archive_dir / archive_name))
        print(f"[update_registry] Archived old registry → _archive/{archive_name}")

    # Copy new file into registry dir (if not already there)
    dest = REGISTRY_DIR / new_path.name
    if new_path.resolve() != dest.resolve():
        shutil.copy2(str(new_path), str(dest))
        print(f"[update_registry] Copied new registry → registry/{new_path.name}")
    else:
        print(f"[update_registry] File already in registry/")

    print(f"[update_registry] Done. Active registry: {new_path.name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--new", required=True, help="Path to the new registry CSV")
    args = parser.parse_args()
    run(args.new)
