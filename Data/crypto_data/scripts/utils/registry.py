"""
registry.py
-----------
Central asset registry loader. Import this in every ingest/analysis script.
Resolves the latest registry file automatically — just drop new monthly exports
into /registry/ with the standard naming convention and this picks it up.

Usage:
    from scripts.utils.registry import Registry
    reg = Registry()

    # Get a single asset
    asset = reg.get("BTC")              # by symbol
    asset = reg.get("bitcoin")          # by coingecko_id

    # Full universe as DataFrame
    df = reg.universe()

    # Filtered subsets
    l1s    = reg.filter(sector="General Purpose Blockchain Networks")
    memes  = reg.filter(sector="Meme")
    defi   = reg.filter(sector="Decentralized Finance")

    # All coingecko IDs (for API loops)
    ids    = reg.coingecko_ids()
    syms   = reg.symbols()
"""

import re
from pathlib import Path
import pandas as pd


# ── Config ──────────────────────────────────────────────────────────────────
REGISTRY_DIR = Path(__file__).resolve().parents[2] / "registry"
REGISTRY_FILENAME_PATTERN = re.compile(r"^assets-export-(\d{4}-\d{2}-\d{2}).*\.csv$")


# ── Helpers ──────────────────────────────────────────────────────────────────
def _latest_registry_path(directory: Path) -> Path:
    """Return the registry CSV with the most recent date in its filename."""
    candidates = []
    for f in directory.glob("assets-export-*.csv"):
        m = REGISTRY_FILENAME_PATTERN.match(f.name)
        if m:
            candidates.append((m.group(1), f))
    if not candidates:
        raise FileNotFoundError(
            f"No registry files found in {directory}. "
            "Expected pattern: assets-export-YYYY-MM-DD*.csv"
        )
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


# ── Registry class ────────────────────────────────────────────────────────────
class Registry:
    """
    Wraps the asset universe CSV. All other scripts should use this
    rather than reading the CSV directly — it centralises column aliasing
    and gives a single upgrade point when the schema changes.
    """

    # Canonical internal column names → CSV column names
    COLUMN_MAP = {
        "symbol":           "Symbol",
        "market_cap":       "Market Cap (M)",
        "sector":           "Sector",
        "use_case":         "Use Case",
        "sub_use_case":     "Sub Use Case",
        "tech_stack":       "Tech Stack",
        "sub_tech_stack":   "Sub Tech Stack",
        "tertiary_tech":    "Tertiary Tech Stack",
        "ecosystem":        "Ecosystem",
        "region":           "Region",
        "custodies":        "Custodies",
        "coingecko_id":     "CoinGecko ID",
        "coingecko_symbol": "CoinGecko Symbol",
        "coingecko_name":   "CoinGecko Name",
        "cmc_id":           "CoinMarketCap ID",
        "cmc_symbol":       "CoinMarketCap Symbol",
        "cmc_name":         "CoinMarketCap Name",
        "cmc_slug":         "CoinMarketCap Slug",
        "coinmetrics_id":   "CoinMetrics ID",
        "coinmetrics_name": "CoinMetrics Name",
    }

    def __init__(self, path: Path | None = None):
        self._path = path or _latest_registry_path(REGISTRY_DIR)
        self._df = self._load()
        print(f"[Registry] Loaded {len(self._df)} assets from {self._path.name}")

    def _load(self) -> pd.DataFrame:
        df = pd.read_csv(self._path)
        # Rename to canonical internal names
        inv_map = {v: k for k, v in self.COLUMN_MAP.items()}
        df = df.rename(columns=inv_map)
        # Normalise types
        df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")
        df["cmc_id"]     = pd.to_numeric(df["cmc_id"], errors="coerce").astype("Int64")
        df["symbol"]     = df["symbol"].str.upper().str.strip()
        df["coingecko_id"] = df["coingecko_id"].str.strip()
        return df

    # ── Lookups ───────────────────────────────────────────────────────────────
    def get(self, identifier: str) -> pd.Series | None:
        """Fetch a single asset by symbol or coingecko_id. Returns None if not found."""
        identifier = identifier.strip()
        # Try symbol first (uppercase match)
        mask = self._df["symbol"] == identifier.upper()
        if mask.any():
            return self._df[mask].iloc[0]
        # Try coingecko_id (lowercase match)
        mask = self._df["coingecko_id"] == identifier.lower()
        if mask.any():
            return self._df[mask].iloc[0]
        return None

    # ── Filtered views ────────────────────────────────────────────────────────
    def universe(self) -> pd.DataFrame:
        """Full universe as a DataFrame."""
        return self._df.copy()

    def filter(self, **kwargs) -> pd.DataFrame:
        """
        Filter by any canonical column name.
        e.g. reg.filter(sector="Meme")
             reg.filter(ecosystem="Solana")
             reg.filter(tech_stack="Blockchain Network", sub_tech_stack="Layer 1")
        """
        df = self._df
        for col, val in kwargs.items():
            df = df[df[col].str.contains(val, na=False, case=False)]
        return df.copy()

    # ── ID lists (for API loops) ──────────────────────────────────────────────
    def coingecko_ids(self) -> list[str]:
        return self._df["coingecko_id"].dropna().tolist()

    def symbols(self) -> list[str]:
        return self._df["symbol"].dropna().tolist()

    def symbol_to_coingecko(self) -> dict[str, str]:
        return dict(zip(self._df["symbol"], self._df["coingecko_id"]))

    def coingecko_to_symbol(self) -> dict[str, str]:
        return dict(zip(self._df["coingecko_id"], self._df["symbol"]))

    # ── Meta ──────────────────────────────────────────────────────────────────
    @property
    def path(self) -> Path:
        return self._path

    @property
    def version_date(self) -> str:
        """Returns the date embedded in the filename, e.g. '2026-02-20'."""
        m = REGISTRY_FILENAME_PATTERN.match(self._path.name)
        return m.group(1) if m else "unknown"

    def __len__(self):
        return len(self._df)

    def __repr__(self):
        return f"<Registry v{self.version_date} | {len(self)} assets>"
