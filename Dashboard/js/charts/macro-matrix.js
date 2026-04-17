async function fetchMacroMatrix(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/macro-matrix?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (data.error) throw new Error(data.error);
    drawHeatmapCanvas(data.macro_tickers, data.crypto_sectors, data.matrix, heatCorr);
    setTitle('Macro vs Crypto Sector Correlation', 'Source: CoinGecko + Yahoo Finance · 30d');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── Macro: DXY vs BTC ─────────────────────────────────────────────────────────
