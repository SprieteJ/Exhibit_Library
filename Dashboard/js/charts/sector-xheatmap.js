async function fetchSectorXHeatmap(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/sector-xheatmap?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (data.error) throw new Error(data.error);
    drawHeatmapCanvas(data.sectors, data.sectors, data.matrix, heatCorr);
    setTitle('Cross-Sector Correlation Heatmap', 'Source: CoinGecko · 30d rolling Pearson');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── Sector: Cumulative Return ─────────────────────────────────────────────────
