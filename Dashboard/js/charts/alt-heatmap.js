async function fetchAltHeatmap(symbols, from, to) {
  spinOn();
  try {
    const syms = symbols.join(',');
    const data = await fetch(`/api/alt-heatmap?symbols=${encodeURIComponent(syms)}&from=${from}&to=${to}`).then(r=>r.json());
    if (data.error) throw new Error(data.error);
    drawHeatmapCanvas(data.symbols, data.symbols, data.matrix, heatCorr);
    setTitle('Altcoin Correlation Heatmap (30d)', 'Source: CoinGecko · Pearson');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── Alt: Funding Heatmap ──────────────────────────────────────────────────────
