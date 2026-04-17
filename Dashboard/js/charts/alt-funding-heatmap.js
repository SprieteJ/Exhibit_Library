async function fetchAltFundingHeatmap(symbols, from, to) {
  spinOn();
  try {
    const syms = symbols.join(',');
    const data = await fetch(`/api/alt-funding-heatmap?symbols=${encodeURIComponent(syms)}&from=${from}&to=${to}`).then(r=>r.json());
    if (data.error) throw new Error(data.error);
    if (!data.symbols?.length) throw new Error('no data');
    const maxAbs = Math.max(...data.symbols.flatMap(s => (data.matrix[s]||[]).filter(v=>v!=null).map(Math.abs)), 0.001);
    const matrix = data.symbols.map(s => data.matrix[s] || []);
    drawHeatmapCanvas(data.symbols, data.dates, matrix, (v) => heatFunding(v, maxAbs));
    setTitle('Altcoin Funding Rate Heatmap', 'Source: Binance/Bybit · 8h avg');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── Macro: Correlation Matrix ─────────────────────────────────────────────────

// ── Macro: Rolling Sharpe Ratio ───────────────────────────────────────────────
