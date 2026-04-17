async function fetchSectorDominance(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/sector-dominance?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (data.error) throw new Error(data.error);
    const sectors = Object.keys(data);
    if (!sectors.length) throw new Error('no data');
    mkChart('line', {
      labels: data[sectors[0]].dates,
      datasets: sectors.map(s => ({
        label: s, data: data[s].values,
        borderColor: data[s].color, backgroundColor: data[s].color + '88',
        fill: true, pointRadius: 0, borderWidth: 1, tension: 0.1,
      }))
    }, {
      scales: {
        x: { ticks: XTICK, grid: XGRID },
        y: { stacked: true, min: 0, max: 100, ticks: { ...YTICK, callback: v => v + '%' }, grid: YGRID },
      },
      plugins: { legend: { display: true, labels: { color: '#aaa', font: { family: 'monospace', size: 11 } } } }
    });
    setTitle('Sector Market Cap Dominance (%)', 'Source: CoinGecko · mcap weighted');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── Sector: Cross-Heatmap ─────────────────────────────────────────────────────
