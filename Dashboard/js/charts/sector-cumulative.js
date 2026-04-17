async function fetchSectorCumulative(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/sector-cumulative?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (data.error) throw new Error(data.error);
    const entries = Object.entries(data).sort((a, b) => b[1].value - a[1].value);
    mkChart('bar', {
      labels: entries.map(e => e[0]),
      datasets: [{ label: 'Cumulative Return', data: entries.map(e => e[1].value),
        backgroundColor: entries.map(e => e[1].value >= 0 ? (e[1].color+'cc') : 'rgba(236,91,91,0.8)'),
        borderWidth: 0 }]
    }, {
      scales: {
        x: { ticks: XTICK, grid: XGRID },
        y: { ticks: { ...YTICK, callback: v => v.toFixed(1)+'%' }, grid: YGRID },
      },
      plugins: { legend: { display: false } }
    });
    setTitle('Sector Cumulative Returns', 'Source: CoinGecko · equal-weighted');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── Sector: Sharpe / Risk-Return ──────────────────────────────────────────────
