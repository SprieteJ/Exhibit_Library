async function fetchAltATHDrawdown() {
  spinOn();
  try {
    const data = await fetch('/api/alt-ath-drawdown', {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.points?.length) throw new Error('no data');
    const pts = data.points.slice(0, 30);
    mkChart('bar', {
      labels: pts.map(p => p.symbol),
      datasets: [{ label: 'Drawdown from ATH', data: pts.map(p => p.drawdown_pct),
        backgroundColor: 'rgba(236,91,91,0.7)', borderWidth: 0 }]
    }, {
      indexAxis: 'y',
      scales: {
        x: { max: 0, ticks: { ...XTICK, callback: v => v+'%' }, grid: XGRID },
        y: { ticks: YTICK, grid: YGRID },
      },
      plugins: { legend: { display: false } }
    });
    setTitle('Altcoin Drawdown from ATH (Top 30)', 'Source: CoinGecko · all-time high');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}


// ── Alt: Drawdown Over Time ───────────────────────────────────────────────────
