async function fetchBtcBull() {
  spinOn();
  try {
    const data = await fetch('/api/btc-bull?days=1000', {signal: _navAbort?.signal}).then(r=>r.json());
    if (data.error) throw new Error(data.error);
    const labels = Object.keys(data);
    if (!labels.length) throw new Error('no data');
    const COLS = ['#F7931A', '#00D64A', '#4FC3F7'];
    mkChart('line', {
      datasets: labels.map((lbl, i) => ({
        label: lbl,
        data: data[lbl].days.map((d, j) => ({ x: d, y: data[lbl].values[j] })),
        borderColor: COLS[i % COLS.length], backgroundColor: 'transparent',
        pointRadius: 0, borderWidth: 2, tension: 0,
      }))
    }, {
      scales: {
        x: { type: 'linear', title: { display: true, text: 'Days from trough', color: '#888' }, ticks: XTICK, grid: XGRID },
        y: { type: 'logarithmic', title: { display: true, text: 'Index (trough=100)', color: '#888' }, ticks: YTICK, grid: YGRID },
      },
      plugins: { legend: { display: true, labels: { color: '#aaa', font: { family: 'monospace', size: 11 } } } }
    });
    setTitle('BTC Bull Cycles — Indexed to Trough', 'Source: CoinGecko · log scale');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── BTC: Realized Volatility ──────────────────────────────────────────────────
