async function fetchAltAltseason(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/alt-altseason?from=${from}&to=${to}&window=30`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error(`no data [${data.reason||'empty_result'} from=${from} to=${to}]`);
    mkChart('line', {
      labels: data.dates,
      datasets: [
        { label: 'Alts outperforming BTC (%)', data: data.values,
          borderColor: '#4FC3F7', backgroundColor: 'rgba(79,195,247,0.15)',
          fill: true, pointRadius: 0, borderWidth: 2, tension: 0.1 },
        { label: 'BTC Dominance (%)', data: data.btc_dominance,
          borderColor: '#F7931A', backgroundColor: 'transparent',
          pointRadius: 0, borderWidth: 1.5, tension: 0, yAxisID: 'y2' },
      ]
    }, {
      scales: {
        x:  { ticks: { ...XTICK, maxTicksLimit: 8 }, grid: XGRID },
        y:  { min: 0, max: 100, ticks: { ...YTICK, callback: v => v+'%' }, grid: YGRID },
        y2: { position: 'right', min: 0, max: 100, ticks: { ...YTICK, color: '#F7931A', callback: v => v.toFixed(0)+'%' }, grid: { display: false } },
      },
      plugins: { legend: { display: true, labels: { color: '#aaa', font: { family: 'monospace', size: 11 } } } }
    });
    setTitle('Altseason Index — Alts vs BTC (90d window)', 'Source: CoinGecko · top 50 ex-BTC');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── Alt: Beta vs BTC ──────────────────────────────────────────────────────────
