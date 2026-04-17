async function fetchBtc200wFloor(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/btc-200w-floor?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    mkChart('line', {
      labels: data.dates,
      datasets: [
        { label: 'BTC Price', data: data.price, borderColor: '#F7931A', backgroundColor: 'transparent', borderWidth: 1.6, pointRadius: 0, tension: 0.1 },
        { label: '200-Week MA', data: data.ma200w, borderColor: '#EC5B5B', backgroundColor: 'rgba(236,91,91,0.05)', borderWidth: 1.8, pointRadius: 0, tension: 0.1, borderDash: [6, 4], fill: true },
      ]
    }, {
      scales: {
        x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 8, callback: function(v) { const l = this.getLabelForValue(v); return l ? l.slice(0,7) : ''; } }, grid: XGRID },
        y: { type: 'logarithmic', ticks: { ...YTICK, callback: v => '$' + (v >= 1000 ? (v/1000).toFixed(0)+'k' : v.toFixed(0)) }, grid: YGRID },
      },
      plugins: { legend: { display: true, labels: { color: '#888', font: { size: 11 }, boxWidth: 12 } } },
    });
    setTitle('BTC Price vs 200-Week MA', 'Source: CoinGecko · log scale · the macro floor');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── BTC: 200d MA Deviation ────────────────────────────────────────────────────
