async function fetchBtcPiCycle(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/btc-pi-cycle?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    mkChart('line', {
      labels: data.dates,
      datasets: [
        { label: 'BTC Price', data: data.price, borderColor: '#F7931A', backgroundColor: 'transparent', borderWidth: 1.4, pointRadius: 0, tension: 0.1 },
        { label: '111d MA', data: data.ma111, borderColor: '#00D64A', backgroundColor: 'transparent', borderWidth: 1.2, pointRadius: 0, tension: 0.1, borderDash: [4, 3] },
        { label: '2× 350d MA', data: data.ma350x2, borderColor: '#EC5B5B', backgroundColor: 'transparent', borderWidth: 1.2, pointRadius: 0, tension: 0.1, borderDash: [6, 4] },
      ]
    }, {
      scales: {
        x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 8, callback: function(v) { const l = this.getLabelForValue(v); return l ? l.slice(0,7) : ''; } }, grid: XGRID },
        y: { type: 'logarithmic', ticks: { ...YTICK, callback: v => '$' + (v >= 1000 ? (v/1000).toFixed(0)+'k' : v.toFixed(0)) }, grid: YGRID },
      },
      plugins: { legend: { display: true, labels: { color: '#888', font: { size: 11 }, boxWidth: 12 } } },
    });
    setTitle('BTC — Pi Cycle Top', 'Source: CoinGecko · 111d MA crossing 2×350d MA = cycle top');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── BTC: Market Cap with Milestones ───────────────────────────────────────────
