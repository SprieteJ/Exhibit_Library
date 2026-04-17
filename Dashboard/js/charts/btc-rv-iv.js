async function fetchBtcRvIv(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/btc-rv-iv?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    mkChart('line', {
      labels: data.dates,
      datasets: [
        { label: '30d Realized Vol', data: data.rv30, borderColor: '#2471CC', backgroundColor: 'transparent', borderWidth: 1.4, pointRadius: 0, tension: 0.1 },
        { label: 'DVOL (Implied Vol)', data: data.dvol, borderColor: '#F7931A', backgroundColor: 'transparent', borderWidth: 1.4, pointRadius: 0, tension: 0.1 },
        { label: 'Spread (IV-RV)', data: data.spread, borderColor: '#746BE6', backgroundColor: 'transparent', borderWidth: 1.0, pointRadius: 0, tension: 0.1, borderDash: [4, 3], yAxisID: 'y2' },
      ]
    }, {
      scales: {
        x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 8, callback: function(v) { const l = this.getLabelForValue(v); return l ? l.slice(0,7) : ''; } }, grid: XGRID },
        y: { ticks: { ...YTICK, callback: v => v + '%' }, grid: YGRID },
        y2: { position: 'right', ticks: { ...YTICK, callback: v => v + 'pp' }, grid: { display: false } },
      },
      plugins: { legend: { display: true, labels: { color: '#888', font: { size: 11 }, boxWidth: 12 } } },
    });
    setTitle('Realized Vol vs Implied Vol (DVOL)', 'Source: CoinGecko + Deribit · positive spread = market pricing risk');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}



// ── Themes: Sector Overview Matrix ────────────────────────────────────────────

// ── Baskets: Deep Dive — Rebased Performance ─────────────────────────────────
