async function fetchBtcMAGap(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/btc-ma-gap?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    mkChart('line', {
      labels: data.dates,
      datasets: [{
        label: '50d/200d MA Gap (%)',
        data: data.gap,
        borderColor: '#2471CC', backgroundColor: 'transparent',
        borderWidth: 1.6, pointRadius: 0, tension: 0.1, fill: {target: 'origin', above: 'rgba(0,214,74,0.08)', below: 'rgba(236,91,91,0.08)'},
      }]
    }, {
      scales: {
        x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 8, callback: function(v) { const l = this.getLabelForValue(v); return l ? l.slice(0,7) : ''; } }, grid: XGRID },
        y: { ticks: { ...YTICK, callback: v => v + '%' }, grid: YGRID },
      },
      plugins: { legend: { display: false }, annotation: {} },
    });
    setTitle('50d / 200d MA Gap', 'Source: CoinGecko · positive = golden cross territory');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── BTC: 200-Week MA Floor ────────────────────────────────────────────────────
