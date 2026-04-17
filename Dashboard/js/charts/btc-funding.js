async function fetchBtcFunding(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/btc-funding?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    mkChart('bar', {
      labels: data.dates,
      datasets: [
        { type: 'bar',  label: 'Funding Rate', data: data.values,
          backgroundColor: data.values.map(v => (v||0) >= 0 ? 'rgba(247,147,26,0.55)' : 'rgba(0,214,74,0.55)'),
          borderWidth: 0, order: 2 },
        { type: 'line', label: '7d MA', data: data.ma7,
          borderColor: '#fff', backgroundColor: 'transparent',
          pointRadius: 0, borderWidth: 1.5, tension: 0, order: 1 },
      ]
    }, {
      scales: {
        x: { ticks: { ...XTICK, maxTicksLimit: 8 }, grid: XGRID },
        y: { ticks: { ...YTICK, callback: v => (v*100).toFixed(3)+'%' }, grid: YGRID },
      },
      plugins: { legend: { display: false } }
    });
    setTitle('BTC Perpetual Funding Rate', 'Source: Binance · 8h avg per day');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── BTC: Open Interest ────────────────────────────────────────────────────────
