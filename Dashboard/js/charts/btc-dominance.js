async function fetchBtcDominance(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/btc-dominance?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    mkChart('line', {
      labels: data.dates,
      datasets: [{ label: 'BTC Dominance', data: data.values,
        borderColor: '#F7931A', backgroundColor: 'rgba(247,147,26,0.1)',
        fill: true, pointRadius: 0, borderWidth: 2, tension: 0 }]
    }, {
      scales: {
        x: { ticks: { ...XTICK, maxTicksLimit: 8 }, grid: XGRID },
        y: { ticks: { ...YTICK, callback: v => v.toFixed(1)+'%' }, grid: YGRID },
      },
      plugins: { legend: { display: false } }
    });
    setTitle('BTC Market Cap Dominance', 'Source: CoinGecko · mcap ratio');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── BTC: Funding Rate ─────────────────────────────────────────────────────────
