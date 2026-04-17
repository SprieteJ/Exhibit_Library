async function fetchBtcDrawdownATH(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/btc-drawdown?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    mkChart('line', {
      labels: data.dates,
      datasets: [{ label: 'Drawdown from ATH', data: data.values,
        borderColor: '#EC5B5B', backgroundColor: 'rgba(236,91,91,0.15)',
        fill: true, pointRadius: 0, borderWidth: 1.5, tension: 0 }]
    }, {
      scales: {
        x: { ticks: { ...XTICK, maxTicksLimit: 8 }, grid: XGRID },
        y: { max: 0, ticks: { ...YTICK, callback: v => v+'%' }, grid: YGRID },
      },
      plugins: { legend: { display: false } }
    });
    setTitle('BTC Drawdown from All-Time High', 'Source: CoinGecko · running ATH');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── BTC: Gold Ratio ───────────────────────────────────────────────────────────
