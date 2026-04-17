async function fetchBtcOI(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/btc-oi?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    mkChart('line', {
      labels: data.dates,
      datasets: [
        { label: 'Open Interest', data: data.oi_values,
          borderColor: '#4FC3F7', backgroundColor: 'rgba(79,195,247,0.1)',
          fill: true, pointRadius: 0, borderWidth: 2, tension: 0, yAxisID: 'y' },
        { label: 'BTC Price', data: data.btc_prices,
          borderColor: '#F7931A', backgroundColor: 'transparent',
          pointRadius: 0, borderWidth: 1.5, tension: 0, yAxisID: 'y2' },
      ]
    }, {
      scales: {
        x:  { ticks: { ...XTICK, maxTicksLimit: 8 }, grid: XGRID },
        y:  { position: 'left',  ticks: { ...YTICK, color: '#4FC3F7', callback: v => fmtBig(v) }, grid: YGRID },
        y2: { position: 'right', ticks: { ...YTICK, color: '#F7931A', callback: v => '$'+fmtBig(v) }, grid: { display: false } },
      },
      plugins: { legend: { display: true, labels: { color: '#aaa', font: { family: 'monospace', size: 11 } } } }
    });
    setTitle('BTC Open Interest', 'Source: Binance · USD notional');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── BTC: Funding Rate Delta ───────────────────────────────────────────────────
