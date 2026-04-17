async function fetchBtcRealvol(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/btc-realvol?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    mkChart('line', {
      labels: data.dates,
      datasets: [
        { label: '30d Vol',  data: data.vol_30d,  borderColor: '#F7931A', backgroundColor: 'transparent', pointRadius: 0, borderWidth: 2, tension: 0 },
        { label: '90d Vol',  data: data.vol_90d,  borderColor: '#4FC3F7', backgroundColor: 'transparent', pointRadius: 0, borderWidth: 2, tension: 0 },
        { label: '180d Vol', data: data.vol_180d, borderColor: '#00D64A', backgroundColor: 'transparent', pointRadius: 0, borderWidth: 2, tension: 0 },
      ]
    }, {
      scales: {
        x: { ticks: { ...XTICK, maxTicksLimit: 8 }, grid: XGRID },
        y: { title: { display: true, text: 'Annualized Vol (%)', color: '#888' }, ticks: { ...YTICK, callback: v => v+'%' }, grid: YGRID },
      },
      plugins: { legend: { display: true, labels: { color: '#aaa', font: { family: 'monospace', size: 11 } } } }
    });
    setTitle('BTC Realized Volatility (Annualized)', 'Source: CoinGecko · log-return std dev');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── BTC: Drawdown from ATH ────────────────────────────────────────────────────


