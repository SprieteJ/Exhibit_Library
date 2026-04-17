async function fetchEthBtcRatio(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/eth-btc-ratio?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    mkChart('line', { labels: data.dates, datasets: [{ label: 'ETH/BTC', data: data.values, borderColor: '#746BE6', backgroundColor: 'rgba(116,107,230,0.06)', borderWidth: 1.6, pointRadius: 0, fill: true }] },
    { scales: { x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 8, callback: function(v) { return this.getLabelForValue(v)?.slice(0,7)||''; } }, grid: XGRID },
      y: { ticks: { ...YTICK, callback: v => v.toFixed(4) }, grid: YGRID } }, plugins: { legend: { display: false } } });
    setTitle('ETH/BTC Ratio', 'Source: CoinGecko · falling = BTC dominance, rising = alt appetite');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── Alt Market charts ─────────────────────────────────────────────────────────
