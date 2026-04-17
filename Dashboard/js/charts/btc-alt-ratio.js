async function fetchBtcAltRatio(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/btc-alt-ratio?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    mkChart('line', { labels: data.dates, datasets: [{ label: 'BTC/Alt Ratio', data: data.ratio, borderColor: '#F7931A', backgroundColor: 'rgba(247,147,26,0.06)', borderWidth: 1.6, pointRadius: 0, fill: true }] },
    { scales: { x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 8, callback: function(v) { return this.getLabelForValue(v)?.slice(0,7)||''; } }, grid: XGRID },
      y: { ticks: YTICK, grid: YGRID } }, plugins: { legend: { display: false } } });
    setTitle('BTC / Altcoin Mcap Ratio', 'Source: CoinGecko · rising = BTC dominance');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

