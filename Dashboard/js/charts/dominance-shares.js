async function fetchDominanceShares(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/dominance-shares?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    mkChart('line', { labels: data.dates, datasets: [
      { label: 'BTC', data: data.btc_pct, borderColor: '#F7931A', backgroundColor: 'transparent', borderWidth: 1.4, pointRadius: 0 },
      { label: 'ETH', data: data.eth_pct, borderColor: '#746BE6', backgroundColor: 'transparent', borderWidth: 1.4, pointRadius: 0 },
      { label: 'Altcoins', data: data.alt_pct, borderColor: '#00D64A', backgroundColor: 'transparent', borderWidth: 1.4, pointRadius: 0 },
    ]}, { scales: { x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 8, callback: function(v) { return this.getLabelForValue(v)?.slice(0,7)||''; } }, grid: XGRID },
      y: { ticks: { ...YTICK, callback: v => v+'%' }, grid: YGRID } },
      plugins: { legend: { display: true, labels: { color: '#888', font: { size: 11 }, boxWidth: 12 } } } });
    setTitle('Dominance — BTC / ETH / Altcoins', 'Source: CoinGecko · share of total crypto mcap');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

