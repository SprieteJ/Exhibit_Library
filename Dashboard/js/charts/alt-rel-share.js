async function fetchAltRelShare(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/alt-rel-share?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    mkChart('line', { labels: data.dates, datasets: [
      { label: 'vs Total Mcap', data: data.vs_total, borderColor: '#00D64A', backgroundColor: 'transparent', borderWidth: 1.4, pointRadius: 0 },
      { label: 'vs BTC Mcap', data: data.vs_btc, borderColor: '#F7931A', backgroundColor: 'transparent', borderWidth: 1.4, pointRadius: 0 },
      { label: 'vs ETH Mcap', data: data.vs_eth, borderColor: '#746BE6', backgroundColor: 'transparent', borderWidth: 1.4, pointRadius: 0 },
    ]}, { scales: { x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 8, callback: function(v) { return this.getLabelForValue(v)?.slice(0,7)||''; } }, grid: XGRID },
      y: { ticks: { ...YTICK, callback: v => v+'%' }, grid: YGRID } },
      plugins: { legend: { display: true, labels: { color: '#888', font: { size: 11 }, boxWidth: 12 } } } });
    setTitle('Altcoin Relative Share', 'Source: CoinGecko');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

