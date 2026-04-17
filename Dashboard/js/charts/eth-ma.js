async function fetchEthMA(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/eth-ma?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    mkChart('line', { labels: data.dates, datasets: [
      { label: 'ETH Price', data: data.price, borderColor: '#746BE6', backgroundColor: 'transparent', borderWidth: 1.6, pointRadius: 0, tension: 0.1 },
      { label: '50d MA', data: data.ma50, borderColor: '#2471CC', backgroundColor: 'transparent', borderWidth: 1.2, pointRadius: 0, borderDash: [4,3] },
      { label: '200d MA', data: data.ma200, borderColor: '#EC5B5B', backgroundColor: 'transparent', borderWidth: 1.2, pointRadius: 0, borderDash: [6,4] },
      { label: '200w MA', data: data.ma200w, borderColor: '#00D64A', backgroundColor: 'transparent', borderWidth: 1.0, pointRadius: 0, borderDash: [2,6] },
    ]}, { scales: { x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 8, callback: function(v) { return this.getLabelForValue(v)?.slice(0,7)||''; } }, grid: XGRID },
      y: { type: 'logarithmic', ticks: { ...YTICK, callback: v => '$'+(v>=1000?(v/1000).toFixed(0)+'k':v.toFixed(0)) }, grid: YGRID } },
      plugins: { legend: { display: true, labels: { color: '#888', font: { size: 11 }, boxWidth: 12 } } } });
    setTitle('ETH — 50-day and 200-day Moving Average', 'Source: CoinGecko · log scale');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

