async function fetchAltMcap(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/alt-mcap-total?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    const fmtT = v => { if(v>=1e12) return '$'+(v/1e12).toFixed(2)+'T'; if(v>=1e9) return '$'+(v/1e9).toFixed(0)+'B'; return '$'+(v/1e6).toFixed(0)+'M'; };
    mkChart('line', { labels: data.dates, datasets: [
      { label: 'Altcoin Mcap', data: data.mcap, borderColor: '#00D64A', backgroundColor: 'rgba(0,214,74,0.06)', borderWidth: 1.6, pointRadius: 0, fill: true },
      { label: '50d MA', data: data.ma50, borderColor: '#2471CC', backgroundColor: 'transparent', borderWidth: 1.2, pointRadius: 0, borderDash: [4,3] },
      { label: '200d MA', data: data.ma200, borderColor: '#EC5B5B', backgroundColor: 'transparent', borderWidth: 1.2, pointRadius: 0, borderDash: [6,4] },
    ]}, { scales: { x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 8, callback: function(v) { return this.getLabelForValue(v)?.slice(0,7)||''; } }, grid: XGRID },
      y: { ticks: { ...YTICK, callback: v => fmtT(v) }, grid: YGRID } },
      plugins: { legend: { display: true, labels: { color: '#888', font: { size: 11 }, boxWidth: 12 } } } });
    setTitle('Altcoin Market Cap (ex-BTC, ex-ETH)', 'Source: CoinGecko');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

