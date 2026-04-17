async function fetchEthMcap(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/eth-mcap?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    const fmtT = v => { if(v>=1e12) return '$'+(v/1e12).toFixed(1)+'T'; if(v>=1e9) return '$'+(v/1e9).toFixed(0)+'B'; return '$'+(v/1e6).toFixed(0)+'M'; };
    mkChart('line', { labels: data.dates, datasets: [{ label: 'ETH Mcap', data: data.mcap, borderColor: '#746BE6', backgroundColor: 'rgba(116,107,230,0.06)', borderWidth: 1.6, pointRadius: 0, fill: true }] },
    { scales: { x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 8, callback: function(v) { return this.getLabelForValue(v)?.slice(0,7)||''; } }, grid: XGRID },
      y: { ticks: { ...YTICK, callback: v => fmtT(v) }, grid: YGRID } }, plugins: { legend: { display: false } } });
    setTitle('ETH Market Cap', 'Source: CoinGecko');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

