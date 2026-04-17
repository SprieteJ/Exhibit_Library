async function fetchEthDrawdown(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/eth-drawdown?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    mkChart('line', { labels: data.dates, datasets: [{ label: 'Drawdown %', data: data.values, borderColor: '#EC5B5B', backgroundColor: 'rgba(236,91,91,0.06)', borderWidth: 1.4, pointRadius: 0, fill: true }] },
    { scales: { x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 8, callback: function(v) { return this.getLabelForValue(v)?.slice(0,7)||''; } }, grid: XGRID },
      y: { max: 0, ticks: { ...YTICK, callback: v => v+'%' }, grid: YGRID } }, plugins: { legend: { display: false } } });
    setTitle('ETH Drawdown from ATH', 'Source: CoinGecko');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

