async function fetchAltMcapDev(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/alt-mcap-dev?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    mkChart('line', { labels: data.dates, datasets: [{ label: '% Deviation', data: data.deviation, borderColor: '#00D64A', backgroundColor: 'transparent', borderWidth: 1.4, pointRadius: 0, fill: {target:'origin',above:'rgba(0,214,74,0.06)',below:'rgba(236,91,91,0.06)'} }] },
    { scales: { x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 8, callback: function(v) { return this.getLabelForValue(v)?.slice(0,7)||''; } }, grid: XGRID },
      y: { ticks: { ...YTICK, callback: v => v+'%' }, grid: YGRID } }, plugins: { legend: { display: false } } });
    setTitle('Altcoin Mcap — Deviation from 200d MA', 'Source: CoinGecko');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

