async function fetchAltIntracorr(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/alt-intracorr?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    const ds = [];
    const tiers = [['top10','Top 10','#F7931A'],['top25','Top 25','#2471CC'],['top50','Top 50','#746BE6'],['top100','Top 100','#00D64A'],['top250','Top 250','#888']];
    for (const [k,l,c] of tiers) {
      if (data[k]?.length) ds.push({ label: l, data: data[k], borderColor: c, backgroundColor: 'transparent', borderWidth: 1.2, pointRadius: 0, tension: 0.1 });
    }
    mkChart('line', { labels: data.dates, datasets: ds },
    { scales: { x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 8, callback: function(v) { return this.getLabelForValue(v)?.slice(0,7)||''; } }, grid: XGRID },
      y: { min: -0.2, max: 1, ticks: { ...YTICK, callback: v => v.toFixed(1) }, grid: YGRID } },
      plugins: { legend: { display: true, labels: { color: '#888', font: { size: 11 }, boxWidth: 12 } } } });
    setTitle('Altcoin Intracorrelation', 'Source: CoinGecko · avg pairwise 30d Pearson · high = everything moves together');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}


// ── BTC: MA Gap (50d vs 200d) ─────────────────────────────────────────────────
