async function fetchAltBeta(to) {
  spinOn();
  try {
    const data = await fetch(`/api/alt-beta?to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.points?.length) throw new Error('no data');
    const pts = data.points;
    mkChart('scatter', {
      datasets: [{ data: pts.map(p => ({ x: p.beta, y: p.alpha, label: p.symbol })),
        backgroundColor: pts.map(p => p.color_sector + 'cc'),
        pointRadius: 5, pointHoverRadius: 7 }]
    }, {
      scales: {
        x: { title: { display: true, text: 'Beta vs BTC', color: '#888' }, ticks: XTICK, grid: XGRID },
        y: { title: { display: true, text: 'Alpha (annualized %)', color: '#888' }, ticks: { ...YTICK, callback: v => v.toFixed(0)+'%' }, grid: YGRID },
      },
      plugins: { legend: { display: false },
        tooltip: { callbacks: { label: ctx => `${ctx.raw.label}  β=${ctx.raw.x.toFixed(2)}  α=${ctx.raw.y.toFixed(1)}%` } } }
    });
    setTitle('Alt Beta & Alpha vs BTC (60d)', 'Source: CoinGecko · OLS regression');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}


// ── Control Center: Signal Matrix ─────────────────────────────────────────────

// ── Category Overview (embedded CC rules per tab) ─────────────────────────────
let _ccDataCache = null;

