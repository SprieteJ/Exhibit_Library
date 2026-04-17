async function fetchSectorSharpe(to) {
  spinOn();
  try {
    const data = await fetch(`/api/sector-sharpe?to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (data.error) throw new Error(data.error);
    const pts = Object.entries(data).map(([s, v]) => ({ label: s, x: v.x, y: v.y, color: v.color }));
    mkChart('scatter', {
      datasets: [{ data: pts.map(p => ({ x: p.x, y: p.y, label: p.label })),
        backgroundColor: pts.map(p => p.color + 'cc'), pointRadius: 8, pointHoverRadius: 10 }]
    }, {
      scales: {
        x: { title: { display: true, text: '30d Vol (%)', color: '#888' }, ticks: XTICK, grid: XGRID },
        y: { title: { display: true, text: '30d Return (%)', color: '#888' }, ticks: YTICK, grid: YGRID },
      },
      plugins: { legend: { display: false } }
    }, [{
      id: 'sectorLabels',
      afterDraw(ch) {
        const c = ch.ctx; c.save(); c.font = '11px monospace'; c.textAlign = 'left';
        ch.data.datasets[0].data.forEach((pt, i) => {
          const el = ch.getDatasetMeta(0).data[i]; if (!el) return;
          c.fillStyle = pts[i].color;
          c.fillText(pt.label, el.x + 6, el.y - 4);
        });
        c.restore();
      }
    }]);
    setTitle('Sector Risk vs Return (30d)', 'Source: CoinGecko · equal-weighted');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── BTC: Bull Cycles ──────────────────────────────────────────────────────────
