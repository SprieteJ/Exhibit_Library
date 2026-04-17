async function fetchMacroRisk(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/macro-risk?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    mkChart('line', {
      labels: data.dates,
      datasets: [
        { label: 'Risk Score (0-100)', data: data.score,
          borderColor: '#00D64A', backgroundColor: 'rgba(0,214,74,0.12)',
          fill: true, pointRadius: 0, borderWidth: 2, tension: 0.1, yAxisID: 'y' },
        { label: 'BTC Price', data: data.btc,
          borderColor: '#F7931A', backgroundColor: 'transparent',
          pointRadius: 0, borderWidth: 1.5, tension: 0, yAxisID: 'y2' },
      ]
    }, {
      scales: {
        x:  { ticks: { ...XTICK, maxTicksLimit: 8 }, grid: XGRID },
        y:  { min: 0, max: 100, position: 'left',  ticks: { ...YTICK, color: '#00D64A' }, grid: YGRID },
        y2: { position: 'right', ticks: { ...YTICK, color: '#F7931A', callback: v => '$'+fmtBig(v) }, grid: { display: false } },
      },
      plugins: { legend: { display: true, labels: { color: '#aaa', font: { family: 'monospace', size: 11 } } } }
    });
    setTitle('Macro Risk-On/Off Score', 'Source: Yahoo Finance · VIX + DXY + HYG/LQD');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── Macro: Real Yields ────────────────────────────────────────────────────────
