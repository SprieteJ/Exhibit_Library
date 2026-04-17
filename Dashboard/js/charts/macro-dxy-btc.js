async function fetchMacroDxyBtc(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/macro-dxy-btc?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    mkChart('line', {
      labels: data.dates,
      datasets: [
        { label: 'DXY', data: data.dxy,
          borderColor: '#4FC3F7', backgroundColor: 'transparent',
          pointRadius: 0, borderWidth: 1.5, tension: 0, yAxisID: 'y' },
        { label: 'BTC', data: data.btc,
          borderColor: '#F7931A', backgroundColor: 'transparent',
          pointRadius: 0, borderWidth: 1.5, tension: 0, yAxisID: 'y2' },
        { label: '30d Corr', data: data.correlation,
          borderColor: '#EC5B5B', backgroundColor: 'transparent',
          pointRadius: 0, borderWidth: 2, tension: 0.1, yAxisID: 'y3',
          borderDash: [4, 3] },
      ]
    }, {
      scales: {
        x:  { ticks: { ...XTICK, maxTicksLimit: 8 }, grid: XGRID },
        y:  { position: 'left',  ticks: { ...YTICK, color: '#4FC3F7' }, grid: YGRID },
        y2: { position: 'right', ticks: { ...YTICK, color: '#F7931A', callback: v => '$'+fmtBig(v) }, grid: { display: false } },
        y3: { display: false, min: -1, max: 1 },
      },
      plugins: { legend: { display: true, labels: { color: '#aaa', font: { family: 'monospace', size: 11 } } } }
    });
    setTitle('DXY vs BTC + 30d Rolling Correlation', 'Source: Yahoo Finance + CoinGecko');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── Macro: Risk-On/Off Score ──────────────────────────────────────────────────
