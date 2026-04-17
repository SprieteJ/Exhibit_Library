async function fetchMacroRealYields(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/macro-real-yields?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    mkChart('line', {
      labels: data.dates,
      datasets: [
        { label: '10Y Yield', data: data.yield_10y,
          borderColor: '#4FC3F7', backgroundColor: 'transparent',
          pointRadius: 0, borderWidth: 1.5, tension: 0, yAxisID: 'y' },
        { label: 'BTC Price', data: data.btc,
          borderColor: '#F7931A', backgroundColor: 'transparent',
          pointRadius: 0, borderWidth: 1.5, tension: 0, yAxisID: 'y2' },
      ]
    }, {
      scales: {
        x:  { ticks: { ...XTICK, maxTicksLimit: 8 }, grid: XGRID },
        y:  { position: 'left',  ticks: { ...YTICK, color: '#4FC3F7', callback: v => v.toFixed(2)+'%' }, grid: YGRID },
        y2: { position: 'right', ticks: { ...YTICK, color: '#F7931A', callback: v => '$'+fmtBig(v) }, grid: { display: false } },
      },
      plugins: { legend: { display: true, labels: { color: '#aaa', font: { family: 'monospace', size: 11 } } } }
    });
    setTitle('US 10Y Yield vs BTC Price', 'Source: Yahoo Finance (^TNX) + CoinGecko');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── Macro: Stablecoin Supply ──────────────────────────────────────────────────
