async function fetchMacroStablecoin(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/macro-stablecoin?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    mkChart('line', {
      labels: data.dates,
      datasets: [{ label: 'Stablecoin Market Cap', data: data.values,
        borderColor: '#00D64A', backgroundColor: 'rgba(0,214,74,0.12)',
        fill: true, pointRadius: 0, borderWidth: 2, tension: 0 }]
    }, {
      scales: {
        x: { ticks: { ...XTICK, maxTicksLimit: 8 }, grid: XGRID },
        y: { ticks: { ...YTICK, callback: v => '$'+fmtBig(v) }, grid: YGRID },
      },
      plugins: { legend: { display: false } }
    });
    setTitle('Stablecoin Total Market Cap', 'Source: CoinGecko · USDT+USDC+DAI+…');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

