async function fetchBtcMcap(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/btc-mcap?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    const fmtT = v => { if (v >= 1e12) return '$' + (v/1e12).toFixed(1) + 'T'; if (v >= 1e9) return '$' + (v/1e9).toFixed(0) + 'B'; return '$' + (v/1e6).toFixed(0) + 'M'; };
    mkChart('line', {
      labels: data.dates,
      datasets: [{
        label: 'BTC Market Cap', data: data.mcap,
        borderColor: '#F7931A', backgroundColor: 'rgba(247,147,26,0.06)',
        borderWidth: 1.6, pointRadius: 0, tension: 0.1, fill: true,
      }]
    }, {
      scales: {
        x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 8, callback: function(v) { const l = this.getLabelForValue(v); return l ? l.slice(0,7) : ''; } }, grid: XGRID },
        y: { ticks: { ...YTICK, callback: v => fmtT(v) }, grid: YGRID },
      },
      plugins: { legend: { display: false } },
    });
    setTitle('BTC Market Cap', 'Source: CoinGecko · $100B / $500B / $1T / $2T milestones');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── BTC: Realized Vol vs Implied Vol (DVOL) ───────────────────────────────────
