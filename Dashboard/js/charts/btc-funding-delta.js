async function fetchBtcFundingDelta(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/btc-funding-delta?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data — check derivatives feed');
    mkChart('line', {
      labels: data.dates,
      datasets: [
        { label: `${data.window||30}d Funding Δ (bps)`, data: data.funding_delta,
          borderColor: '#4FC3F7', backgroundColor: 'transparent',
          pointRadius: 0, borderWidth: 2, tension: 0, yAxisID: 'y' },
        { label: `${data.window||30}d Price Δ (%)`, data: data.price_delta,
          borderColor: '#F7931A', backgroundColor: 'transparent',
          pointRadius: 0, borderWidth: 2, tension: 0, yAxisID: 'y2' },
      ]
    }, {
      scales: {
        x:  { ticks: { ...XTICK, maxTicksLimit: 8 }, grid: XGRID },
        y:  { position: 'left',
              title: { display: true, text: 'Funding Δ (bps)', color: '#4FC3F7' },
              ticks: { ...YTICK, color: '#4FC3F7', callback: v => v.toFixed(1)+'bp' },
              grid: YGRID },
        y2: { position: 'right',
              title: { display: true, text: 'Price Δ (%)', color: '#F7931A' },
              ticks: { ...YTICK, color: '#F7931A', callback: v => v.toFixed(1)+'%' },
              grid: { display: false } },
      },
      plugins: { legend: { display: true, labels: { color: '#aaa', font: { family: 'monospace', size: 11 } } } }
    });
    setTitle(`BTC Funding Rate Delta vs Price Return (${data.window||30}d rolling)`,
             'Source: Binance · 8h funding avg · CoinGecko price');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── Alt: Altseason Index ──────────────────────────────────────────────────────

// ── Altcoins: Deep Dive — Rebased Performance ────────────────────────────────
