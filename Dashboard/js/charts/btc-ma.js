async function fetchBtcMA(from, to) {
  spinOn();
  try {
    const customEl = document.getElementById('rp-custom-ma');
    const custom = customEl?.value || '';
    let url = `/api/btc-ma?from=${from}&to=${to}`;
    if (custom && parseInt(custom) >= 2) url += `&custom=${custom}`;
    const data = await fetch(url, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');
    const datasets = [
      { label: 'BTC Price', data: data.price, borderColor: '#F7931A', backgroundColor: 'transparent',
        borderWidth: 1.8, pointRadius: 0, tension: 0.1, yAxisID: 'y' },
      { label: '50d MA', data: data.ma50, borderColor: '#2471CC', backgroundColor: 'transparent',
        borderWidth: 1.2, pointRadius: 0, tension: 0.1, borderDash: [4, 3], yAxisID: 'y' },
      { label: '200d MA', data: data.ma200, borderColor: '#EC5B5B', backgroundColor: 'transparent',
        borderWidth: 1.2, pointRadius: 0, tension: 0.1, borderDash: [6, 4], yAxisID: 'y' },
    ];
    if (data.custom_ma?.length && data.custom_window) {
      datasets.push({
        label: `${data.custom_window}d MA`, data: data.custom_ma, borderColor: '#746BE6', backgroundColor: 'transparent',
        borderWidth: 1.2, pointRadius: 0, tension: 0.1, borderDash: [2, 2], yAxisID: 'y',
      });
    }
    mkChart('line', { labels: data.dates, datasets }, {
      scales: {
        x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 8,
          callback: function(val) { const l = this.getLabelForValue(val); return l ? l.slice(0,7) : ''; } }, grid: XGRID },
        y: { ticks: { ...YTICK, callback: v => '$' + (v >= 1000 ? (v/1000).toFixed(0)+'k' : v.toFixed(0)) }, grid: YGRID },
      },
      plugins: { legend: { display: true, labels: { color: '#888', font: { size: 11 }, boxWidth: 12 } } },
    });
    setTitle('BTC Price — Moving Averages', 'Source: CoinGecko · 50d, 200d' + (data.custom_window ? `, ${data.custom_window}d` : ''));
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── Crypto Market: Total Market Cap ───────────────────────────────────────────
