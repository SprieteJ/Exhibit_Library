async function fetchEtfDailyBar(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/etf-flows?from=${from}&to=${to}&window=1`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.assets || !Object.keys(data.assets).length) throw new Error('no data');

    const COLORS = { 'BTC': { pos: '#F7931A', neg: '#F7931A80' }, 'ETH': { pos: '#627EEA', neg: '#627EEA80' } };

    // Build common date axis
    let allDates = new Set();
    for (const a of Object.values(data.assets)) a.dates.forEach(d => allDates.add(d));
    allDates = [...allDates].sort();

    const datasets = [];
    for (const [asset, a] of Object.entries(data.assets)) {
      const dateMap = {};
      a.dates.forEach((d, i) => dateMap[d] = a.daily[i]);
      const values = allDates.map(d => dateMap[d] !== undefined ? dateMap[d] : null);
      const bgColors = values.map(v => v != null && v >= 0 ? COLORS[asset]?.pos || '#888' : COLORS[asset]?.neg || '#88880');

      datasets.push({
        label: asset + ' ETF',
        data: values,
        backgroundColor: bgColors,
        borderColor: 'transparent',
        borderWidth: 0,
        stack: 'stack1',
      });
    }

    mkChart('bar', { labels: allDates, datasets }, {
      scales: {
        x: { stacked: true, type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 10,
          callback: function(val) { const l = this.getLabelForValue(val); return l ? l.slice(0,7) : ''; } }, grid: XGRID },
        y: { stacked: true, ticks: { ...YTICK, callback: v => (v >= 0 ? '+' : '') + v.toFixed(0) + 'M' }, grid: YGRID },
      },
      plugins: {
        legend: { display: true, labels: { color: '#888', font: { size: 11 }, boxWidth: 12 } },
      },
    });

    // Summary
    const summaryDiv = document.getElementById('perf-row');
    if (summaryDiv) {
      const items = [];
      for (const [asset, a] of Object.entries(data.assets)) {
        if (a.current_daily != null) {
          const clr = a.current_daily >= 0 ? '#00D64A' : '#EC5B5B';
          items.push(`<div class="perf-item"><span style="color:${asset === 'BTC' ? '#F7931A' : '#627EEA'};font-weight:600">${asset}</span> <span style="color:${clr}">${a.current_daily >= 0 ? '+' : ''}${a.current_daily.toFixed(1)}M</span></div>`);
        }
      }
      summaryDiv.innerHTML = items.join('');
    }

    setTitle('Daily — Spot ETF Flows', 'Source: Farside Investors · USD millions · stacked');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── ETF: Weekly Bar Chart ────────────────────────────────────────────────────
