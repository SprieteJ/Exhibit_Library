async function fetchEtfWeeklyBar(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/etf-flows-weekly?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.assets || !Object.keys(data.assets).length) throw new Error('no data');

    const COLORS = { 'BTC': { pos: '#F7931A', neg: '#F7931A80' }, 'ETH': { pos: '#627EEA', neg: '#627EEA80' } };

    let allDates = new Set();
    for (const a of Object.values(data.assets)) a.dates.forEach(d => allDates.add(d));
    allDates = [...allDates].sort();

    const datasets = [];
    for (const [asset, a] of Object.entries(data.assets)) {
      const dateMap = {};
      a.dates.forEach((d, i) => dateMap[d] = a.flows[i]);
      const values = allDates.map(d => dateMap[d] !== undefined ? dateMap[d] : null);
      const bgColors = values.map(v => v != null && v >= 0 ? COLORS[asset]?.pos || '#888' : COLORS[asset]?.neg || '#888');

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
        x: { stacked: true, type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 12,
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
        const last = a.flows[a.flows.length - 1];
        if (last != null) {
          const clr = last >= 0 ? '#00D64A' : '#EC5B5B';
          items.push(`<div class="perf-item"><span style="color:${asset === 'BTC' ? '#F7931A' : '#627EEA'};font-weight:600">${asset} this week</span> <span style="color:${clr}">${last >= 0 ? '+' : ''}${last.toFixed(1)}M</span></div>`);
        }
      }
      summaryDiv.innerHTML = items.join('');
    }

    setTitle('Weekly — Spot ETF Flows', 'Source: Farside Investors · USD millions · stacked');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}


// ── ETF: Spot Net Flows ──────────────────────────────────────────────────────
