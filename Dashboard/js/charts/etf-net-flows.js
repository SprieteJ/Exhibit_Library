async function fetchEtfNetFlows(from, to) {
  spinOn();
  try {
    const winEl = document.getElementById('rp-window');
    const window = winEl?.value || '7';
    const data = await fetch(`/api/etf-flows?from=${from}&to=${to}&window=${window}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.assets || !Object.keys(data.assets).length) throw new Error('no data');

    const COLORS = { 'BTC': '#F7931A', 'ETH': '#627EEA' };

    // Build common date axis
    let allDates = new Set();
    for (const a of Object.values(data.assets)) a.dates.forEach(d => allDates.add(d));
    allDates = [...allDates].sort();

    const datasets = [];
    for (const [asset, a] of Object.entries(data.assets)) {
      const dateMap = {};
      a.dates.forEach((d, i) => dateMap[d] = a.rolling[i]);

      datasets.push({
        label: asset + ' ETF (' + data.window + 'd trailing)',
        data: allDates.map(d => dateMap[d] !== undefined ? dateMap[d] : null),
        borderColor: COLORS[asset] || PAL[datasets.length % PAL.length],
        backgroundColor: COLORS[asset] ? COLORS[asset] + '15' : 'transparent',
        borderWidth: 1.8,
        pointRadius: 0,
        tension: 0.1,
        spanGaps: true,
        fill: true,
      });
    }

    // Zero line
    datasets.push({
      label: 'Zero',
      data: allDates.map(() => 0),
      borderColor: darkMode ? 'rgba(255,255,255,0.15)' : 'rgba(0,0,0,0.15)',
      borderWidth: 1,
      pointRadius: 0,
      borderDash: [4, 4],
    });

    mkChart('line', { labels: allDates, datasets }, {
      scales: {
        x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 10,
          callback: function(val) { const l = this.getLabelForValue(val); return l ? l.slice(0,7) : ''; } }, grid: XGRID },
        y: { ticks: { ...YTICK, callback: v => (v >= 0 ? '+' : '') + v.toFixed(0) + 'M' }, grid: YGRID },
      },
      plugins: {
        legend: { display: true, labels: { color: '#888', font: { size: 11 }, boxWidth: 12 },
          onClick: function(e, legendItem, legend) {
            if (legend.chart.data.datasets[legendItem.datasetIndex].label === 'Zero') return;
            Chart.defaults.plugins.legend.onClick.call(this, e, legendItem, legend);
          }
        },
      },
    });

    // Summary bar
    const summaryDiv = document.getElementById('perf-row');
    if (summaryDiv) {
      const items = [];
      for (const [asset, a] of Object.entries(data.assets)) {
        const clr = COLORS[asset] || '#888';
        if (a.current_daily != null) {
          const dClr = a.current_daily > 0 ? '#00D64A' : '#EC5B5B';
          items.push(`<div class="perf-item"><span style="color:${clr};font-weight:600">${asset} daily</span> <span style="color:${dClr}">${a.current_daily > 0 ? '+' : ''}${a.current_daily.toFixed(1)}M</span></div>`);
        }
        if (a.current_rolling != null) {
          const rClr = a.current_rolling > 0 ? '#00D64A' : '#EC5B5B';
          items.push(`<div class="perf-item"><span style="color:${clr};font-weight:600">${asset} ${data.window}d</span> <span style="color:${rClr}">${a.current_rolling > 0 ? '+' : ''}${a.current_rolling.toFixed(1)}M</span></div>`);
        }
      }
      summaryDiv.innerHTML = items.join('');
    }

    const winLabels = {'1': 'daily', '7': '7d trailing', '14': '14d trailing', '30': '30d trailing'};
    setTitle('Spot ETF Net Flows (' + (winLabels[window] || window + 'd trailing') + ')', 'Source: Farside Investors · USD millions');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}



// ── Control Center: Chart of the Month ────────────────────────────────────────
