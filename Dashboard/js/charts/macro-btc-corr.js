async function fetchMacroBtcCorr(from, to) {
  spinOn();
  try {
    const winEl = document.getElementById('rp-window');
    const window = winEl?.value || '90';

    // Default assets — user can change via console or we add UI later
    const MACRO_ASSETS = ['SPY','QQQ','IWM','TLT','GLD','BNO','DX-Y.NYB','^VIX'];
    const syms = MACRO_ASSETS.join(',');

    const data = await fetch(`/api/macro-btc-corr?symbols=${encodeURIComponent(syms)}&from=${from}&to=${to}&window=${window}`).then(r=>r.json());
    if (!data.assets || !Object.keys(data.assets).length) throw new Error('no data');

    const COLORS = {
      'SPY': '#7Fb2F1', 'QQQ': '#2471CC', 'IWM': '#AEA9EA',
      'TLT': '#ED9B9B', 'GLD': '#E1C87E', 'BNO': '#9EA4A0',
      'DX-Y.NYB': '#C084FC', '^VIX': '#F87171',
      'IBIT': '#F7931A', 'MSTR': '#F97316', 'COIN': '#3B82F6',
    };

    // Default visibility: only QQQ and GLD shown initially
    const DEFAULT_VISIBLE = ['SPY', 'GLD'];

    // Build common date axis
    let allDates = new Set();
    for (const a of Object.values(data.assets)) a.dates.forEach(d => allDates.add(d));
    allDates = [...allDates].sort();

    const datasets = [];
    for (const [sym, a] of Object.entries(data.assets)) {
      const dateMap = {};
      a.dates.forEach((d, i) => dateMap[d] = a.corr[i]);
      datasets.push({
        label: a.label,
        data: allDates.map(d => dateMap[d] !== undefined ? dateMap[d] : null),
        borderColor: COLORS[sym] || PAL[datasets.length % PAL.length],
        backgroundColor: 'transparent',
        borderWidth: 1.6,
        pointRadius: 0,
        tension: 0.1,
        spanGaps: true,
        hidden: !DEFAULT_VISIBLE.includes(sym),
      });
    }

    // Add zero line
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
        y: { min: -1, max: 1, ticks: { ...YTICK, callback: v => v.toFixed(1), stepSize: 0.2 }, grid: YGRID },
      },
      plugins: {
        legend: { display: true, labels: { color: '#888', font: { size: 11 }, boxWidth: 12 },
          onClick: function(e, legendItem, legend) {
            const idx = legendItem.datasetIndex;
            if (legend.chart.data.datasets[idx].label === 'Zero') return;
            Chart.defaults.plugins.legend.onClick.call(this, e, legendItem, legend);
          }
        },
      },
    });

    // Summary bar
    const summaryDiv = document.getElementById('perf-row');
    if (summaryDiv) {
      const sorted = Object.entries(data.assets)
        .map(([sym, a]) => ({ sym, label: a.label, current: a.current, color: COLORS[sym] || '#888' }))
        .filter(a => a.current != null)
        .sort((a, b) => b.current - a.current);

      summaryDiv.innerHTML = sorted.map(a => {
        const clr = a.current > 0.3 ? '#00D64A' : (a.current < -0.3 ? '#EC5B5B' : '#888');
        return `<div class="perf-item"><span style="color:${a.color};font-weight:600">${a.label}</span> <span style="color:${clr}">${a.current > 0 ? '+' : ''}${a.current.toFixed(2)}</span></div>`;
      }).join('');
    }

    const winLabels = {'14': '14d', '30': '30d', '60': '60d', '90': '90d', '180': '180d', '365': '1Y'};
    setTitle('Rolling ' + (winLabels[window] || window + 'd') + ' Correlation vs BTC', 'Source: CoinGecko + Yahoo Finance · log daily returns · click legend to toggle assets');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}


