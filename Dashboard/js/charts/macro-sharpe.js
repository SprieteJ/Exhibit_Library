async function fetchMacroSharpe(from, to) {
  spinOn();
  try {
    const winEl = document.getElementById('rp-window');
    const window = winEl?.value || '180';
    const data = await fetch(`/api/macro-sharpe?from=${from}&to=${to}&window=${window}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.assets || !Object.keys(data.assets).length) throw new Error('no data');

    const COLORS = {
      'BTC': '#F7931A', 'ETH': '#627EEA', 'ALTS': '#00D64A',
      'SPY': '#7Fb2F1', 'QQQ': '#2471CC', 'IWM': '#AEA9EA',
      'TLT': '#ED9B9B', 'GLD': '#E1C87E', 'BNO': '#9EA4A0',
    };

    // Build common date axis
    let allDates = new Set();
    for (const a of Object.values(data.assets)) a.dates.forEach(d => allDates.add(d));
    allDates = [...allDates].sort();

    const datasets = [];
    for (const [sym, a] of Object.entries(data.assets)) {
      const dateMap = {};
      a.dates.forEach((d, i) => dateMap[d] = a.sharpe[i]);
      datasets.push({
        label: a.label,
        data: allDates.map(d => dateMap[d] !== undefined ? dateMap[d] : null),
        borderColor: COLORS[sym] || PAL[datasets.length % PAL.length],
        backgroundColor: 'transparent',
        borderWidth: 1.4,
        pointRadius: 0,
        tension: 0.1,
        spanGaps: true,
      });
    }

    mkChart('line', { labels: allDates, datasets }, {
      scales: {
        x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 8,
          callback: function(val) { const l = this.getLabelForValue(val); return l ? l.slice(0,7) : ''; } }, grid: XGRID },
        y: { ticks: { ...YTICK, callback: v => v.toFixed(1) }, grid: YGRID },
      },
      plugins: {
        legend: { display: true, labels: { color: '#888', font: { size: 11 }, boxWidth: 12 } },
      },
    });

    // Summary bar below the chart
    const summaryDiv = document.getElementById('perf-row');
    if (summaryDiv) {
      const sorted = Object.entries(data.assets)
        .map(([sym, a]) => ({ sym, label: a.label, current: a.current, color: COLORS[sym] || '#888' }))
        .filter(a => a.current != null)
        .sort((a, b) => b.current - a.current);

      summaryDiv.innerHTML = sorted.map(a => {
        const color = a.current > 0 ? '#00D64A' : '#EC5B5B';
        return `<div class="perf-item"><span style="color:${a.color};font-weight:600">${a.label}</span> <span class="${a.current > 0 ? 'pos' : 'neg'}">${a.current > 0 ? '+' : ''}${a.current.toFixed(2)}</span></div>`;
      }).join('');
    }

    const winLabels = {'30': '1M', '90': '3M', '180': '6M', '365': '1Y', '730': '2Y', '1460': '4Y'};
    setTitle('Rolling Sharpe Ratio (' + (winLabels[window] || window + 'd') + ')', 'Source: CoinGecko + Yahoo Finance · annualised, risk-free = 0');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}



