async function fetchEtfTotalAum(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/etf-aum?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.assets || !Object.keys(data.assets).length) throw new Error('no data');

    const COLORS = { 'BTC': '#F7931A', 'ETH': '#627EEA' };

    // Build common date axis
    let allDates = new Set();
    for (const a of Object.values(data.assets)) a.dates.forEach(d => allDates.add(d));
    allDates = [...allDates].sort();

    // BTC on bottom (larger), ETH stacked on top
    const order = ['BTC', 'ETH'];
    const datasets = [];

    for (const asset of order) {
      const a = data.assets[asset];
      if (!a) continue;
      const dateMap = {};
      a.dates.forEach((d, i) => dateMap[d] = a.aum[i]);

      datasets.push({
        label: asset + ' ETF',
        data: allDates.map(d => dateMap[d] !== undefined ? dateMap[d] : null),
        borderColor: COLORS[asset],
        backgroundColor: COLORS[asset] + '40',
        borderWidth: 1.4,
        pointRadius: 0,
        tension: 0.1,
        fill: true,
        spanGaps: true,
        stack: 'stack1',
      });
    }

    mkChart('line', { labels: allDates, datasets }, {
      scales: {
        x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 10,
          callback: function(val) { const l = this.getLabelForValue(val); return l ? l.slice(0,7) : ''; } }, grid: XGRID },
        y: { stacked: true, ticks: { ...YTICK, callback: function(v) {
          return '$' + v.toFixed(0) + 'B';
        }}, grid: YGRID },
      },
      plugins: {
        legend: { display: true, labels: { color: '#888', font: { size: 11 }, boxWidth: 12 } },
        filler: { propagate: true },
      },
    });

    // Summary bar
    const summaryDiv = document.getElementById('perf-row');
    if (summaryDiv) {
      const items = [];
      let total = 0;
      for (const asset of order) {
        const a = data.assets[asset];
        if (!a) continue;
        const latest = a.aum[a.aum.length - 1];
        total += latest;
        const clr = asset === 'BTC' ? '#F7931A' : '#627EEA';
        const valStr = latest.toFixed(1) + 'B';
        items.push(`<div class="perf-item"><span style="color:${clr};font-weight:600">${asset}</span> <span>$${valStr}</span></div>`);
      }
      const totalStr = total.toFixed(1) + 'B';
      items.push(`<div class="perf-item"><span style="font-weight:600">Total</span> <span>$${totalStr}</span></div>`);
      summaryDiv.innerHTML = items.join('');
    }

    setTitle('Total AuM — Spot ETFs', 'Source: Farside Investors · USD · stacked area');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}


// ── ETF: Daily Bar Chart ─────────────────────────────────────────────────────
