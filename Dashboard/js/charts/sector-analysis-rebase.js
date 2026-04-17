async function fetchSectorAnalysisRebase(from, to) {
  spinOn();
  try {
    // Get selected sector from sidebar or default
    const sectorSel = selected?.length ? selected[0] : 'Layer 2';

    const data = await fetch(`/api/sector-analysis-rebase?sector=${encodeURIComponent(sectorSel)}&from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.assets || !Object.keys(data.assets).length) throw new Error('no data');

    const PAL_LOCAL = ['#F7931A','#627EEA','#00D64A','#EC5B5B','#2471CC','#AEA9EA','#E1C87E','#DB33CB','#9EA4A0','#C084FC','#F87171','#34D399'];

    // Build common date axis
    let allDates = new Set();
    for (const a of Object.values(data.assets)) a.dates.forEach(d => allDates.add(d));
    allDates = [...allDates].sort();

    const datasets = [];
    let colorIdx = 0;

    // Benchmarks and index first (dashed, thicker)
    for (const key of ['_INDEX', '_ALTS', '_ETH']) {
      const a = data.assets[key];
      if (!a) continue;
      const dateMap = {};
      a.dates.forEach((d, i) => dateMap[d] = a.rebased[i]);

      datasets.push({
        label: a.label,
        data: allDates.map(d => dateMap[d] ?? null),
        borderColor: a.color,
        backgroundColor: 'transparent',
        borderWidth: key === '_INDEX' ? 2.5 : 1.8,
        pointRadius: 0,
        tension: 0.1,
        spanGaps: true,
        borderDash: key === '_INDEX' ? [] : [5, 3],
      });
    }

    // Constituents
    for (const [sym, a] of Object.entries(data.assets)) {
      if (sym.startsWith('_')) continue;
      const dateMap = {};
      a.dates.forEach((d, i) => dateMap[d] = a.rebased[i]);

      datasets.push({
        label: sym,
        data: allDates.map(d => dateMap[d] ?? null),
        borderColor: PAL_LOCAL[colorIdx % PAL_LOCAL.length],
        backgroundColor: 'transparent',
        borderWidth: 1.4,
        pointRadius: 0,
        tension: 0.1,
        spanGaps: true,
      });
      colorIdx++;
    }

    mkChart('line', { labels: allDates, datasets }, {
      scales: {
        x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 10,
          callback: function(val) { const l = this.getLabelForValue(val); return l ? l.slice(0,7) : ''; } }, grid: XGRID },
        y: { ticks: { ...YTICK, callback: v => v.toFixed(0) }, grid: YGRID },
      },
      plugins: {
        legend: { display: true, position: 'top', labels: { color: '#888', font: { size: 10 }, boxWidth: 10, padding: 8 } },
      },
    });

    // Summary bar: current rebased values
    const summaryDiv = document.getElementById('perf-row');
    if (summaryDiv) {
      const items = [];
      // Show index performance
      const idx = data.assets['_INDEX'];
      if (idx) {
        const last = idx.rebased[idx.rebased.length - 1];
        const perf = last - 100;
        const clr = perf >= 0 ? '#00D64A' : '#EC5B5B';
        items.push(`<div class="perf-item"><span style="color:${data.color};font-weight:600">${sectorSel}</span> <span style="color:${clr}">${perf >= 0 ? '+' : ''}${perf.toFixed(1)}%</span></div>`);
      }
      // Top 3 and bottom 3 constituents
      const perfs = Object.entries(data.assets)
        .filter(([k]) => !k.startsWith('_'))
        .map(([sym, a]) => ({ sym, perf: a.rebased[a.rebased.length - 1] - 100 }))
        .sort((a, b) => b.perf - a.perf);
      const show = [...perfs.slice(0, 3), ...perfs.slice(-2)];
      for (const p of show) {
        const clr = p.perf >= 0 ? '#00D64A' : '#EC5B5B';
        items.push(`<div class="perf-item"><span style="font-weight:500">${p.sym}</span> <span style="color:${clr}">${p.perf >= 0 ? '+' : ''}${p.perf.toFixed(1)}%</span></div>`);
      }
      summaryDiv.innerHTML = items.join('');
    }

    setTitle('Rebased Performance — ' + sectorSel, 'Source: CoinGecko Pro · all constituents rebased to 100 · click legend to toggle');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}


