async function fetchAltRebase(from, to) {
  spinOn();
  try {
    if (!selected?.length) {
      document.getElementById('chart-spin').classList.remove('on');
      document.getElementById('chart-empty').style.display = 'flex';
      document.getElementById('empty-msg').textContent = 'Select assets in the sidebar';
      return;
    }

    const data = await fetch(`/api/alt-rebase?symbols=${encodeURIComponent(selected.join(','))}&from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.assets || !Object.keys(data.assets).length) throw new Error('no data');

    const PAL_LOCAL = ['#F7931A','#627EEA','#00D64A','#EC5B5B','#2471CC','#AEA9EA','#E1C87E','#DB33CB','#9EA4A0','#C084FC','#F87171','#34D399','#FBBF24','#A78BFA','#F472B6'];

    let allDates = new Set();
    for (const a of Object.values(data.assets)) a.dates.forEach(d => allDates.add(d));
    allDates = [...allDates].sort();

    const datasets = [];
    let i = 0;
    for (const [sym, a] of Object.entries(data.assets)) {
      const dateMap = {};
      a.dates.forEach((d, j) => dateMap[d] = a.rebased[j]);
      datasets.push({
        label: sym,
        data: allDates.map(d => dateMap[d] ?? null),
        borderColor: PAL_LOCAL[i % PAL_LOCAL.length],
        backgroundColor: 'transparent',
        borderWidth: 1.6,
        pointRadius: 0,
        tension: 0.1,
        spanGaps: true,
      });
      i++;
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

    // Summary: performance from 100
    const summaryDiv = document.getElementById('perf-row');
    if (summaryDiv) {
      const perfs = Object.entries(data.assets)
        .map(([sym, a]) => ({ sym, perf: a.rebased[a.rebased.length - 1] - 100 }))
        .sort((a, b) => b.perf - a.perf);
      summaryDiv.innerHTML = perfs.map(p => {
        const clr = p.perf >= 0 ? '#00D64A' : '#EC5B5B';
        return `<div class="perf-item"><span style="font-weight:600">${p.sym}</span> <span style="color:${clr}">${p.perf >= 0 ? '+' : ''}${p.perf.toFixed(1)}%</span></div>`;
      }).join('');
    }

    setTitle('Rebased Performance', 'Source: CoinGecko Pro · rebased to 100 · click legend to toggle');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── Altcoins: Deep Dive — Z-Scored Momentum ──────────────────────────────────
