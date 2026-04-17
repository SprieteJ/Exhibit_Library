async function fetchAltZscoreMom(from, to) {
  spinOn();
  try {
    if (!selected?.length) {
      document.getElementById('chart-spin').classList.remove('on');
      document.getElementById('chart-empty').style.display = 'flex';
      document.getElementById('empty-msg').textContent = 'Select assets in the sidebar';
      return;
    }

    const data = await fetch(`/api/alt-zscore-mom?symbols=${encodeURIComponent(selected.join(','))}&from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.assets || !Object.keys(data.assets).length) throw new Error('no data');

    const PAL_LOCAL = ['#F7931A','#627EEA','#00D64A','#EC5B5B','#2471CC','#AEA9EA','#E1C87E','#DB33CB','#9EA4A0','#C084FC','#F87171','#34D399','#FBBF24','#A78BFA','#F472B6'];

    let allDates = new Set();
    for (const a of Object.values(data.assets)) a.dates.forEach(d => allDates.add(d));
    allDates = [...allDates].sort();

    const datasets = [];
    let i = 0;
    for (const [sym, a] of Object.entries(data.assets)) {
      const dateMap = {};
      a.dates.forEach((d, j) => dateMap[d] = a.zscore[j]);
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

    // Zero line
    datasets.push({
      label: 'Mean',
      data: allDates.map(() => 0),
      borderColor: darkMode ? 'rgba(255,255,255,0.15)' : 'rgba(0,0,0,0.15)',
      borderWidth: 1, pointRadius: 0, borderDash: [4, 4],
    });

    mkChart('line', { labels: allDates, datasets }, {
      scales: {
        x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 10,
          callback: function(val) { const l = this.getLabelForValue(val); return l ? l.slice(0,7) : ''; } }, grid: XGRID },
        y: { ticks: { ...YTICK, callback: v => v.toFixed(1) + 'σ' }, grid: YGRID },
      },
      plugins: {
        legend: { display: true, position: 'top', labels: { color: '#888', font: { size: 10 }, boxWidth: 10, padding: 8 },
          onClick: function(e, legendItem, legend) {
            if (legend.chart.data.datasets[legendItem.datasetIndex].label === 'Mean') return;
            Chart.defaults.plugins.legend.onClick.call(this, e, legendItem, legend);
          }
        },
      },
    });

    // Summary: current z-scores
    const summaryDiv = document.getElementById('perf-row');
    if (summaryDiv) {
      const sorted = Object.entries(data.assets)
        .map(([sym, a]) => ({ sym, z: a.current }))
        .filter(a => a.z != null)
        .sort((a, b) => b.z - a.z);
      summaryDiv.innerHTML = sorted.map(a => {
        const clr = a.z > 1 ? '#00D64A' : (a.z < -1 ? '#EC5B5B' : '#888');
        return `<div class="perf-item"><span style="font-weight:600">${a.sym}</span> <span style="color:${clr}">${a.z > 0 ? '+' : ''}${a.z.toFixed(2)}σ</span></div>`;
      }).join('');
    }

    setTitle('Z-Scored Momentum (14d ret / 14d vol, z over 90d)', 'Source: CoinGecko Pro · click legend to toggle');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}


