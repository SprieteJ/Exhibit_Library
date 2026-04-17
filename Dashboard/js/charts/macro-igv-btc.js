async function fetchMacroIgvBtc(from, to) {
  const _v = _navVersion;
  spinOn();
  try {
    const data = await fetch(`/api/macro-igv-btc?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (_v !== _navVersion) return;
    const igvFirst = data.igv.findIndex(v => v !== null);
    const btcFirst = data.btc.findIndex(v => v !== null);
    const startIdx = Math.max(igvFirst, btcFirst);
    const igvBase = data.igv[startIdx];
    const btcBase = data.btc[startIdx];
    const igvRebased = data.igv.map(v => v !== null ? v / igvBase * 100 : null);
    const btcRebased = data.btc.map(v => v !== null ? v / btcBase * 100 : null);
    mkChart('line', {
      labels: data.dates,
      datasets: [
        { label: 'IGV (US Software)', data: igvRebased, borderColor: '#2471CC', backgroundColor: 'transparent', borderWidth: 1.5, pointRadius: 0, yAxisID: 'y', tension: 0.1 },
        { label: 'BTC', data: btcRebased, borderColor: '#F7931A', backgroundColor: 'transparent', borderWidth: 1.5, pointRadius: 0, yAxisID: 'y', tension: 0.1 },
        { label: '30d Correlation', data: data.correlation, borderColor: darkMode ? '#746BE6' : '#5B4FCF', backgroundColor: 'transparent', borderWidth: 1, pointRadius: 0, yAxisID: 'y1', borderDash: [4,3], tension: 0.3 },
      ]
    }, {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: true, position: 'top', labels: { usePointStyle: true, pointStyle: 'line', padding: 16, font: { size: 11 }, color: darkMode ? '#AAA' : '#555' } },
        tooltip: { callbacks: {
          label: function(ctx) {
            if (ctx.datasetIndex <= 1) return ctx.dataset.label + ': ' + (ctx.parsed.y?.toFixed(1) || '\u2014');
            return 'Correlation: ' + (ctx.parsed.y?.toFixed(3) || '\u2014');
          }
        }}
      },
      scales: {
        x: { ticks: { maxTicksToAuto: 10, font: { size: 10 }, color: darkMode ? '#888' : '#999' }, grid: { color: darkMode ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.06)' } },
        y: { position: 'left', title: { display: true, text: 'Rebased (100)', font: { size: 11 }, color: darkMode ? '#888' : '#999' }, ticks: { font: { size: 10 }, color: darkMode ? '#888' : '#999' }, grid: { color: darkMode ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.06)' } },
        y1: { position: 'right', min: -1, max: 1, title: { display: true, text: 'Correlation', font: { size: 11 }, color: darkMode ? '#888' : '#999' }, ticks: { font: { size: 10 }, color: darkMode ? '#888' : '#999' }, grid: { drawOnChartArea: false } },
      }
    });
    setTitle('US Software (IGV) vs BTC', 'Rebased price comparison + 30d rolling correlation');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

