async function fetchBtcDominanceMa(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/btc-dominance-ma?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');

    mkChart('line', {
      labels: data.dates,
      datasets: [
        { label: 'BTC Dominance (%)', data: data.dominance, borderColor: '#F7931A',
          backgroundColor: 'rgba(247,147,26,0.08)', borderWidth: 1.4, pointRadius: 0,
          tension: 0.1, fill: true },
        { label: '50d MA', data: data.ma50, borderColor: '#00D64A',
          backgroundColor: 'transparent', borderWidth: 1.8, pointRadius: 0,
          tension: 0.1, borderDash: [4, 2] },
        { label: '200d MA', data: data.ma200, borderColor: '#EC5B5B',
          backgroundColor: 'transparent', borderWidth: 1.8, pointRadius: 0,
          tension: 0.1, borderDash: [6, 3] },
      ]
    }, {
      scales: {
        x: { type: 'category', ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 10,
          callback: function(val) { const l = this.getLabelForValue(val); return l ? l.slice(0,7) : ''; } }, grid: XGRID },
        y: { ticks: { ...YTICK, callback: v => v.toFixed(1) + '%' }, grid: YGRID },
      },
      plugins: {
        legend: { display: true, labels: { color: '#888', font: { size: 11 }, boxWidth: 12 } },
      },
    });

    // Summary bar
    const summaryDiv = document.getElementById('perf-row');
    if (summaryDiv) {
      const dom = data.dominance[data.dominance.length - 1];
      const m50 = data.ma50.filter(v => v != null).slice(-1)[0];
      const m200 = data.ma200.filter(v => v != null).slice(-1)[0];
      const items = [];
      if (dom != null) items.push(`<div class="perf-item"><span style="color:#F7931A;font-weight:600">Dominance</span> <span>${dom.toFixed(1)}%</span></div>`);
      if (m50 != null) items.push(`<div class="perf-item"><span style="color:#00D64A;font-weight:600">50d MA</span> <span>${m50.toFixed(1)}%</span></div>`);
      if (m200 != null) items.push(`<div class="perf-item"><span style="color:#EC5B5B;font-weight:600">200d MA</span> <span>${m200.toFixed(1)}%</span></div>`);
      if (m50 != null && m200 != null) {
        const gap = m50 - m200;
        const clr = gap > 0 ? '#00D64A' : '#EC5B5B';
        items.push(`<div class="perf-item"><span style="font-weight:600">Gap</span> <span style="color:${clr}">${gap > 0 ? '+' : ''}${gap.toFixed(1)}pp</span></div>`);
      }
      summaryDiv.innerHTML = items.join('');
    }

    setTitle('BTC Dominance — Moving Averages', 'Source: CoinGecko Pro · 50d and 200d MA');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}


