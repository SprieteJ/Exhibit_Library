async function fetchEthMACombined(from, to) {
  spinOn();
  try {
    const data = await fetch(`/api/eth-ma-combined?from=${from}&to=${to}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.dates?.length) throw new Error('no data');

    // Destroy existing chart
    if (chart) { chart.destroy(); chart = null; }

    const canvas = document.getElementById('main-chart');
    canvas.style.display = 'none';

    // Remove any existing combined container
    let existing = document.getElementById('cc-matrix-container');
    if (existing) existing.remove();
    existing = document.getElementById('combined-chart-container');
    if (existing) existing.remove();

    const container = document.createElement('div');
    container.id = 'combined-chart-container';
    container.style.cssText = 'position:absolute;inset:0;display:flex;flex-direction:column;gap:8px;padding:8px;';

    // Top canvas: Price + 200w MA
    const topDiv = document.createElement('div');
    topDiv.style.cssText = 'flex:3;position:relative;';
    const topCanvas = document.createElement('canvas');
    topDiv.appendChild(topCanvas);
    container.appendChild(topDiv);

    // Bottom canvas: 50d/200d gap
    const botDiv = document.createElement('div');
    botDiv.style.cssText = 'flex:2;position:relative;';
    const botCanvas = document.createElement('canvas');
    botDiv.appendChild(botCanvas);
    container.appendChild(botDiv);

    canvas.parentElement.appendChild(container);

    const tc = darkMode ? '#E8EAE8' : '#606663';
    const gc = darkMode ? 'rgba(232,234,232,0.06)' : 'rgba(50,57,53,0.06)';

    // ── Top chart: Price + 200w MA ──
    const topChart = new Chart(topCanvas.getContext('2d'), {
      type: 'line',
      data: {
        labels: data.dates,
        datasets: [
          { label: 'ETH Price', data: data.price, borderColor: '#A8F5C2', backgroundColor: 'transparent',
            borderWidth: 1.4, pointRadius: 0, tension: 0.1 },
          { label: '200W MA', data: data.ma200w, borderColor: '#FAFAFA', backgroundColor: 'transparent',
            borderWidth: 2, pointRadius: 0, tension: 0.1 },
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        animation: { duration: 200 },
        scales: {
          x: { type: 'category', display: false },
          y: { ticks: { color: tc, font: { size: 11 },
            callback: v => '$' + (v >= 1000 ? (v/1000).toFixed(0) + 'k' : v.toFixed(0)) },
            grid: { color: gc } },
        },
        plugins: {
          legend: { display: true, labels: { color: tc, font: { size: 11 }, boxWidth: 12 } },
          title: { display: true, text: '200-Week Moving Average', color: tc, font: { size: 13, weight: 'normal' }, padding: { bottom: 8 } },
        },
      }
    });

    // ── Bottom chart: 50d/200d gap ──
    const gapData = data.gap_pct;
    const posData = gapData.map(v => v != null && v >= 0 ? v : null);
    const negData = gapData.map(v => v != null && v < 0 ? v : null);

    const botChart = new Chart(botCanvas.getContext('2d'), {
      type: 'line',
      data: {
        labels: data.dates,
        datasets: [
          { label: '50d/200d Gap (%)', data: gapData, borderColor: '#00D64A', backgroundColor: 'transparent',
            borderWidth: 1.2, pointRadius: 0, tension: 0.1,
            fill: { target: 'origin', above: 'rgba(0,214,74,0.15)', below: 'rgba(232,154,154,0.2)' } },
          { label: 'Zero', data: data.dates.map(() => 0), borderColor: darkMode ? 'rgba(255,255,255,0.3)' : 'rgba(0,0,0,0.4)',
            borderWidth: 0.8, pointRadius: 0, borderDash: [] },
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        animation: { duration: 200 },
        scales: {
          x: { type: 'category', ticks: { color: tc, font: { size: 11 }, maxRotation: 0, maxTicksLimit: 10,
            callback: function(val) { const l = this.getLabelForValue(val); return l ? l.slice(0,4) : ''; } },
            grid: { display: false } },
          y: { ticks: { color: tc, font: { size: 11 }, callback: v => v + '%' }, grid: { color: gc } },
        },
        plugins: {
          legend: { display: false },
          title: { display: true, text: '50-Day / 200-Day Moving Average Gap', color: tc, font: { size: 13, weight: 'normal' }, padding: { bottom: 8 } },
        },
      }
    });

    // Store reference so cleanup works
    chart = topChart;
    chart._combinedBot = botChart;
    chart._combinedContainer = container;

    setTitle('ETH Moving Averages — Combined', 'Source: CoinGecko · 200-week MA + 50d/200d gap');
    document.getElementById('chart-spin').classList.remove('on');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}


// ── ETH: Moving Averages ──────────────────────────────────────────────────────
