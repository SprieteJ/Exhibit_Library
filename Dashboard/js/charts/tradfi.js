async function fetchTradFi(asset, perfWin, corrWin, smoothWin, from, to) {
  if (chart) { chart.destroy(); chart = null; }
  document.getElementById('chart-empty').style.display = 'none';
  document.getElementById('chart-spin').classList.add('on');

  const ASSET_LABELS = {
    'QQQ':'Nasdaq','SPY':'S&P 500','IWM':'Russell 2k',
    'GLD':'Gold','BNO':'Brent Oil','DX-Y.NYB':'DXY',
    '^VIX':'VIX','TLT':'US 20Y Bond'
  };
  const assetLabel = ASSET_LABELS[asset] || asset;
  const tc = darkMode ? '#E8EAE8' : '#606663';

  try {
    const url = `/api/btc-tradfi?asset=${encodeURIComponent(asset)}&perf_win=${perfWin}&corr_win=${corrWin}&smooth_win=${smoothWin}&from=${from}&to=${to}`;
    const res  = await fetch(url);
    const data = await res.json();
    if (data.error) throw new Error(data.error);

    const dates      = data.dates;
    const btcPerf    = data.btc_perf;
    const assetPerf  = data.asset_perf;
    const corr       = data.corr;
    const corrSmooth = data.corr_smooth;

    // Build two-panel chart using Chart.js with custom plugin
    const canvas = document.getElementById('main-chart');
    const ctx    = canvas.getContext('2d');
    const W = canvas.offsetWidth, H = canvas.offsetHeight;

    // Use Chart.js with two y-axes on same chart as workaround
    // Top 45% = performance, bottom 55% = correlation
    chart = new Chart(canvas, {
      type: 'line',
      data: {
        labels: dates,
        datasets: [
          {
            label: `BTC ${perfWin}d return`,
            data: btcPerf,
            borderColor: '#00D64A',
            backgroundColor: 'transparent',
            borderWidth: 1.4,
            pointRadius: 0,
            yAxisID: 'yPerf',
            spanGaps: true,
            order: 1,
          },
          {
            label: `${assetLabel} ${perfWin}d return`,
            data: assetPerf,
            borderColor: '#2471CC',
            backgroundColor: 'transparent',
            borderWidth: 1.4,
            pointRadius: 0,
            yAxisID: 'yPerf',
            spanGaps: true,
            order: 2,
          },
          {
            label: `${corrWin}d correlation`,
            data: corr,
            borderColor: '#323935',
            backgroundColor: 'transparent',
            borderWidth: 1.4,
            pointRadius: 0,
            yAxisID: 'yCorr',
            spanGaps: true,
            order: 3,
          },
          {
            label: `${smoothWin}d avg`,
            data: corrSmooth,
            borderColor: '#606663',
            backgroundColor: 'transparent',
            borderWidth: 1,
            borderDash: [4, 3],
            pointRadius: 0,
            yAxisID: 'yCorr',
            spanGaps: true,
            order: 4,
          },
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: { duration: 300 },
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: darkMode ? '#1A1D1B' : '#fff',
            borderColor: darkMode ? '#2A2D2B' : '#E4E4E2',
            borderWidth: 1,
            titleColor: darkMode ? '#E8EAE8' : '#323935',
            bodyColor:  darkMode ? '#888B88' : '#606663',
            padding: 12,
            filter: item => item.parsed.y !== null,
            callbacks: {
              label: ctx => {
                const v = ctx.parsed.y;
                if (v == null) return null;
                if (ctx.dataset.yAxisID === 'yPerf') return ` ${ctx.dataset.label}  ${v >= 0 ? '+' : ''}${v.toFixed(1)}%`;
                return ` ${ctx.dataset.label}  ${v.toFixed(3)}`;
              }
            }
          },
          // Draw dividing line between panels
          panelDivider: {
            afterDraw(chart) {
              const yPerf = chart.scales.yPerf;
              const yCorr = chart.scales.yCorr;
              if (!yPerf || !yCorr) return;
              const ctx   = chart.ctx;
              const mid   = (yPerf.bottom + yCorr.top) / 2;
              ctx.save();
              ctx.strokeStyle = tc;
              ctx.lineWidth   = 0.5;
              ctx.setLineDash([]);
              ctx.beginPath();
              ctx.moveTo(chart.chartArea.left,  mid);
              ctx.lineTo(chart.chartArea.right, mid);
              ctx.stroke();
              // Zero lines
              const zeroPerf = yPerf.getPixelForValue(0);
              const zeroCorr = yCorr.getPixelForValue(0);
              ctx.strokeStyle = tc + '66';
              ctx.lineWidth   = 0.5;
              ctx.setLineDash([4, 4]);
              [zeroPerf, zeroCorr].forEach(y => {
                ctx.beginPath();
                ctx.moveTo(chart.chartArea.left,  y);
                ctx.lineTo(chart.chartArea.right, y);
                ctx.stroke();
              });
              // Panel labels
              ctx.setLineDash([]);
              ctx.fillStyle = tc;
              ctx.font      = '400 11px DM Mono, monospace';
              ctx.fillText(`BTC vs ${assetLabel} · ${perfWin}d return`, chart.chartArea.left + 8, yPerf.top + 16);
              ctx.fillText(`${corrWin}d rolling correlation`, chart.chartArea.left + 8, yCorr.top + 16);
              ctx.restore();
            }
          }
        },
        scales: {
          x: {
            type: 'category',
            grid: { display: false },
            border: { color: tc, width: 0.8 },
            ticks: {
              color: tc, font: { family: 'DM Mono, monospace', size: 11 },
              maxRotation: 0, maxTicksLimit: 8,
              callback: function(val) { const l = this.getLabelForValue(val); return l ? l.slice(0,7) : ''; }
            }
          },
          yPerf: {
            type: 'linear',
            position: 'left',
            weight: 1,
            grid: { display: false },
            border: { color: tc, width: 0.8 },
            ticks: {
              color: tc, font: { family: 'DM Mono, monospace', size: 11 },
              callback: v => `${v >= 0 ? '+' : ''}${v.toFixed(0)}%`
            }
          },
          yCorr: {
            type: 'linear',
            position: 'left',
            weight: 2,
            min: -1.05, max: 1.05,
            grid: { display: false },
            border: { color: tc, width: 0.8 },
            ticks: {
              color: tc, font: { family: 'DM Mono, monospace', size: 11 },
              callback: v => v.toFixed(2),
              stepSize: 0.25,
            }
          }
        }
      },
      plugins: [{ id: 'panelDivider', afterDraw(chart) {
        const yPerf = chart.scales.yPerf;
        const yCorr = chart.scales.yCorr;
        if (!yPerf || !yCorr) return;
        const ctx  = chart.ctx;
        const mid  = (yPerf.bottom + yCorr.top) / 2;
        ctx.save();
        ctx.strokeStyle = tc; ctx.lineWidth = 0.5; ctx.setLineDash([]);
        ctx.beginPath(); ctx.moveTo(chart.chartArea.left, mid); ctx.lineTo(chart.chartArea.right, mid); ctx.stroke();
        const zeroPerf = yPerf.getPixelForValue(0);
        const zeroCorr = yCorr.getPixelForValue(0);
        ctx.strokeStyle = tc + '66'; ctx.lineWidth = 0.5; ctx.setLineDash([4,4]);
        [zeroPerf, zeroCorr].forEach(y => { ctx.beginPath(); ctx.moveTo(chart.chartArea.left, y); ctx.lineTo(chart.chartArea.right, y); ctx.stroke(); });
        ctx.setLineDash([]);
        ctx.fillStyle = tc; ctx.font = '400 11px DM Mono, monospace';
        ctx.fillText('Rolling return', chart.chartArea.left + 8, yPerf.top + 14);
        ctx.fillText('Rolling correlation', chart.chartArea.left + 8, yCorr.top + 14);
        ctx.restore();
      }}]
    });

    // Perf row
    const lastCorr = corr.filter(v => v !== null).pop();
    const cls = lastCorr === null ? 'neu' : lastCorr >= 0.5 ? 'pos' : lastCorr <= -0.2 ? 'neg' : 'neu';
    document.getElementById('perf-row').innerHTML = `
      <div class="perf-item"><div class="perf-dot" style="background:#00D64A"></div><span class="perf-sym">BTC</span></div>
      <div class="perf-item"><div class="perf-dot" style="background:#2471CC"></div><span class="perf-sym">${assetLabel}</span></div>
      <div class="perf-item" style="margin-left:12px"><span style="font-size:12px;color:var(--muted)">Corr (latest):</span></div>
      <div class="perf-item"><span class="perf-val ${cls}">${lastCorr !== null ? lastCorr.toFixed(3) : '—'}</span></div>`;

    document.getElementById('chart-title').textContent = `BTC vs ${assetLabel}`;

  } catch(e) {
    document.getElementById('chart-empty').style.display = 'flex';
    document.getElementById('empty-msg').textContent = `Error: ${e.message}`;
  } finally {
    document.getElementById('chart-spin').classList.remove('on');
  }
}

