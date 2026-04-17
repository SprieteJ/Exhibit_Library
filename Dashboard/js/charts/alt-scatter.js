async function fetchAltScatter(from, to, days) {
  if (chart) { chart.destroy(); chart = null; }
  document.getElementById('chart-empty').style.display = 'none';
  document.getElementById('chart-spin').classList.add('on');

  const topN = parseInt(document.getElementById('rp-topn')?.value || '50');

  try {
    const res  = await fetch(`/api/alt-scatter?from=${from}&to=${to}&days=${days}&topn=${topN}`);
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    if (!data.points?.length) throw new Error('no data');

    const tc = darkMode ? '#E8EAE8' : '#606663';

    chart = new Chart(document.getElementById('main-chart'), {
      type: 'scatter',
      data: {
        datasets: [{
          label: 'Altcoins',
          data: data.points.map(p => ({ x: p.vol, y: p.perf, label: p.symbol })),
          backgroundColor: 'transparent',
          borderColor: 'transparent',
          pointRadius: 0,
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        animation: { duration: 300 },
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: darkMode ? '#1A1D1B' : '#fff',
            borderColor: darkMode ? '#2A2D2B' : '#E4E4E2',
            borderWidth: 1, padding: 10,
            titleColor: darkMode ? '#E8EAE8' : '#323935',
            bodyColor:  darkMode ? '#888B88' : '#606663',
            callbacks: {
              title: ctx => ctx[0].raw.label,
              label: ctx => [` Perf vs BTC: ${ctx.raw.y >= 0 ? '+' : ''}${ctx.raw.y.toFixed(1)}%`, ` Vol vs BTC: ${ctx.raw.x.toFixed(1)}%`]
            }
          }
        },
        scales: {
          x: {
            reverse: true,
            title: { display: true, text: '← high vol    volatility vs BTC (%)    low vol →', color: tc, font: { family: 'DM Mono, monospace', size: 11 } },
            grid: { display: false }, border: { color: tc, width: 0.8 },
            ticks: { color: tc, font: { family: 'DM Mono, monospace', size: 11 }, callback: v => `${v.toFixed(0)}%` }
          },
          y: {
            type: document.getElementById('rp-logscale')?.checked ? 'logarithmic' : 'linear',
            title: { display: true, text: '% performance vs BTC', color: tc, font: { family: 'DM Mono, monospace', size: 11 } },
            grid: { display: false }, border: { color: tc, width: 0.8 },
            ticks: { color: tc, font: { family: 'DM Mono, monospace', size: 11 }, callback: v => `${v >= 0 ? '+' : ''}${v.toFixed(0)}%` }
          }
        }
      },
      plugins: [{
        id: 'scatterOverlay',
        afterDatasetsDraw(ch) {
          const ctx2  = ch.ctx;
          const ds    = ch.data.datasets[0];
          const meta  = ch.getDatasetMeta(0);
          const xAxis = ch.scales.x;
          const yAxis = ch.scales.y;
          const pts   = (ds?.data || []).filter(p => p && p.x != null && p.y != null);
          if (!pts.length) return;

          const sortX = pts.map(p => p.x).sort((a,b) => a-b);
          const sortY = pts.map(p => p.y).sort((a,b) => a-b);
          const medX  = sortX[Math.floor(sortX.length / 2)];
          const medY  = sortY[Math.floor(sortY.length / 2)];
          const mxPx  = xAxis.getPixelForValue(medX);
          const myPx  = yAxis.getPixelForValue(medY);

          ctx2.save();
          ctx2.strokeStyle = darkMode ? 'rgba(255,255,255,0.18)' : 'rgba(0,0,0,0.12)';
          ctx2.lineWidth = 1;
          ctx2.setLineDash([5, 5]);
          ctx2.beginPath(); ctx2.moveTo(mxPx, yAxis.top);    ctx2.lineTo(mxPx, yAxis.bottom); ctx2.stroke();
          ctx2.beginPath(); ctx2.moveTo(xAxis.left, myPx);   ctx2.lineTo(xAxis.right, myPx);  ctx2.stroke();
          ctx2.setLineDash([]);

          const qc = darkMode ? 'rgba(255,255,255,0.2)' : 'rgba(0,0,0,0.15)';
          ctx2.font = '400 10px DM Mono, monospace';
          ctx2.fillStyle = qc;
          ctx2.textAlign = 'right'; ctx2.textBaseline = 'top';
          ctx2.fillText('outperform · low vol',    xAxis.left + 4,    yAxis.top + 6);
          ctx2.textAlign = 'left';
          ctx2.fillText('outperform · high vol',   xAxis.right - 4,   yAxis.top + 6);
          ctx2.textAlign = 'right'; ctx2.textBaseline = 'bottom';
          ctx2.fillText('underperform · low vol',  xAxis.left + 4,    yAxis.bottom - 6);
          ctx2.textAlign = 'left';
          ctx2.fillText('underperform · high vol', xAxis.right - 4,   yAxis.bottom - 6);

          ctx2.font = '500 11px DM Sans, sans-serif';
          ctx2.textAlign = 'center';
          ctx2.textBaseline = 'bottom';
          meta.data.forEach((pt, i) => {
            const p = ds.data[i];
            if (!p) return;
            ctx2.fillStyle = p.y >= 0 ? '#00D64A' : '#EC5B5B';
            ctx2.fillText(p.label, pt.x, pt.y - 4);
          });
          ctx2.restore();
        }
      }]
    });

    // Zero lines
    chart.options.plugins.annotation = {};

    document.getElementById('chart-title').textContent = `Top ${topN} altcoins — performance vs BTC`;
    document.getElementById('perf-row').innerHTML = `
      <span style="font-family:var(--mono);font-size:12px;color:var(--muted)">${data.points.length} assets · ${days}d window</span>`;

  } catch(e) {
    document.getElementById('chart-empty').style.display = 'flex';
    document.getElementById('empty-msg').textContent = `Error: ${e.message}`;
  } finally {
    document.getElementById('chart-spin').classList.remove('on');
  }
}
