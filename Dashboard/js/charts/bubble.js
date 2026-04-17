async function fetchBubble(from, to, window) {
  // Bubble chart uses Chart.js scatter with bubble type
  if (chart) { chart.destroy(); chart = null; }
  document.getElementById('chart-empty').style.display = 'none';
  document.getElementById('chart-spin').classList.add('on');

  try {
    const res  = await fetch(`/api/sector-bubble?to=${to}&window=${window}`);
    const data = await res.json();
    if (data.error) throw new Error(data.error);

    const sectors = Object.keys(data).filter(k => data[k].x !== null && data[k].y !== null);
    if (!sectors.length) throw new Error('no data');

    // Max mcap for bubble sizing
    const maxMcap = Math.max(...sectors.map(k => data[k].mcap || 0));
    const tc = darkMode ? '#E8EAE8' : '#606663';

    chart = new Chart(document.getElementById('main-chart'), {
      type: 'bubble',
      data: {
        datasets: sectors.map(sector => ({
          label: sector,
          data: [{
            x: data[sector].x,
            y: data[sector].y,
            r: maxMcap > 0 ? Math.max(6, Math.sqrt(data[sector].mcap / maxMcap) * 40) : 12,
          }],
          backgroundColor: (data[sector].color || '#888') + 'AA',
          borderColor:     data[sector].color || '#888',
          borderWidth: 1.5,
        }))
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        animation: { duration: 400 },
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: darkMode ? '#1A1D1B' : '#fff',
            borderColor: darkMode ? '#2A2D2B' : '#E4E4E2',
            borderWidth: 1,
            titleColor: darkMode ? '#E8EAE8' : '#323935',
            bodyColor:  darkMode ? '#888B88' : '#606663',
            padding: 12,
            callbacks: {
              title: ctx => ctx[0].dataset.label,
              label: ctx => {
                const s = ctx.dataset.label;
                const d = data[s];
                const mcap = d.mcap >= 1e9 ? `$${(d.mcap/1e9).toFixed(1)}B` : `$${(d.mcap/1e6).toFixed(0)}M`;
                return [
                  ` Momentum: ${d.y >= 0 ? '+' : ''}${d.y.toFixed(1)}%`,
                  ` Autocorr: ${d.x.toFixed(3)}`,
                  ` Mkt cap: ${mcap}`,
                  ` Assets: ${d.count}`,
                ];
              }
            }
          }
        },
        scales: {
          x: {
            title: { display: true, text: 'Autocorrelation (lag-1)', color: tc, font: { family: 'DM Mono, monospace', size: 11 } },
            grid: { display: false },
            border: { color: tc, width: 0.8 },
            ticks: { color: tc, font: { family: 'DM Mono, monospace', size: 11 }, callback: v => v.toFixed(2) }
          },
          y: {
            title: { display: true, text: `${window}d momentum (%)`, color: tc, font: { family: 'DM Mono, monospace', size: 11 } },
            grid: { display: false },
            border: { color: tc, width: 0.8 },
            ticks: { color: tc, font: { family: 'DM Mono, monospace', size: 11 }, callback: v => `${v >= 0 ? '+' : ''}${v.toFixed(0)}%` }
          }
        }
      }
    });

    // Draw sector labels on bubbles
    // Draw sector labels directly on canvas (not via plugin registration)
    chart.update();
    setTimeout(() => {
      const ctx2 = chart.canvas.getContext('2d');
      chart.data.datasets.forEach((ds, i) => {
        const meta = chart.getDatasetMeta(i);
        if (!meta.visible) return;
        meta.data.forEach(point => {
          ctx2.save();
          ctx2.fillStyle = darkMode ? '#E8EAE8' : '#323935';
          ctx2.font = '500 11px DM Sans, sans-serif';
          ctx2.textAlign = 'center';
          ctx2.textBaseline = 'middle';
          ctx2.fillText(ds.label, point.x, point.y);
          ctx2.restore();
        });
      });
    }, 50);

    // Perf row
    document.getElementById('perf-row').innerHTML = sectors.map(s => {
      const d = data[s];
      const cls = d.y === null ? 'neu' : d.y >= 0 ? 'pos' : 'neg';
      const fmt = d.y !== null ? `${d.y >= 0 ? '+' : ''}${d.y.toFixed(1)}%` : '—';
      return `<div class="perf-item">
        <div class="perf-dot" style="background:${d.color}"></div>
        <span class="perf-sym">${s}</span>
        <span class="perf-val ${cls}">${fmt}</span>
      </div>`;
    }).join('');

    document.getElementById('chart-title').textContent = 'Momentum vs autocorrelation';

  } catch(e) {
    document.getElementById('chart-empty').style.display = 'flex';
    document.getElementById('empty-msg').textContent = `Error: ${e.message}`;
  } finally {
    document.getElementById('chart-spin').classList.remove('on');
  }
}

