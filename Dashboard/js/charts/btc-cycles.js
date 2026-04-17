async function fetchBtcCycles(from, to, window) {
  if (chart) { chart.destroy(); chart = null; }
  document.getElementById('chart-empty').style.display = 'none';
  document.getElementById('chart-spin').classList.add('on');
  try {
    const res  = await fetch(`/api/btc-cycles`);
    const data = await res.json();
    if (data.error) throw new Error(data.error);

    const PAL_BTC = ['#00D64A','#2471CC','#746BE6'];
    const tc = darkMode ? '#E8EAE8' : '#606663';
    const keys = Object.keys(data);

    chart = new Chart(document.getElementById('main-chart'), {
      type: 'line',
      data: {
        datasets: keys.map((k, i) => ({
          label: k,
          data:  data[k].x.map((x, j) => ({ x, y: data[k].y[j] })),
          borderColor: PAL_BTC[i % PAL_BTC.length],
          backgroundColor: 'transparent',
          borderWidth: i === keys.length - 1 ? 1.8 : 1.3,
          pointRadius: 0, pointHoverRadius: 4,
          tension: 0.1, spanGaps: true,
        }))
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        animation: { duration: 300 },
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: darkMode ? '#1A1D1B' : '#fff',
            borderColor: darkMode ? '#2A2D2B' : '#E4E4E2',
            borderWidth: 1, titleColor: darkMode ? '#E8EAE8' : '#323935',
            bodyColor: darkMode ? '#888B88' : '#606663', padding: 12,
            callbacks: {
              title: ctx => `Day ${ctx[0].parsed.x}`,
              label: ctx => ` ${ctx.dataset.label}  ${ctx.parsed.y.toFixed(1)}`
            }
          }
        },
        scales: {
          x: {
            type: 'linear',
            title: { display: true, text: 'Days since peak', color: tc, font: { family: 'DM Mono, monospace', size: 11 } },
            grid: { display: false }, border: { color: tc, width: 0.8 },
            ticks: { color: tc, font: { family: 'DM Mono, monospace', size: 11 } }
          },
          y: {
            title: { display: true, text: 'Indexed to peak (peak = 100)', color: tc, font: { family: 'DM Mono, monospace', size: 11 } },
            grid: { display: false }, border: { color: tc, width: 0.8 },
            ticks: { color: tc, font: { family: 'DM Mono, monospace', size: 11 }, callback: v => v.toFixed(0) }
          }
        }
      }
    });

    document.getElementById('perf-row').innerHTML = keys.map((k, i) => {
      const last = data[k].y[data[k].y.length - 1];
      const col  = PAL_BTC[i % PAL_BTC.length];
      const cls  = last !== null ? (last >= 100 ? 'pos' : 'neg') : 'neu';
      return `<div class="perf-item">
        <div class="perf-dot" style="background:${col}"></div>
        <span class="perf-sym">${k}</span>
        <span class="perf-val ${cls}">${last ? last.toFixed(1) : '—'}</span>
      </div>`;
    }).join('');

    document.getElementById('chart-title').textContent = 'BTC bear market cycles';
  } catch(e) {
    document.getElementById('chart-empty').style.display = 'flex';
    document.getElementById('empty-msg').textContent = `Error: ${e.message}`;
  } finally {
    document.getElementById('chart-spin').classList.remove('on');
  }
}

