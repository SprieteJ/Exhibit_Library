async function fetchBtcRolling(from, to, window) {
  if (chart) { chart.destroy(); chart = null; }
  document.getElementById('chart-empty').style.display = 'none';
  document.getElementById('chart-spin').classList.add('on');
  try {
    const res  = await fetch(`/api/btc-rolling?from=${from}&to=${to}&window=${window}`);
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    if (!data.dates?.length) throw new Error('no data');

    const tc = darkMode ? '#E8EAE8' : '#606663';
    // Filter out leading nulls
    const points = data.dates
      .map((d, i) => ({ date: d, value: data.values[i] }))
      .filter(p => p.value !== null);
    if (!points.length) throw new Error('insufficient data for window');

    const labels       = points.map(p => p.date);
    const values       = points.map(p => p.value);
    const bgColors     = values.map(v => v >= 0 ? 'rgba(0,214,74,0.65)' : 'rgba(220,53,69,0.65)');
    const borderColors = values.map(v => v >= 0 ? '#00D64A' : '#DC3545');

    chart = new Chart(document.getElementById('main-chart'), {
      type: 'bar',
      data: {
        labels,
        datasets: [{
          label: `${window}d rolling return`,
          data: values,
          backgroundColor: bgColors,
          borderColor: borderColors,
          borderWidth: 0,
          borderRadius: 1,
          barPercentage: 0.9,
          categoryPercentage: 1.0,
        }]
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
            borderWidth: 1, padding: 12,
            titleColor: darkMode ? '#E8EAE8' : '#323935',
            bodyColor:  darkMode ? '#888B88' : '#606663',
            callbacks: {
              label: ctx => {
                const v = ctx.parsed.y;
                const sign = v >= 0 ? '+' : '';
                return ` ${window}d return  ${sign}${v.toFixed(2)}%`;
              }
            }
          }
        },
        scales: {
          x: {
            type: 'category', grid: { display: false },
            border: { color: tc, width: 0.8 },
            ticks: {
              color: tc, font: { family: 'DM Mono, monospace', size: 11 },
              maxRotation: 0, maxTicksLimit: 8,
              callback: function(val) { const l = this.getLabelForValue(val); return l ? l.slice(0,7) : ''; }
            }
          },
          y: {
            grid: { color: darkMode ? '#2A2D2B' : '#E8EAE6' },
            border: { color: tc, width: 0.8 },
            ticks: {
              color: tc, font: { family: 'DM Mono, monospace', size: 11 },
              callback: v => `${v >= 0 ? '+' : ''}${v.toFixed(0)}%`
            }
          }
        }
      }
    });

    const last = values[values.length - 1];
    const cls  = last >= 0 ? 'pos' : 'neg';
    const sign = last >= 0 ? '+' : '';
    document.getElementById('perf-row').innerHTML = `
      <div class="perf-item">
        <div class="perf-dot" style="background:${last >= 0 ? '#00D64A' : '#DC3545'}"></div>
        <span class="perf-sym">BTC ${window}d return</span>
        <span class="perf-val ${cls}">${sign}${last.toFixed(2)}%</span>
      </div>`;
    document.getElementById('chart-title').textContent = `BTC ${window}d rolling return`;
  } catch(e) {
    document.getElementById('chart-empty').style.display = 'flex';
    document.getElementById('empty-msg').textContent = `Error: ${e.message}`;
  } finally {
    document.getElementById('chart-spin').classList.remove('on');
  }
}

