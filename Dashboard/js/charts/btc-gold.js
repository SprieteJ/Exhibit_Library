async function fetchBtcGold(from, to) {
  if (chart) { chart.destroy(); chart = null; }
  document.getElementById('chart-empty').style.display = 'none';
  document.getElementById('chart-spin').classList.add('on');

  try {
    const resp = await fetch(`/api/btc-gold?from=${from}&to=${to}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    if (data.error) throw new Error(data.error);
    if (!data.dates?.length) throw new Error('no data');

    const tc = darkMode ? '#E8EAE8' : '#606663';

    chart = new Chart(document.getElementById('main-chart'), {
      type: 'line',
      data: {
        labels: data.dates,
        datasets: [
          {
            label: 'BTC',
            data: data.btc_prices,
            borderColor: '#F7931A',
            backgroundColor: 'transparent',
            borderWidth: 1.8,
            pointRadius: 0, pointHoverRadius: 4,
            tension: 0.1, yAxisID: 'yBTC', spanGaps: true,
          },
          {
            label: 'Gold (GLD)',
            data: data.gold_prices,
            borderColor: '#FFB800',
            backgroundColor: 'transparent',
            borderWidth: 1.5,
            pointRadius: 0, pointHoverRadius: 4,
            tension: 0.1, yAxisID: 'yGold', spanGaps: true,
          }
        ]
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
                if (v == null) return ` ${ctx.dataset.label}  —`;
                return ctx.dataset.label === 'BTC'
                  ? ` BTC  $${v.toLocaleString(undefined, {maximumFractionDigits:0})}`
                  : ` Gold (GLD)  $${v.toFixed(2)}`;
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
          yBTC: {
            type: 'linear', position: 'left',
            grid: { display: false }, border: { color: '#F7931A', width: 0.8 },
            ticks: {
              color: '#F7931A', font: { family: 'DM Mono, monospace', size: 11 },
              callback: v => `$${v >= 1000 ? (v/1000).toFixed(0)+'k' : v.toFixed(0)}`
            }
          },
          yGold: {
            type: 'linear', position: 'right',
            grid: { display: false }, border: { color: '#FFB800', width: 0.8 },
            ticks: {
              color: '#FFB800', font: { family: 'DM Mono, monospace', size: 11 },
              callback: v => `$${v.toFixed(0)}`
            }
          }
        }
      }
    });

    const btcLast  = data.btc_prices.filter(v=>v!=null).slice(-1)[0];
    const goldLast = data.gold_prices.filter(v=>v!=null).slice(-1)[0];
    document.getElementById('perf-row').innerHTML = `
      <div class="perf-item"><div class="perf-dot" style="background:#F7931A"></div><span class="perf-sym">BTC</span><span class="perf-val neu">$${btcLast?.toLocaleString(undefined,{maximumFractionDigits:0}) || '—'}</span></div>
      <div class="perf-item"><div class="perf-dot" style="background:#FFB800"></div><span class="perf-sym">Gold (GLD)</span><span class="perf-val neu">$${goldLast?.toFixed(2) || '—'}</span></div>`;
    document.getElementById('chart-title').textContent = 'Bitcoin vs Gold';

  } catch(e) {
    document.getElementById('chart-empty').style.display = 'flex';
    document.getElementById('empty-msg').textContent = `Error: ${e.message}`;
  } finally {
    document.getElementById('chart-spin').classList.remove('on');
  }
}

