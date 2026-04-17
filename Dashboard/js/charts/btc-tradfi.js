async function fetchBtcChart(type, from, to, windowDays) {
  if (chart) { chart.destroy(); chart = null; }
  document.getElementById('chart-empty').style.display = 'none';
  document.getElementById('chart-spin').classList.add('on');

  const COLORS = { "Epoch 3 (2016)": "#00D64A", "Epoch 4 (2020)": "#2471CC", "Epoch 5 (2024)": "#746BE6",
                   "2017/18 Bear": "#00D64A", "2021/22 Bear": "#2471CC", "2025 Bear (ongoing)": "#746BE6" };

  try {
    const days = windowDays || (type === 'epochs' ? 1400 : 1000);
    const url  = type === 'epochs'
      ? `/api/btc-epochs?days=${days}`
      : `/api/btc-cycles?days=${days}`;

    const res  = await fetch(url);
    const data = await res.json();
    if (data.error) throw new Error(data.error);

    const labels = Object.keys(data);
    if (!labels.length) throw new Error('no data');

    const tc = darkMode ? '#E8EAE8' : '#606663';

    const datasets = labels.map(label => ({
      label,
      data:             data[label].values,
      borderColor:      COLORS[label] || '#888',
      backgroundColor:  'transparent',
      borderWidth:      label.includes('ongoing') || label.includes('2024') ? 1.8 : 1.2,
      pointRadius:      0,
      pointHoverRadius: 4,
      tension:          0.1,
      fill:             false,
      spanGaps:         true,
    }));

    // All epochs/cycles share the same day-based x axis
    const maxDays  = Math.max(...labels.filter(l => data[l]?.days?.length).map(l => data[l].days[data[l].days.length - 1]));
    const allDays  = Array.from({length: maxDays + 1}, (_, i) => i);

    // Add 1x reference line for epochs chart only (after allDays is defined)
    if (type === 'epochs') {
      datasets.push({
        label:           '1x (halving price)',
        data:            allDays.map(() => 1),
        borderColor:     darkMode ? 'rgba(255,255,255,0.2)' : 'rgba(0,0,0,0.2)',
        backgroundColor: 'transparent',
        borderWidth:     1,
        borderDash:      [4, 4],
        pointRadius:     0,
        pointHoverRadius:0,
        tension:         0,
        fill:            false,
        spanGaps:        true,
      });
    }

    // Align values to full day array
    const validLabels = labels.filter(l => data[l]?.days?.length && data[l]?.values?.length);
    if (!validLabels.length) throw new Error('no data returned from API');
    const alignedDatasets = datasets.filter((ds, i) => data[labels[i]]?.days?.length).map((ds, i) => {
      const li = labels.indexOf(ds.label);
      const label  = ds.label;
      const daysArr = data[label]?.days || [];
      const valsArr = data[label]?.values || [];
      const map    = {};
      daysArr.forEach((d, j) => map[d] = valsArr[j]);
      return { ...ds, data: allDays.map(d => map[d] !== undefined ? map[d] : null) };
    });

    const isLog = type === 'epochs';

    chart = new Chart(document.getElementById('main-chart'), {
      type: 'line',
      data: { labels: allDays, datasets: alignedDatasets },
      options: {
        responsive: true, maintainAspectRatio: false,
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
            callbacks: {
              title: ctx => `Day ${ctx[0].label}`,
              filter: item => item.dataset.label !== '1x (halving price)',
              label: ctx => {
                const v = ctx.parsed.y;
                if (v == null) return ` ${ctx.dataset.label}  —`;
                return type === 'epochs'
                  ? ` ${ctx.dataset.label}  ${v.toFixed(2)}x`
                  : ` ${ctx.dataset.label}  ${v.toFixed(1)}`;
              }
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
              maxRotation: 0, maxTicksLimit: 10,
              callback: (val, idx) => allDays[idx] % 200 === 0 ? `d${allDays[idx]}` : ''
            }
          },
          y: {
            type: isLog ? 'logarithmic' : 'linear',
            grid: { display: false },
            border: { color: tc, width: 0.8 },
            ticks: {
              color: tc, font: { family: 'DM Mono, monospace', size: 11 },
              callback: v => isLog ? `${v}x` : `${v.toFixed(0)}`
            }
          }
        }
      }
    });

    // Perf row
    document.getElementById('perf-row').innerHTML = labels.map(label => {
      const vals = data[label].values.filter(v => v != null);
      const last = vals[vals.length - 1];
      const col  = COLORS[label] || '#888';
      const fmt  = type === 'epochs'
        ? `${last ? last.toFixed(2) + 'x' : '—'}`
        : `${last ? last.toFixed(1) : '—'}`;
      return `<div class="perf-item">
        <div class="perf-dot" style="background:${col}"></div>
        <span class="perf-sym">${label}</span>
        <span class="perf-val neu">${fmt}</span>
      </div>`;
    }).join('');

    document.getElementById('chart-title').textContent =
      type === 'epochs' ? 'BTC halving epochs' : 'BTC bear market cycles';

  } catch(e) {
    document.getElementById('chart-empty').style.display = 'flex';
    document.getElementById('empty-msg').textContent = `Error: ${e.message}`;
  } finally {
    document.getElementById('chart-spin').classList.remove('on');
  }
}

function selectTradFi(ticker) {
  document.querySelectorAll('.tradfi-btn').forEach(b => b.classList.toggle('active', b.dataset.asset === ticker));
  fetchCurrent();
}


