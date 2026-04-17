async function fetchRRG() {
  if (chart) { chart.destroy(); chart = null; }
  document.getElementById('chart-empty').style.display = 'none';
  document.getElementById('chart-spin').classList.add('on');

  try {
    const to      = document.getElementById('rp-rrg-to')?.value  || new Date().toISOString().split('T')[0];
    const rsWin   = document.getElementById('rp-rrg-rs')?.value  || '10';
    const momWin  = document.getElementById('rp-rrg-mom')?.value || '6';
    const tailOn  = document.getElementById('rp-rrg-tail')?.checked;
    const tailLen = document.getElementById('rp-rrg-tail-len')?.value || '6';

    const url = `/api/sector-rrg?to=${to}&window=${rsWin}&momentum=${momWin}&tail=${tailOn ? tailLen : 0}&granularity=${rrgGran}&benchmark=${rrgBenchmark}`;
    const res  = await fetch(url);
    const data = await res.json();
    if (data.error) throw new Error(data.error);

    const sectors = Object.keys(data).filter(k => data[k].x != null && data[k].y != null);
    if (!sectors.length) throw new Error('no data');

    const tc = darkMode ? '#E8EAE8' : '#606663';

    // Axis range centered on 100
    const allX = sectors.map(s => data[s].x);
    const allY = sectors.map(s => data[s].y);
    const xPad = Math.max(3, ...allX.map(v => Math.abs(v - 100))) * 1.4;
    const yPad = Math.max(3, ...allY.map(v => Math.abs(v - 100))) * 1.4;

    // Datasets: tails first (behind), then invisible anchor points for tooltip hit areas
    const datasets = [];

    if (tailOn) {
      sectors.forEach(sector => {
        const d = data[sector];
        if (!d.tail?.length) return;
        const pts   = [...d.tail, {x: d.x, y: d.y}];
        const color = d.color || '#888';
        const r = parseInt(color.slice(1,3),16), g = parseInt(color.slice(3,5),16), b = parseInt(color.slice(5,7),16);
        datasets.push({
          label: `${sector}__tail`,
          data:  pts,
          showLine: true,
          tension: 0.3,
          borderColor: `rgba(${r},${g},${b},0.3)`,
          borderWidth: 1.5,
          backgroundColor: 'transparent',
          pointRadius: pts.map((_, i) => i === pts.length - 1 ? 0 : 2),
          pointBackgroundColor: pts.map((_, i) => `rgba(${r},${g},${b},${0.12 + (i / pts.length) * 0.45})`),
          pointBorderWidth: 0,
          order: 2,
        });
      });
    }

    // Invisible anchor points — used only for tooltip hit detection
    sectors.forEach(sector => {
      const d = data[sector];
      datasets.push({
        label: sector,
        data:  [{x: d.x, y: d.y}],
        backgroundColor: 'transparent',
        borderColor:     'transparent',
        pointRadius: 18,          // large invisible hit area
        pointHoverRadius: 18,
        order: 1,
      });
    });

    // Crosshair + quadrant label plugin (no fills)
    const quadrantPlugin = {
      id: 'rrg-quadrant',
      beforeDraw(ch) {
        const {ctx, chartArea: {left, right, top, bottom}, scales} = ch;
        const cx = scales.x.getPixelForValue(100);
        const cy = scales.y.getPixelForValue(100);
        ctx.save();
        ctx.strokeStyle = darkMode ? 'rgba(255,255,255,0.15)' : 'rgba(0,0,0,0.12)';
        ctx.lineWidth = 1; ctx.setLineDash([4,4]);
        ctx.beginPath(); ctx.moveTo(cx,top);  ctx.lineTo(cx,bottom); ctx.stroke();
        ctx.beginPath(); ctx.moveTo(left,cy); ctx.lineTo(right,cy);  ctx.stroke();
        ctx.setLineDash([]);
        ctx.font = '10px DM Mono, monospace';
        ctx.fillStyle = darkMode ? 'rgba(255,255,255,0.2)' : 'rgba(0,0,0,0.2)';
        ctx.textBaseline = 'top';
        ctx.textAlign = 'right'; ctx.fillText('IMPROVING', cx-8, top+8);
        ctx.textAlign = 'left';  ctx.fillText('LEADING',   cx+8, top+8);
        ctx.textBaseline = 'bottom';
        ctx.textAlign = 'left';  ctx.fillText('WEAKENING', cx+8, bottom-8);
        ctx.textAlign = 'right'; ctx.fillText('LAGGING',   cx-8, bottom-8);
        ctx.restore();
      },
      // Draw sector names directly on canvas (no dots)
      afterDraw(ch) {
        const {ctx} = ch;
        ctx.save();
        ch.data.datasets.forEach((ds, i) => {
          if (ds.label?.endsWith('__tail')) return;
          const meta = ch.getDatasetMeta(i);
          if (!meta.visible) return;
          const sector = ds.label;
          const d = data[sector];
          if (!d) return;
          const color = d.color || '#888';
          meta.data.forEach(pt => {
            ctx.font = '600 12px DM Sans, sans-serif';
            ctx.fillStyle = color;
            ctx.textAlign = 'center';
            ctx.textBaseline = 'middle';
            ctx.fillText(sector, pt.x, pt.y);
          });
        });
        ctx.restore();
      }
    };

    chart = new Chart(document.getElementById('main-chart'), {
      type: 'scatter',
      data: { datasets },
      options: {
        responsive: true, maintainAspectRatio: false,
        animation: { duration: 400 },
        interaction: { mode: 'nearest', intersect: false, axis: 'xy' },
        plugins: {
          legend: { display: false },
          tooltip: {
            filter: ctx => !ctx.dataset.label?.endsWith('__tail'),
            backgroundColor: darkMode ? '#1A1D1B' : '#fff',
            borderColor: darkMode ? '#2A2D2B' : '#E4E4E2',
            borderWidth: 1, padding: 12,
            titleColor: darkMode ? '#E8EAE8' : '#323935',
            bodyColor:  darkMode ? '#888B88' : '#606663',
            callbacks: {
              title: ctx => ctx[0]?.dataset.label,
              label: ctx => {
                const d = data[ctx.dataset.label];
                if (!d) return [];
                return [
                  ` RS-Ratio:  ${d.x.toFixed(2)}`,
                  ` RS-Mom:    ${d.y.toFixed(2)}`,
                  ` Quadrant:  ${d.quadrant}`,
                ];
              }
            }
          }
        },
        scales: {
          x: {
            min: 100 - xPad, max: 100 + xPad,
            title: { display: true, text: `← Weak    RS vs ${rrgBenchmark === 'market' ? 'market' : rrgBenchmark.toUpperCase()}    Strong →`, color: tc, font: { family: 'DM Mono, monospace', size: 11 } },
            grid: { display: false },
            border: { color: tc, width: 0.8 },
            ticks: { color: tc, font: { family: 'DM Mono, monospace', size: 11 }, callback: v => v.toFixed(1) }
          },
          y: {
            min: 100 - yPad, max: 100 + yPad,
            title: { display: true, text: '↑ Accelerating    RS Momentum    Decelerating ↓', color: tc, font: { family: 'DM Mono, monospace', size: 11 } },
            grid: { display: false },
            border: { color: tc, width: 0.8 },
            ticks: { color: tc, font: { family: 'DM Mono, monospace', size: 11 }, callback: v => v.toFixed(1) }
          }
        }
      },
      plugins: [quadrantPlugin]
    });

    const qColors = { Leading: '#00D64A', Improving: '#F7931A', Lagging: '#DC3545', Weakening: '#2471CC' };
    document.getElementById('perf-row').innerHTML = sectors.map(s => {
      const d = data[s];
      return `<div class="perf-item">
        <div class="perf-dot" style="background:${d.color}"></div>
        <span class="perf-sym">${s}</span>
        <span class="perf-val" style="color:${qColors[d.quadrant]||'#888'}">${d.quadrant}</span>
      </div>`;
    }).join('');

    document.getElementById('chart-title').textContent = 'Relative Rotation Graph';

    // Methodology info block
    const bmLabel = rrgBenchmark === 'market' ? 'equal-weighted market average' : rrgBenchmark.toUpperCase();
    const infoEl  = document.getElementById('chart-info');
    infoEl.style.display = 'block';
    infoEl.innerHTML = `
      <b>How it's calculated</b><br>
      1. <b>RS ratio</b> — each sector's equal-weighted price index is divided by the ${bmLabel} index to get its relative strength (RS).<br>
      2. <b>RS-Ratio (x-axis)</b> — measures whether the sector is currently outperforming: <code>100 + ((RS_now / RS_${rsWin}${rrgGran === 'weekly' ? ' weeks' : ' days'}_ago) − 1) × 100</code>. Above 100 = outperforming.<br>
      3. <b>RS-Momentum (y-axis)</b> — measures whether that outperformance is accelerating: <code>100 + ((RS-Ratio_now / RS-Ratio_${momWin}${rrgGran === 'weekly' ? ' weeks' : ' days'}_ago) − 1) × 100</code>. Above 100 = accelerating.<br>
      4. <b>Quadrants</b> — Leading (strong & accelerating) → Weakening (strong but slowing) → Lagging (weak & decelerating) → Improving (weak but accelerating). Sectors rotate clockwise over time.<br>
      ${tailOn ? `5. <b>Trail</b> — the fading line behind each sector shows its last ${tailLen} ${rrgGran === 'weekly' ? 'weekly' : 'daily'} positions, giving a sense of direction and speed of rotation.<br>` : ''}
      <span style="opacity:0.6">Lookback: RS = ${rsWin} ${rrgGran} periods · Momentum = ${momWin} ${rrgGran} periods · Benchmark = ${bmLabel} · As of ${to}</span>`;

  } catch(e) {
    document.getElementById('chart-empty').style.display = 'flex';
    document.getElementById('empty-msg').textContent = `Error: ${e.message}`;
  } finally {
    document.getElementById('chart-spin').classList.remove('on');
  }
}

