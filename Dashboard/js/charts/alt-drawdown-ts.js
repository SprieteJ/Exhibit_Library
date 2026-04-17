async function fetchAltDrawdownTS(from, to) {
  spinOn();
  try {
    const topN = parseInt(document.getElementById('rp-topn')?.value || '10');
    const data = await fetch(`/api/alt-drawdown-ts?from=${from}&to=${to}&topn=${topN}`, {signal: _navAbort?.signal}).then(r=>r.json());
    if (!data.series || !Object.keys(data.series).length) throw new Error('no data');

    const syms = Object.keys(data.series).slice(0, 10);
    const datasets = syms.map((sym, i) => {
      const s = data.series[sym];
      const dateMap = {};
      s.dates.forEach((d, j) => dateMap[d] = s.drawdowns[j]);
      return {
        label: sym,
        data: data.dates.map(d => dateMap[d] !== undefined ? dateMap[d] : null),
        borderColor: PAL[i % PAL.length],
        backgroundColor: 'transparent',
        borderWidth: 1.4,
        pointRadius: 0,
        tension: 0.1,
        spanGaps: true,
      };
    });

    mkChart('line', {
      labels: data.dates,
      datasets,
    }, {
      scales: {
        x: {
          type: 'category',
          ticks: { ...XTICK, maxRotation: 0, maxTicksLimit: 8,
            callback: function(val) { const l = this.getLabelForValue(val); return l ? l.slice(0,7) : ''; } },
          grid: XGRID,
        },
        y: {
          max: 0,
          ticks: { ...YTICK, callback: v => v + '%' },
          grid: YGRID,
        },
      },
      plugins: {
        legend: { display: true, labels: { color: '#888', font: { size: 11 }, boxWidth: 12 } },
        tooltip: {
          callbacks: {
            label: ctx => {
              const v = ctx.parsed.y;
              return v != null ? ` ${ctx.dataset.label}  ${v.toFixed(1)}%` : null;
            }
          }
        }
      },
    });
    setTitle(`Altcoin Drawdown from Running ATH (Top ${syms.length})`, 'Source: CoinGecko · running all-time high');
  } catch(e) { if (e.name === 'AbortError') return; showErr(e); }
}

// ── Alt: Correlation Heatmap ──────────────────────────────────────────────────
