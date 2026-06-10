#!/usr/bin/env python3
"""Generate inline SVG (for HTML) and PGF/TikZ (for LaTeX) figures from
   the coordinator-comparison summary CSV. No matplotlib dependency.

   Usage: python3 examples/coordinator_comparison_plot.py
"""

import csv
import os
import sys

DATA_DIR = "docs/arxiv/paper2a/data"
SUMMARY  = os.path.join(DATA_DIR, "summary.csv")
SVG_OUT  = os.path.join(DATA_DIR, "fig_decision_latency.svg")
TEX_OUT  = os.path.join(DATA_DIR, "fig_decision_latency.tex")

def load(path):
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            rows.append({
                "mode": r["mode"],
                "n": int(r["n"]),
                "mean_us": float(r["mean_us"]),
                "p50_us": int(r["p50_us"]),
                "p95_us": int(r["p95_us"]),
                "p99_us": int(r["p99_us"]),
                "mean_staleness": float(r["mean_staleness"]),
                "misroute_rate": float(r["misroute_rate"]),
            })
    return rows

def by_mode(rows, mode):
    return sorted([r for r in rows if r["mode"] == mode], key=lambda r: r["n"])

def svg_figure(rows):
    """Two side-by-side panels:
       (a) decision latency (log-scale) — mean + p99 for each mode vs N
       (b) mean staleness — for each mode vs N
    """
    g = by_mode(rows, "gossip")
    b = by_mode(rows, "broker")
    if not g or not b:
        return "<!-- insufficient data -->"

    Ns = sorted({r["n"] for r in rows})
    n_max = max(Ns)
    n_min = min(Ns)

    # Log-scale latency in µs
    lat_max = max(max(r["p99_us"] for r in g), max(r["p99_us"] for r in b))
    import math
    log_min = 0
    log_max = math.ceil(math.log10(max(lat_max, 10)))

    PANEL_W, PANEL_H = 360, 240
    MARGIN_L, MARGIN_R, MARGIN_T, MARGIN_B = 60, 20, 30, 40
    PLOT_W = PANEL_W - MARGIN_L - MARGIN_R
    PLOT_H = PANEL_H - MARGIN_T - MARGIN_B

    def x_pos(n, panel_x):
        return panel_x + MARGIN_L + (n - n_min) / max(n_max - n_min, 1) * PLOT_W
    def y_log(v):
        if v <= 0: v = 1
        l = math.log10(v)
        return MARGIN_T + PLOT_H - (l - log_min) / (log_max - log_min) * PLOT_H
    def y_lin(v, vmax):
        if vmax == 0: return MARGIN_T + PLOT_H
        return MARGIN_T + PLOT_H - (v / vmax) * PLOT_H

    parts = []
    parts.append(f'<svg xmlns="http://www.w3.org/2000/svg" '
                 f'viewBox="0 0 {PANEL_W * 2} {PANEL_H + 30}" '
                 f'style="background:#0d1117; font-family:sans-serif;">')
    parts.append('<style>'
                 'text{fill:#c9d1d9;font-size:11px}'
                 '.tick text{fill:#8b949e;font-size:10px}'
                 '.title{fill:#e6edf3;font-weight:bold;font-size:13px}'
                 '.legend{fill:#c9d1d9;font-size:11px}'
                 '.axis{stroke:#30363d;stroke-width:1}'
                 '.grid{stroke:#21262d;stroke-width:1}'
                 '.gossip{stroke:#3fb950;fill:#3fb950}'
                 '.broker{stroke:#ff7b72;fill:#ff7b72}'
                 '.gossip-p99{stroke:#3fb950;stroke-dasharray:3,3;fill:none}'
                 '.broker-p99{stroke:#ff7b72;stroke-dasharray:3,3;fill:none}'
                 '</style>')

    # ── Panel A: decision latency (log) ─────────────────────────────────────
    px = 0
    parts.append(f'<text class="title" x="{px + PANEL_W/2}" y="18" text-anchor="middle">'
                 '(a) Decision latency (log µs)</text>')

    # axes
    parts.append(f'<line class="axis" x1="{px+MARGIN_L}" y1="{MARGIN_T}" '
                 f'x2="{px+MARGIN_L}" y2="{MARGIN_T+PLOT_H}"/>')
    parts.append(f'<line class="axis" x1="{px+MARGIN_L}" y1="{MARGIN_T+PLOT_H}" '
                 f'x2="{px+MARGIN_L+PLOT_W}" y2="{MARGIN_T+PLOT_H}"/>')

    # y log ticks
    for power in range(log_min, log_max + 1):
        y = y_log(10 ** power)
        parts.append(f'<line class="grid" x1="{px+MARGIN_L}" y1="{y}" '
                     f'x2="{px+MARGIN_L+PLOT_W}" y2="{y}"/>')
        parts.append(f'<g class="tick"><text x="{px+MARGIN_L-6}" y="{y+3}" text-anchor="end">'
                     f'10^{power}</text></g>')

    # x ticks
    for n in Ns:
        x = x_pos(n, px)
        parts.append(f'<g class="tick"><text x="{x}" y="{MARGIN_T+PLOT_H+14}" text-anchor="middle">'
                     f'N={n}</text></g>')

    # Plot lines: gossip mean (solid), gossip p99 (dashed)
    for series, cls, key in [(g, "gossip", "mean_us"), (b, "broker", "mean_us")]:
        path = " ".join(f"L {x_pos(r['n'], px):.1f} {y_log(r[key]):.1f}" for r in series)
        path = "M" + path[1:]
        parts.append(f'<path class="{cls}" d="{path}" fill="none" stroke-width="2"/>')
        for r in series:
            parts.append(f'<circle class="{cls}" cx="{x_pos(r["n"], px):.1f}" '
                         f'cy="{y_log(r[key]):.1f}" r="3"/>')
    for series, cls, key in [(g, "gossip-p99", "p99_us"), (b, "broker-p99", "p99_us")]:
        path = " ".join(f"L {x_pos(r['n'], px):.1f} {y_log(r[key]):.1f}" for r in series)
        path = "M" + path[1:]
        parts.append(f'<path class="{cls}" d="{path}" stroke-width="1.5"/>')

    # ── Panel B: mean staleness ─────────────────────────────────────────────
    px = PANEL_W
    parts.append(f'<text class="title" x="{px + PANEL_W/2}" y="18" text-anchor="middle">'
                 '(b) Mean staleness (load-units)</text>')

    stale_max = max(max(r["mean_staleness"] for r in g), max(r["mean_staleness"] for r in b)) * 1.15
    stale_max = max(stale_max, 0.05)

    parts.append(f'<line class="axis" x1="{px+MARGIN_L}" y1="{MARGIN_T}" '
                 f'x2="{px+MARGIN_L}" y2="{MARGIN_T+PLOT_H}"/>')
    parts.append(f'<line class="axis" x1="{px+MARGIN_L}" y1="{MARGIN_T+PLOT_H}" '
                 f'x2="{px+MARGIN_L+PLOT_W}" y2="{MARGIN_T+PLOT_H}"/>')

    for i in range(5):
        v = stale_max * i / 4
        y = y_lin(v, stale_max)
        parts.append(f'<line class="grid" x1="{px+MARGIN_L}" y1="{y}" '
                     f'x2="{px+MARGIN_L+PLOT_W}" y2="{y}"/>')
        parts.append(f'<g class="tick"><text x="{px+MARGIN_L-6}" y="{y+3}" text-anchor="end">'
                     f'{v:.2f}</text></g>')

    for n in Ns:
        x = x_pos(n, px)
        parts.append(f'<g class="tick"><text x="{x}" y="{MARGIN_T+PLOT_H+14}" text-anchor="middle">'
                     f'N={n}</text></g>')

    for series, cls in [(g, "gossip"), (b, "broker")]:
        path = " ".join(f"L {x_pos(r['n'], px):.1f} {y_lin(r['mean_staleness'], stale_max):.1f}" for r in series)
        path = "M" + path[1:]
        parts.append(f'<path class="{cls}" d="{path}" fill="none" stroke-width="2"/>')
        for r in series:
            parts.append(f'<circle class="{cls}" cx="{x_pos(r["n"], px):.1f}" '
                         f'cy="{y_lin(r["mean_staleness"], stale_max):.1f}" r="3"/>')

    # ── Legend ──────────────────────────────────────────────────────────────
    legend_y = PANEL_H + 18
    parts.append(f'<line class="gossip" x1="40"  y1="{legend_y}" x2="60"  y2="{legend_y}" stroke-width="2"/>'
                 f'<text class="legend" x="65" y="{legend_y + 4}">gossip (locally-resolved) — mean</text>')
    parts.append(f'<line class="gossip-p99" x1="260" y1="{legend_y}" x2="280" y2="{legend_y}" stroke-width="1.5"/>'
                 f'<text class="legend" x="285" y="{legend_y + 4}">gossip p99</text>')
    parts.append(f'<line class="broker" x1="380" y1="{legend_y}" x2="400" y2="{legend_y}" stroke-width="2"/>'
                 f'<text class="legend" x="405" y="{legend_y + 4}">broker (coordinator) — mean</text>')
    parts.append(f'<line class="broker-p99" x1="580" y1="{legend_y}" x2="600" y2="{legend_y}" stroke-width="1.5"/>'
                 f'<text class="legend" x="605" y="{legend_y + 4}">broker p99</text>')

    parts.append('</svg>')
    return "".join(parts)

def pgf_figure(rows):
    """Compile-ready TikZ/pgfplots fragment for the LaTeX paper."""
    g = by_mode(rows, "gossip")
    b = by_mode(rows, "broker")

    def coords(series, key):
        return " ".join(f"({r['n']},{r[key]})" for r in series)

    return r"""\begin{figure}[t]
\centering
\begin{tikzpicture}
\begin{semilogyaxis}[
  width=0.46\textwidth, height=5.2cm,
  xlabel={Cluster size $N$ (workers)},
  ylabel={Decision latency ($\mu s$)},
  xtick={10,20,40},
  legend pos=north west,
  legend style={font=\scriptsize},
  ymin=10, ymax=1e6,
  grid=both, major grid style={gray!30}, minor grid style={gray!10},
  title={(a) Decision latency},
  title style={font=\small},
  name=plota,
]
\addplot[mark=*, thick, green!60!black] coordinates { """ + coords(g, "mean_us") + r""" };
\addlegendentry{gossip mean};
\addplot[mark=o, thick, green!60!black, dashed] coordinates { """ + coords(g, "p99_us") + r""" };
\addlegendentry{gossip p99};
\addplot[mark=square*, thick, red!70!black] coordinates { """ + coords(b, "mean_us") + r""" };
\addlegendentry{broker mean};
\addplot[mark=square, thick, red!70!black, dashed] coordinates { """ + coords(b, "p99_us") + r""" };
\addlegendentry{broker p99};
\end{semilogyaxis}
\begin{axis}[
  at={(plota.east)}, anchor=west, xshift=1cm,
  width=0.46\textwidth, height=5.2cm,
  xlabel={Cluster size $N$ (workers)},
  ylabel={Mean staleness (load-units)},
  xtick={10,20,40},
  legend pos=north west,
  legend style={font=\scriptsize},
  ymin=0,
  grid=both, major grid style={gray!30}, minor grid style={gray!10},
  title={(b) Mean staleness},
  title style={font=\small},
]
\addplot[mark=*, thick, green!60!black] coordinates { """ + coords(g, "mean_staleness") + r""" };
\addlegendentry{gossip};
\addplot[mark=square*, thick, red!70!black] coordinates { """ + coords(b, "mean_staleness") + r""" };
\addlegendentry{broker};
\end{axis}
\end{tikzpicture}
\caption{Decision latency (log scale, panel a) and routing-decision staleness
(panel b) for the locally-resolved gossip mode and the broker-mediated mode
on the same Mycelium substrate. Each point is the aggregate of one 20-second
run at 50 decisions/second. Gossip mean grows from 15.7 to 27.9 $\mu s$ over a
4$\times$ cluster scale; broker mean is two to three orders of magnitude
higher and p99 includes timeouts at the RPC ceiling.}
\label{fig:coordinator-latency}
\end{figure}
"""

def main():
    if not os.path.exists(SUMMARY):
        print(f"error: {SUMMARY} not found — run coordinator_comparison_runner.sh first",
              file=sys.stderr)
        sys.exit(1)
    rows = load(SUMMARY)
    svg = svg_figure(rows)
    pgf = pgf_figure(rows)
    with open(SVG_OUT, "w") as f: f.write(svg)
    with open(TEX_OUT, "w") as f: f.write(pgf)
    print(f"wrote {SVG_OUT}")
    print(f"wrote {TEX_OUT}")

if __name__ == "__main__":
    main()
