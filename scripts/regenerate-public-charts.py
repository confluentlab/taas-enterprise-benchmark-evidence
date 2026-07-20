#!/usr/bin/env python3
from __future__ import annotations

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def svg_chart(title: str, labels: list[str], values: list[float], unit: str) -> str:
    width, height = 900, 420
    left, top, plot_w, plot_h = 80, 70, 760, 270
    maximum = max(values) * 1.08
    points = []
    for index, value in enumerate(values):
        x = left + (plot_w * index / max(1, len(values) - 1))
        y = top + plot_h - (value / maximum * plot_h)
        points.append((x, y, value, labels[index]))
    polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y, _, _ in points)
    marks = "".join(
        f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="#22d3ee"/>'
        f'<text x="{x:.1f}" y="{height-42}" text-anchor="middle" class="s">{label}</text>'
        f'<text x="{x:.1f}" y="{y-12:.1f}" text-anchor="middle" class="s">{value:.3f}</text>'
        for x, y, value, label in points
    )
    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img"><title>{title}</title><rect width="{width}" height="{height}" rx="20" fill="#07111f"/><style>.h{{font:700 24px system-ui;fill:#f8fafc}}.s{{font:13px system-ui;fill:#94a3b8}}</style><text x="40" y="43" class="h">{title}</text><text x="40" y="{height-15}" class="s">{unit}</text><path d="M{left} {top}V{top+plot_h}H{left+plot_w}" fill="none" stroke="#475569" stroke-width="2"/><polyline points="{polyline}" fill="none" stroke="#22d3ee" stroke-width="4"/>{marks}</svg>'''


with (ROOT / "evidence/payload-scaling/results.csv").open(newline="", encoding="utf-8") as handle:
    rows = list(csv.DictReader(handle))
(ROOT / "evidence/payload-scaling/charts/scaling.svg").write_text(
    svg_chart("Mean execution time by payload size", [row["size_mib"] + " MiB" for row in rows], [float(row["mean_ms_per_op"]) for row in rows], "Mean milliseconds per operation"), encoding="utf-8"
)

with (ROOT / "evidence/core-engine/results.csv").open(newline="", encoding="utf-8") as handle:
    core = list(csv.DictReader(handle))
(ROOT / "evidence/core-engine/charts/latency.svg").write_text(
    svg_chart("Core 1 MiB mean latency (provisional rejected)", [row["mode"].replace("_", " ") for row in core], [float(row["mean_us_per_op"]) for row in core], "Mean microseconds per operation"), encoding="utf-8"
)
print("Public charts regenerated.")
