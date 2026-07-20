#!/usr/bin/env python3
from __future__ import annotations

import csv
import html
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BG, PANEL, GRID, TEXT, MUTED, CYAN, BLUE, AMBER = (
    "#07111f", "#101d30", "#334155", "#f8fafc", "#94a3b8", "#22d3ee", "#60a5fa", "#fbbf24"
)


def line_chart(title: str, labels: list[str], series: list[tuple[str, list[float], str]], unit: str, metadata: list[str]) -> str:
    width, height, left, top, plot_w, plot_h = 1100, 520, 88, 150, 930, 260
    maximum = max(value for _, values, _ in series for value in values) * 1.12
    elements: list[str] = []
    for name, values, color in series:
        points = []
        for index, value in enumerate(values):
            x = left + plot_w * index / max(1, len(values) - 1)
            y = top + plot_h - value / maximum * plot_h
            points.append((x, y, value))
        polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y, _ in points)
        elements.append(f'<polyline points="{polyline}" fill="none" stroke="{color}" stroke-width="4"/>')
        for x, y, value in points:
            elements.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{color}"/><text x="{x:.1f}" y="{y-11:.1f}" text-anchor="middle" class="small">{value:,.3f}</text>')
        elements.append(f'<text x="{left + len(elements) * 4}" y="126" class="legend" fill="{color}">{html.escape(name)}</text>')
    xlabels = "".join(f'<text x="{left + plot_w*i/max(1,len(labels)-1):.1f}" y="438" text-anchor="middle" class="small">{html.escape(label)}</text>' for i, label in enumerate(labels))
    meta = "".join(f'<text x="40" y="{72 + i*18}" class="meta">{html.escape(value)}</text>' for i, value in enumerate(metadata))
    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img"><title>{html.escape(title)}</title><rect width="{width}" height="{height}" rx="20" fill="{BG}"/><style>.title{{font:700 25px system-ui;fill:{TEXT}}}.meta,.small{{font:12px system-ui;fill:{MUTED}}}.legend{{font:600 13px system-ui}}</style><text x="40" y="40" class="title">{html.escape(title)}</text>{meta}<path d="M{left} {top}V{top+plot_h}H{left+plot_w}" fill="none" stroke="{GRID}" stroke-width="2"/>{''.join(elements)}{xlabels}<text x="40" y="470" class="meta">{html.escape(unit)}</text><text x="40" y="498" class="meta">Not a universal throughput guarantee.</text></svg>'''


def soak_chart(produced: int, output: int, failed: int, lag_start: int, lag_end: int) -> str:
    values = [produced, output, failed]
    labels = ["Inputs", "Outputs", "Intentional failures"]
    maximum = max(values)
    bars = []
    for index, (label, value) in enumerate(zip(labels, values)):
        x = 90 + index * 210
        height = max(3, 230 * value / maximum)
        y = 380 - height
        bars.append(f'<rect x="{x}" y="{y:.1f}" width="140" height="{height:.1f}" rx="8" fill="{CYAN}"/><text x="{x+70}" y="{y-12:.1f}" text-anchor="middle" class="small">{value:,}</text><text x="{x+70}" y="405" text-anchor="middle" class="small">{label}</text>')
    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="1100" height="520" viewBox="0 0 1100 520" role="img"><title>Kafka/Flink soak accounting and lag</title><rect width="1100" height="520" rx="20" fill="{BG}"/><style>.title{{font:700 25px system-ui;fill:{TEXT}}}.meta,.small{{font:12px system-ui;fill:{MUTED}}}.value{{font:700 30px system-ui;fill:{CYAN}}}</style><text x="40" y="40" class="title">Kafka/Flink soak accounting and final lag</text><text x="40" y="72" class="meta">Test date: 2026-07-11 · Host: local Docker · Workload: kafka-flink-100kb-600rps-30m</text><text x="40" y="92" class="meta">Status: LIVE_LOCAL_VERIFIED · Raw: evidence/kafka-soak/producer-results.json + consumer-results.json</text><path d="M70 150V380H710" fill="none" stroke="{GRID}" stroke-width="2"/>{''.join(bars)}<rect x="760" y="150" width="290" height="230" rx="16" fill="{PANEL}" stroke="{GRID}"/><text x="790" y="190" class="small">Raw-topic lag timeline</text><text x="810" y="250" class="value">{lag_start}</text><text x="810" y="280" class="small">start</text><path d="M860 245 H940" stroke="{BLUE}" stroke-width="4"/><text x="970" y="250" class="value">{lag_end}</text><text x="970" y="280" class="small">end</text><text x="790" y="330" class="small">Final accounting: {output:,} + {failed:,} = {produced:,}</text><text x="40" y="470" class="meta">Counts use separate scales for readability; failure bar is intentionally small.</text><text x="40" y="498" class="meta">Not a universal throughput guarantee.</text></svg>'''


with (ROOT / "evidence/payload-scaling/results.csv").open(newline="", encoding="utf-8") as handle:
    scaling = list(csv.DictReader(handle))
labels = [row["size_mib"] + " MiB" for row in scaling]
metadata = [
    "Test date: 2026-07-18 · Host: Windows 11 / Intel Core Ultra 9 285H / JDK 21 / G1",
    "Workload: core-1m-to-64m-976-fields-fixed-output · Status: MEASURED",
    "Raw evidence: evidence/payload-scaling/results.csv",
]
(ROOT / "evidence/payload-scaling/charts/mean-latency.svg").write_text(line_chart("Mean latency versus input size", labels, [("mean ms/op", [float(row["mean_ms_per_op"]) for row in scaling], CYAN)], "Mean milliseconds per operation", metadata), encoding="utf-8")
(ROOT / "evidence/payload-scaling/charts/allocation.svg").write_text(line_chart("Allocation versus input size", labels, [("bytes/op", [float(row["allocation_bytes_per_op"]) for row in scaling], BLUE)], "Allocated bytes per operation; grown field is unreferenced", metadata), encoding="utf-8")
tail = [row for row in scaling if row["p95_us"]]
(ROOT / "evidence/payload-scaling/charts/tail-latency.svg").write_text(line_chart("Representative p95 and p99 latency", [row["size_mib"] + " MiB" for row in tail], [("p95 µs", [float(row["p95_us"]) for row in tail], BLUE), ("p99 µs", [float(row["p99_us"]) for row in tail], AMBER)], "Microseconds at representative checkpoint sizes", metadata), encoding="utf-8")

producer = json.loads((ROOT / "evidence/kafka-soak/producer-results.json").read_text())
consumer = json.loads((ROOT / "evidence/kafka-soak/consumer-results.json").read_text())
(ROOT / "evidence/kafka-soak/charts/accounting-and-lag.svg").write_text(soak_chart(producer["recordsProducedToRaw"], consumer["recordsProducedToOutput"], consumer["recordsFailed"], consumer["rawConsumerLagStart"], consumer["rawConsumerLagEnd"]), encoding="utf-8")

with (ROOT / "evidence/core-engine/results.csv").open(newline="", encoding="utf-8") as handle:
    core = list(csv.DictReader(handle))
(ROOT / "evidence/core-engine/charts/latency.svg").write_text(line_chart("Core 1 MiB mean latency", [row["mode"].replace("_", " ") for row in core], [("mean µs/op", [float(row["mean_us_per_op"]) for row in core], CYAN)], "Status: MEASURED_NOT_QUALIFIED · Raw: evidence/core-engine/results.csv · Not a universal throughput guarantee.", ["Test date: 2026-07-17 · Host: Windows 11 / Intel Core Ultra 9 285H / JDK 21 / G1", "Workload: core-1m-976-fields · Repeatability: 9/12 checks met", "Raw evidence: evidence/core-engine/results.csv"]), encoding="utf-8")
print("Four publication charts and the core chart regenerated.")
