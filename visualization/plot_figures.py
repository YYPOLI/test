# coding=utf-8
"""
Paper figure generation scripts for PermitGuard.
Each function produces one figure for the paper.

Usage:
    python visualization/plot_figures.py [function_name]

Available figures:
    fig2_permit_trend          - Figure 2: The permit transaction trend from 2023 to 2024
    fig3_cross_stage_semantics - Figure 3: Analysis of cross-stage semantics
    fig5_report_quality        - Figure 5: Cross-model consensus evaluation of forensic report quality
    fig6_efficiency_cost       - Figure 6: Efficiency and economic cost with different LLMs
"""

import os
import sys
import re
import pathlib

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mtick
import matplotlib
import pandas as pd
import numpy as np
import seaborn as sns

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.utils.config import CONFIG

BASE_PATH = CONFIG["BASE_PATH"]
ANALYSIS_DIR = os.path.join(BASE_PATH, "analysis_stats")
FIGURE_DIR = CONFIG["PATHS"]["FIGURE_DIR"]
os.makedirs(FIGURE_DIR, exist_ok=True)


def _setup_academic_style():
    plt.rcParams.update({
        'font.family': 'serif',
        'font.serif': ['Times New Roman'],
        'font.size': 14,
        'axes.labelsize': 16,
        'axes.titlesize': 12,
        'xtick.labelsize': 14,
        'ytick.labelsize': 14,
        'legend.fontsize': 14,
        'axes.linewidth': 1.5,
    })


def fig2_permit_trend():
    """Figure 2: The permit transaction trend from 2023 to 2024."""
    _setup_academic_style()

    csv_path = os.path.join(ANALYSIS_DIR, 'monthly_permit_deep_analysis.csv')
    df = pd.read_csv(csv_path)
    df['Period'] = pd.to_datetime(df['Period'])
    df = df[(df['Period'] >= '2023-01-01') & (df['Period'] <= '2024-09-30')]
    df['Period_Str'] = df['Period'].dt.strftime('%Y-%m')

    periods = df['Period_Str'].tolist()
    total_tx = df['Total_Tx'].tolist()
    phishing_tx = df['Phishing_Tx_Count'].tolist()
    phishing_pct = [pct * 100 for pct in df['Phishing_Tx_Pct'].tolist()]
    normal_tx = [t - p for t, p in zip(total_tx, phishing_tx)]

    fig, ax1 = plt.subplots(figsize=(9, 4.5))
    bar_width = 0.6
    ax1.bar(periods, normal_tx, label='Legitimate Tx', color='#C2D6ED', width=bar_width, edgecolor='grey', linewidth=0.5)
    ax1.bar(periods, phishing_tx, label='Phishing Tx', color='#8B0000', width=bar_width, edgecolor='black', linewidth=0.5)
    ax1.set_ylabel('Permit Transaction Count', labelpad=10, fontweight='bold')
    ax1.tick_params(axis='x', rotation=20)
    ax1.set_ylim(0, 120000)

    ax2 = ax1.twinx()
    ax2.plot(periods, phishing_pct, color='#FF4500', marker='o', linestyle='-', linewidth=2, markersize=6, label='Phishing Ratio (%)')
    ax2.set_ylabel('Phishing Ratio', labelpad=10, fontweight='bold')
    ax2.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=1))
    ax2.set_ylim(0, 10)

    bars_handles, bars_labels = ax1.get_legend_handles_labels()
    lines_handles, lines_labels = ax2.get_legend_handles_labels()
    ax1.legend(bars_handles[::-1] + lines_handles, bars_labels[::-1] + lines_labels,
               loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=3, frameon=False)

    ax1.grid(axis='y', linestyle='--', alpha=0.4)
    ax1.set_axisbelow(True)

    odd_ticks, odd_labels = [], []
    for i, period in enumerate(periods):
        month = int(period.split('-')[1])
        if month % 2 != 0:
            odd_ticks.append(i)
            odd_labels.append(period)
    ax1.set_xticks(odd_ticks)
    ax1.set_xticklabels(odd_labels)

    max_pct_idx = phishing_pct.index(max(phishing_pct))
    ax2.annotate(f'Peak: {max(phishing_pct):.2f}%',
                 xy=(max_pct_idx, max(phishing_pct)),
                 xytext=(max_pct_idx - 1.5, max(phishing_pct) + 1),
                 arrowprops=dict(facecolor='black', arrowstyle='->'),
                 fontsize=14, fontweight='bold')

    plt.tight_layout()
    plt.savefig(os.path.join(FIGURE_DIR, 'permit_landscape_trend.pdf'), dpi=300, bbox_inches='tight')
    plt.show()



def fig3_cross_stage_semantics():
    """Figure 3: Analysis of cross-stage semantics (dumbbell chart)."""
    matplotlib.rcParams['font.family'] = 'Times New Roman'
    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    matplotlib.rcParams['axes.unicode_minus'] = False

    features = [
        'Self-Submit\n& Third-Party Transfer', 'Infinite Allowance\n& Ghost Spender',
        'Infinite Time\n& New Contract', 'Delegated-Submit\n& Third-Party Transfer',
        'Infinite Allowance\n& Zero Utilization',
    ]
    benign = [15.8, 3.96, 0.26, 6.54, 16.95]
    malicious = [2.08, 11.26, 15.4, 71.03, 79.98]

    y = np.arange(len(features))
    band_h = 0.28
    fig, ax = plt.subplots(figsize=(7.0, 3.8), dpi=300)

    color_benign = '#2B6DA1'
    color_malicious = '#B82020'
    bg_stripe = '#F6F6F6'

    for i in range(len(features)):
        if i % 2 == 0:
            ax.axhspan(i - 0.5, i + 0.5, color=bg_stripe, zorder=0)

    for i, (b, m) in enumerate(zip(benign, malicious)):
        lo, hi = min(b, m), max(b, m)
        fc = color_malicious if m > b else color_benign
        ax.fill_between([lo, hi], y[i] - band_h, y[i] + band_h, color=fc, alpha=0.12, zorder=1)
        ax.plot([lo, hi], [y[i], y[i]], color=fc, linewidth=2.0, alpha=0.35, zorder=2, solid_capstyle='round')

    ax.scatter(benign, y, color=color_benign, s=90, zorder=4, edgecolors='white', linewidth=1.2, label='Legitimate')
    ax.scatter(malicious, y, color=color_malicious, s=90, zorder=4, edgecolors='white', linewidth=1.2, marker='D', label='Phishing')

    for i, (b, m) in enumerate(zip(benign, malicious)):
        ratio = m / b if (m > b and b > 0) else (b / m if m > 0 else float('inf'))
        ratio_str = f'($\\times${ratio:.0f})' if ratio >= 10 else f'($\\times${ratio:.1f})'
        lo, hi = (b, m) if b < m else (m, b)
        lo_is_benign = (b < m)
        lo_color = color_benign if lo_is_benign else color_malicious
        hi_color = color_malicious if lo_is_benign else color_benign
        lo_x, hi_x = lo, hi
        if (hi - lo) < 10:
            lo_x = lo - 2
            hi_x = hi + 4
        elif lo < 1:
            lo_x = lo + 2
        ax.text(lo_x, i - 0.3, f'{lo}%', ha='center', va='top', fontsize=9.5, fontweight='bold', color=lo_color)
        ax.text(hi_x, i - 0.3, f'{hi}%  {ratio_str}', ha='center', va='top', fontsize=9.5, fontweight='bold', color=hi_color)

    ax.set_yticks(y)
    ax.set_yticklabels(features, fontsize=10.5)
    ax.set_xlabel('Proportion of Samples (%)', fontsize=12, labelpad=8)
    ax.set_xticks(np.arange(0, 81, 10))
    ax.set_xticklabels([f'{v}%' for v in range(0, 81, 10)], fontsize=10)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.set_xlim(-2, 90)
    ax.set_ylim(-0.55, len(features) - 0.45)
    ax.grid(axis='x', linestyle='--', linewidth=0.4, alpha=0.5, color='#BBBBBB', zorder=0)

    legend = ax.legend(loc='lower right', frameon=True, fontsize=10.5, edgecolor='#BBBBBB',
                       fancybox=False, framealpha=0.95, borderpad=0.5, handlelength=1.0)
    legend.get_frame().set_linewidth(0.5)

    plt.tight_layout(pad=0.5)
    plt.savefig(os.path.join(FIGURE_DIR, 'combined_features_dumbbell.pdf'), bbox_inches='tight', dpi=300, pad_inches=0.05)
    plt.savefig(os.path.join(FIGURE_DIR, 'combined_features_dumbbell.png'), bbox_inches='tight', dpi=300, pad_inches=0.05)
    plt.show()


def fig5_report_quality():
    """Figure 5: Cross-model consensus evaluation of forensic report quality."""
    _setup_academic_style()

    raw_data = {
        'Gemini-3-Pro': {
            'Factual\nConsistency': [5, 5, 5, 5, 5, 5, 5, 5, 3, 5],
            'Logical\nCoherence': [5] * 10,
            'Forensic\nActionability': [5] * 10,
        },
        'Claude-Opus-4.5': {
            'Factual\nConsistency': [5, 5, 5, 5, 5, 5, 5, 5, 4, 5],
            'Logical\nCoherence': [5, 5, 5, 5, 5, 5, 5, 5, 4, 5],
            'Forensic\nActionability': [5, 5, 5, 5, 5, 5, 5, 5, 4, 5],
        },
        'GPT-5.4-High': {
            'Factual\nConsistency': [5, 5, 4, 5, 5, 4, 5, 5, 3, 5],
            'Logical\nCoherence': [5, 5, 5, 5, 5, 4, 5, 5, 4, 5],
            'Forensic\nActionability': [5, 5, 5, 5, 5, 5, 5, 5, 4, 5],
        },
    }

    records = []
    for model, dimensions in raw_data.items():
        for dim, scores in dimensions.items():
            for score in scores:
                records.append({'Model': model, 'Dimension': dim, 'Score': score})
    df = pd.DataFrame(records)

    fig, ax = plt.subplots(figsize=(4.0, 2.5), dpi=300)
    colors = ['#B7CCDA', '#F1CFBD', '#C6DEC8']

    sns.barplot(data=df, x='Dimension', y='Score', hue='Model', palette=colors,
                errorbar='sd', capsize=0.1, errcolor='black', errwidth=1.0,
                linewidth=1.0, edgecolor='black', ax=ax)

    ax.set_ylim(1, 5.5)
    ax.set_yticks([1.0, 2.0, 3.0, 4.0, 5.0])
    ax.set_xlabel('')
    ax.set_ylabel('Judge Score (1-5)', fontsize=9)
    ax.tick_params(axis='x', labelsize=9)
    ax.tick_params(axis='y', labelsize=9)
    sns.despine(top=True, right=True)

    plt.legend(title='', loc='upper center', bbox_to_anchor=(0.5, 1.25), ncol=3,
               frameon=False, fontsize=9, columnspacing=0.8, handletextpad=0.3)

    plt.tight_layout()
    plt.savefig(os.path.join(FIGURE_DIR, 'RQ3_Boxplot_Evaluation.pdf'), bbox_inches='tight')
    plt.show()


def fig6_efficiency_cost():
    """Figure 6: Efficiency and economic cost with different LLMs."""
    import json
    import matplotlib.gridspec as gridspec
    import matplotlib.ticker as mticker
    import matplotlib.colors as mcolors
    from matplotlib.lines import Line2D

    matplotlib.rcParams.update({
        'font.family': 'Times New Roman',
        'mathtext.fontset': 'stix',
        'axes.unicode_minus': False,
    })

    SCALE = 2.7
    COLOR_BLUE = '#2B6DA1'
    COLOR_RED = '#B82020'
    MODELS = ["Gemini-3-Flash", "GPT-5.4", "Claude-4.5-Sonnet", "Qwen3-Max", "DeepSeek-V3.2"]

    data_path = os.path.join(BASE_PATH, 'calc_results.json')
    with open(data_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    cost_order = sorted(MODELS, key=lambda m: data[m]["cost_per_report"]["mean"], reverse=True)
    y_pos = np.arange(len(cost_order))

    costs = [data[m]["cost_per_report"]["mean"] for m in cost_order]
    cost_min, cost_max = min(costs), max(costs)
    cmap = mcolors.LinearSegmentedColormap.from_list('unified', [COLOR_BLUE, COLOR_RED])
    cnorm = mcolors.LogNorm(vmin=cost_min, vmax=cost_max)
    model_colors = {m: cmap(cnorm(data[m]["cost_per_report"]["mean"])) for m in MODELS}

    def darken(rgba, factor=0.55):
        return (rgba[0] * factor, rgba[1] * factor, rgba[2] * factor, 1.0)

    # --- Layout ---
    fig = plt.figure(figsize=(14, 9.5))
    gs = gridspec.GridSpec(2, 1, height_ratios=[1.5, 1],
                           hspace=0.7, left=0.20, right=0.97, top=0.90, bottom=0.16)
    gs_top = gridspec.GridSpecFromSubplotSpec(
        1, 4, subplot_spec=gs[0, 0], wspace=0.04, width_ratios=[1, 1, 1, 1])

    # --- (a) Four-zone Range Dot Chart ---
    zones = [
        ("prompt_tokens", "Input Tokens"),
        ("completion_tokens", "Output Tokens"),
        ("total_tokens", "Total Tokens"),
        ("reasoning_time", "Reasoning Time (s)"),
    ]
    x_cap_map = {"reasoning_time": 40}
    xlim_map = {
        "prompt_tokens": (3200, 4100, [3200, 3450, 3700, 3950]),
        "completion_tokens": (0, 1200, [0, 300, 600, 900]),
        "total_tokens": (3200, 5000, [3200, 3700, 4200, 4700]),
        "reasoning_time": (0, 40, [0, 10, 20, 30]),
    }
    outlier_idx = 0

    for z_idx, (metric_key, zone_title) in enumerate(zones):
        ax = fig.add_subplot(gs_top[0, z_idx])
        ax.set_facecolor('#FAFAFA')
        x_cap = x_cap_map.get(metric_key)

        for i, m in enumerate(cost_order):
            s = data[m][metric_key]
            c = model_colors[m]
            edge = darken(c, 0.50)
            vmin, vmed, vmean, vmax = s["min"], s["median"], s["mean"], s["max"]
            vmax_disp = min(vmax, x_cap) if x_cap else vmax

            ax.plot([vmin, vmax_disp], [y_pos[i]] * 2, '-', color=c,
                    linewidth=8.0 * SCALE / 1.6, alpha=0.30, solid_capstyle='round')
            ax.plot(vmin, y_pos[i], '|', color=edge, markersize=11 * SCALE / 1.6, markeredgewidth=2.2)
            ax.plot(vmax_disp, y_pos[i], '|', color=edge, markersize=11 * SCALE / 1.6, markeredgewidth=2.2)
            ax.scatter(vmed, y_pos[i], marker='o', c=[c], s=100 * SCALE / 1.6,
                       edgecolors='black', linewidths=1.1, zorder=6)
            ax.scatter(vmean, y_pos[i], marker='D', facecolors='white',
                       edgecolors=[edge], s=65 * SCALE / 1.6, linewidths=1.7, zorder=6)

            if x_cap and vmax > x_cap:
                y_off = 0.28 if outlier_idx % 2 == 0 else -0.28
                ax.annotate(f'{vmax:.0f}', xy=(vmax_disp, y_pos[i]),
                            xytext=(x_cap * 0.88, y_pos[i] + y_off),
                            fontsize=8 * SCALE, color=edge, fontweight='bold', va='center',
                            arrowprops=dict(arrowstyle='->', color=edge, lw=0.8,
                                            connectionstyle='arc3,rad=0.15'))
                outlier_idx += 1

        ax.set_yticks(y_pos)
        if z_idx == 0:
            ax.set_yticklabels(cost_order, fontsize=9 * SCALE, color='black')
        else:
            ax.set_yticklabels([])
            ax.tick_params(axis='y', length=0)
        ax.invert_yaxis()
        ax.set_title(zone_title, y=-0.25, fontsize=9 * SCALE, pad=8)
        ax.grid(axis='both', alpha=0.18, linestyle='--', linewidth=0.5)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        if z_idx > 0:
            ax.spines['left'].set_linewidth(0.8)
            ax.spines['left'].set_color('#AAAAAA')

        if metric_key in xlim_map:
            lo, hi, ticks = xlim_map[metric_key]
            ax.set_xlim(lo, hi)
            ax.set_xticks(ticks)
        ax.tick_params(axis='x', labelsize=8 * SCALE)

    # --- (b) Cost Bar Chart ---
    ax_c = fig.add_subplot(gs[1, 0])
    bh = 0.70
    totals = [data[m]["cost_per_report"]["mean"] for m in cost_order]
    max_total = max(totals)

    for i, (m, total) in enumerate(zip(cost_order, totals)):
        c = model_colors[m]
        ax_c.barh(y_pos[i], total, height=bh, color=c, alpha=0.88,
                  edgecolor='white', linewidth=0.6)
        ax_c.text(total + max_total * 0.015, y_pos[i],
                  f'{total * 1000:.2f}', ha='left', va='center',
                  fontsize=8 * SCALE, color='black')

    ax_c.set_xlim(0, 0.025)
    ax_c.tick_params(axis='x', labelsize=8 * SCALE)
    ax_c.set_yticks(y_pos)
    ax_c.set_yticklabels(cost_order, fontsize=9 * SCALE, color='black')
    ax_c.invert_yaxis()
    ax_c.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x * 1000:.1f}'))
    ax_c.set_xlabel('Cost of 1000 Reports (USD)', fontsize=9 * SCALE, labelpad=10)
    ax_c.spines['top'].set_visible(False)
    ax_c.spines['right'].set_visible(False)
    ax_c.grid(axis='x', alpha=0.2, linestyle='--', linewidth=0.5)

    # --- Global legend & subtitles ---
    center_x = 0.515
    legend_handles = [
        Line2D([0], [0], marker='|', color='#555', markersize=10, markeredgewidth=2.0,
               linestyle='-', linewidth=6.0, alpha=0.30, label='Min \u2013 Max range'),
        Line2D([0], [0], marker='o', color='#555', markerfacecolor='#555', markersize=10,
               linestyle='None', markeredgecolor='black', markeredgewidth=1.0, label='Median'),
        Line2D([0], [0], marker='D', color='#555', markerfacecolor='white', markersize=9.5,
               linestyle='None', markeredgecolor='#555', markeredgewidth=1.5, label='Mean'),
    ]
    fig.legend(handles=legend_handles, fontsize=9 * SCALE, loc='upper center',
               bbox_to_anchor=(center_x, 1), ncol=3, framealpha=0.95,
               edgecolor='#999', fancybox=True, handlelength=2.0, columnspacing=1.8)
    fig.text(center_x, 0.445, "(a) Token Consumption and Reasoning Time",
             ha='center', va='center', fontsize=11 * SCALE)
    fig.text(center_x, 0.025, "(b) Cost of Generating Forensic Reports",
             ha='center', va='center', fontsize=11 * SCALE)

    plt.savefig(os.path.join(FIGURE_DIR, 'fig6_efficiency_cost.pdf'),
                facecolor='white', bbox_inches='tight', dpi=300)
    plt.show()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Generate paper figures")
    parser.add_argument("func", nargs="?", default="fig2_permit_trend",
                        choices=["fig2_permit_trend",
                                 "fig3_cross_stage_semantics",
                                 "fig5_report_quality",
                                 "fig6_efficiency_cost"])
    args = parser.parse_args()
    globals()[args.func]()
