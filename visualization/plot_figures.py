# coding=utf-8
"""
Paper figure generation scripts for PermitGuard.
Each function produces one figure for the paper.

Usage:
    python visualization/plot_figures.py [function_name]

Example:
    python visualization/plot_figures.py monthly_permits
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
ANALYSIS_DIR = os.path.join(BASE_PATH, "BigQuery_since20251230/analysis/")
FIGURE_DIR = os.path.join(os.path.dirname(__file__), "figures")
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


def monthly_permits():
    """Fig: Monthly permit transaction trend with phishing ratio overlay."""
    _setup_academic_style()

    csv_path = os.path.join(ANALYSIS_DIR, 'monthly_permit_deep_analysis_0209_2300.csv')
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
    ax1.bar(periods, normal_tx, label='Benign Txs', color='#C2D6ED', width=bar_width, edgecolor='grey', linewidth=0.5)
    ax1.bar(periods, phishing_tx, label='Phishing Txs', color='#8B0000', width=bar_width, edgecolor='black', linewidth=0.5)
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


def plot_global_atomicity():
    """Fig: Monthly atomicity trend comparison between benign and phishing."""
    _setup_academic_style()

    csv_path = os.path.join(ANALYSIS_DIR, 'monthly_atomic_stats.csv')
    df = pd.read_csv(csv_path)
    df['Period'] = pd.to_datetime(df['Period'])

    norm_atomic = df['Norm_Atomic_Pct'] * 100
    norm_delayed = df['Norm_Delayed_Pct'] * 100
    phish_atomic = df['Phish_Atomic_Pct'] * 100
    phish_delayed = df['Phish_Delayed_Pct'] * 100

    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(12, 10), gridspec_kw={'hspace': 0.15})
    width = 20
    color_atomic = '#2c3e50'
    color_delayed = '#e67e22'

    ax1.bar(df['Period'], norm_atomic, width=width, label='Atomic (Same Tx)', color=color_atomic, edgecolor='black', linewidth=0.5)
    ax1.bar(df['Period'], norm_delayed, width=width, bottom=norm_atomic, label='Delayed (Separate Tx)', color=color_delayed, edgecolor='black', linewidth=0.5)
    ax1.set_ylabel('Percentage (%)', fontweight='bold')
    ax1.set_title('(a) Normal Users: Atomicity Trend', loc='left', fontweight='bold', pad=10)
    ax1.set_ylim(0, 100)
    ax1.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax1.legend(loc='lower center', bbox_to_anchor=(0.5, 1.1), ncol=2, frameon=False)

    ax2.bar(df['Period'], phish_atomic, width=width, color=color_atomic, edgecolor='black', linewidth=0.5)
    ax2.bar(df['Period'], phish_delayed, width=width, bottom=phish_atomic, color=color_delayed, edgecolor='black', linewidth=0.5)
    ax2.set_ylabel('Percentage (%)', fontweight='bold')
    ax2.set_xlabel('Timeline (Month)', fontweight='bold')
    ax2.set_title('(b) Phishing Attacks: Atomicity Trend', loc='left', fontweight='bold', pad=10)
    ax2.set_ylim(0, 100)
    ax2.yaxis.set_major_formatter(mtick.PercentFormatter())

    padding = pd.Timedelta(days=15)
    ax2.set_xlim(df['Period'].min() - padding, df['Period'].max() + padding)
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax2.xaxis.set_minor_locator(mdates.MonthLocator(interval=1))

    for ax in [ax1, ax2]:
        ax.grid(axis='y', linestyle='--', alpha=0.3)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

    plt.tight_layout()
    plt.savefig(os.path.join(FIGURE_DIR, 'monthly_atomicity_trend_comparison.pdf'), dpi=300, bbox_inches='tight')
    plt.show()


def combined_feature_bar_chart():
    """Fig: Diverging bar chart comparing benign vs malicious combined feature hit rates."""
    _setup_academic_style()

    features = [
        'Self-Submit\n+ Third-Party Transfer', 'Infinite Allowance\n+ Ghost Spender',
        'Infinite Time\n+ New Contract', 'Delegated-Submit\n+ Third-Party Transfer',
        'Infinite Allowance\n+ Zero Utilization',
    ]
    benign_pct = [15.80, 3.96, 0.26, 6.54, 16.95]
    malicious_pct = [2.08, 11.26, 15.40, 71.03, 79.98]
    benign_vals = [-x for x in benign_pct]

    fig, ax = plt.subplots(figsize=(9, 4))
    color_benign = '#C2D6ED'
    color_malic = '#8B0000'
    height = 0.45

    ax.barh(np.arange(len(features)), benign_vals, height, label='Benign', color=color_benign, edgecolor='white', linewidth=1.5)
    ax.barh(np.arange(len(features)), malicious_pct, height, label='Malicious', color=color_malic, edgecolor='white', linewidth=1.5)
    ax.axvline(0, color='#222222', linewidth=1.2, zorder=3)

    xticks = [-20, -10, 0, 20, 40, 60, 80]
    ax.set_xticks(xticks)
    ax.set_xticklabels([f"{abs(x)}%" if x != 0 else "0" for x in xticks], fontsize=14)
    ax.set_xlim(-30, 90)
    ax.set_yticks(np.arange(len(features)))
    ax.set_yticklabels(features, fontsize=14)

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['bottom'].set_color('#cccccc')
    ax.tick_params(axis='y', length=0, pad=10)
    ax.grid(axis='x', linestyle='--', alpha=0.4, color='#999999', zorder=0)

    for i, (b_val, m_val) in enumerate(zip(benign_pct, malicious_pct)):
        ax.annotate(f"{b_val}%", xy=(-b_val, i), xytext=(-6, 0), textcoords="offset points",
                    ha='right', va='center', fontsize=14, fontweight='bold')
        ax.annotate(f"{m_val}%", xy=(m_val, i), xytext=(6, 0), textcoords="offset points",
                    ha='left', va='center', fontsize=14, fontweight='bold')

    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=2, frameon=False, fontsize=14)
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURE_DIR, 'combined_feature_bar.pdf'), dpi=300, bbox_inches='tight')
    plt.show()


def combined_features_dumbbell():
    """Fig: Dumbbell (Cleveland dot) chart for combined feature comparison."""
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

    ax.scatter(benign, y, color=color_benign, s=90, zorder=4, edgecolors='white', linewidth=1.2, label='Benign')
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


def quality_of_reports():
    """Fig: RQ3 evaluation - LLM report quality bar chart with error bars."""
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


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Generate paper figures")
    parser.add_argument("func", nargs="?", default="monthly_permits",
                        choices=["monthly_permits", "plot_global_atomicity",
                                 "combined_feature_bar_chart", "combined_features_dumbbell",
                                 "quality_of_reports"])
    args = parser.parse_args()
    globals()[args.func]()
