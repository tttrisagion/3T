#!/usr/bin/env python3
"""
ANN Parameter Optimization Analysis

Analyzes historical Providence run data to identify which ANN parameter ranges
correlate with profitability. Computes Pareto frontier analysis per symbol and
outputs suggested narrowed config ranges.

Usage:
    python scripts/analyze_ann_params.py [options]
      --host         DB host (default: localhost)
      --symbol       Filter to one symbol
      --min-runs     Minimum runs required (default: 100)
      --output/-o    Write report to file
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import mysql.connector
import pandas as pd
import yaml

# Parameters that are tunable via config ranges (excludes fixed/derived values)
TUNABLE_PARAMS = [
    "max_duration",
    "pos_scaler",
    "apr_target",
    "min_goal",
    "max_goal",
    "min_goal_weight",
    "max_goal_weight",
    "volatility_entropy_window_minutes",
    "volatility_entropy_window_samples",
    "machine_vision_entropy_max",
    "rolling_apr_minutes",
]

# Reverse transforms: stored_value * multiplier = config_space_value
# Derived from generate_ann_params() in trading_logic.py
REVERSE_TRANSFORMS = {
    "max_duration": {"multiplier": 1, "config_key": "max_duration"},
    "pos_scaler": {"multiplier": 1000, "config_key": "pos_scaler"},
    # apr_target: stored = config_val / (balance_divisor * apr_target_divisor)
    # reverse: stored * balance_divisor * apr_target_divisor = config_val
    # balance_divisor=5, apr_target_divisor=1.618 → multiplier = 8.09
    "apr_target": {
        "multiplier": None,
        "config_key": "apr_target",
    },  # computed at runtime
    "min_goal": {"multiplier": 10, "config_key": "min_goal"},
    "max_goal": {"multiplier": 1, "config_key": "max_goal"},
    "min_goal_weight": {"multiplier": 10000, "config_key": "min_goal_weight"},
    "max_goal_weight": {"multiplier": 10000, "config_key": "max_goal_weight"},
    "machine_vision_entropy_max": {
        "multiplier": 100,
        "config_key": "machine_vision_entropy_max",
    },
    "volatility_entropy_window_minutes": {
        "multiplier": 1,
        "config_key": "entropy_window_minutes",
    },
    "volatility_entropy_window_samples": {
        "multiplier": 1,
        "config_key": "entropy_window_samples",
    },
    "rolling_apr_minutes": {"multiplier": 1, "config_key": "rolling_apr_minutes"},
}


def load_configs():
    """Load config.yml and secrets.yml from project root."""
    project_root = Path(__file__).resolve().parent.parent
    with open(project_root / "config.yml") as f:
        cfg = yaml.safe_load(f)
    with open(project_root / "secrets.yml") as f:
        secrets = yaml.safe_load(f)
    return cfg, secrets


def get_apr_target_multiplier(cfg):
    """Compute the apr_target reverse-transform multiplier from config."""
    ann = cfg.get("providence", {}).get("ann_params", {})
    balance_divisor = ann.get("balance_divisor", 5)
    apr_target_divisor = ann.get("apr_target_divisor", 1.618)
    return balance_divisor * apr_target_divisor


def fetch_runs(host, cfg, secrets):
    """Fetch all runs with valid ann_params from MariaDB."""
    db_cfg = cfg.get("database", {})
    db_secrets = secrets.get("database", {})

    cnx = mysql.connector.connect(
        host=host,
        port=3306,
        user=db_cfg.get("user", "root"),
        password=db_secrets.get("password", ""),
        database=db_cfg.get("database", "3t"),
    )
    cursor = cnx.cursor(dictionary=True)
    cursor.execute("""
        SELECT id, start_time, end_time, start_balance, end_balance,
               max_duration, position_direction, symbol, ann_params,
               live_pnl, height
        FROM runs
        WHERE ann_params IS NOT NULL
          AND ann_params != ''
    """)
    rows = cursor.fetchall()
    cursor.close()
    cnx.close()
    return rows


def parse_runs(rows):
    """Parse raw DB rows into a pandas DataFrame with ann_params expanded."""
    records = []
    for row in rows:
        try:
            params = json.loads(row["ann_params"])
        except (json.JSONDecodeError, TypeError):
            continue

        record = {
            "run_id": row["id"],
            "start_time": row["start_time"],
            "end_time": row["end_time"],
            "start_balance": row["start_balance"],
            "end_balance": row["end_balance"],
            "symbol": row["symbol"],
            "live_pnl": row["live_pnl"] or 0.0,
            "height": row["height"],
            "position_direction": row["position_direction"],
            "completed": row["end_time"] is not None,
        }

        # Duration in seconds
        if row["end_time"] and row["start_time"]:
            record["duration_s"] = (row["end_time"] - row["start_time"]).total_seconds()
        else:
            record["duration_s"] = None

        # Extract tunable params
        for p in TUNABLE_PARAMS:
            record[p] = params.get(p)

        # Also grab max_direction_reversal for reversal_divisor computation
        record["max_direction_reversal"] = params.get("max_direction_reversal")
        record["system_swing"] = params.get("system_swing")
        record["leverage"] = params.get("leverage")
        record["choose"] = params.get("choose")

        records.append(record)

    df = pd.DataFrame(records)
    if len(df) == 0:
        return df

    # Compute reversal_divisor (derived param)
    mask = (df["max_direction_reversal"].notna()) & (df["max_direction_reversal"] > 0)
    df.loc[mask, "reversal_divisor"] = (
        df.loc[mask, "max_duration"] / df.loc[mask, "max_direction_reversal"]
    )

    # Classify profitability
    df["profitable"] = df["live_pnl"] > 0

    return df


def section_overview(df):
    """Section 1: Overview statistics."""
    lines = []
    lines.append("=" * 70)
    lines.append("1. OVERVIEW")
    lines.append("=" * 70)
    total = len(df)
    completed = df["completed"].sum()
    active = total - completed
    profitable = df["profitable"].sum()
    pct_profitable = (profitable / total * 100) if total > 0 else 0

    lines.append(f"  Total runs analyzed:    {total:,}")
    lines.append(f"  Completed runs:         {completed:,}")
    lines.append(f"  Active runs:            {active:,}")
    lines.append(f"  Profitable runs:        {profitable:,} ({pct_profitable:.1f}%)")
    lines.append(f"  Avg PnL:                {df['live_pnl'].mean():.4f}")
    lines.append(f"  Median PnL:             {df['live_pnl'].median():.4f}")
    lines.append(f"  Total PnL:              {df['live_pnl'].sum():.4f}")
    lines.append(f"  Std Dev PnL:            {df['live_pnl'].std():.4f}")
    lines.append(f"  Max PnL:                {df['live_pnl'].max():.4f}")
    lines.append(f"  Min PnL:                {df['live_pnl'].min():.4f}")
    lines.append("")
    return "\n".join(lines)


def section_height_analysis(df):
    """Section 2: Per-height cohort analysis."""
    lines = []
    lines.append("=" * 70)
    lines.append("2. HEIGHT (EPOCH) ANALYSIS")
    lines.append("=" * 70)

    # Split into active (NULL height AND not completed) vs height groups
    active_mask = df["height"].isna()
    active = df[active_mask]
    with_height = df[~active_mask]

    if len(with_height) > 0:
        height_groups = with_height.groupby("height")
        lines.append(
            f"\n  {'Height':<10} {'Runs':>8} {'Profitable%':>13} {'Avg PnL':>12} {'Sum PnL':>14}"
        )
        lines.append(f"  {'-' * 10} {'-' * 8} {'-' * 13} {'-' * 12} {'-' * 14}")

        for height, group in sorted(height_groups, key=lambda x: x[0]):
            n = len(group)
            pct = (group["profitable"].sum() / n * 100) if n > 0 else 0
            avg = group["live_pnl"].mean()
            total = group["live_pnl"].sum()
            lines.append(
                f"  {int(height):<10} {n:>8,} {pct:>12.1f}% {avg:>12.4f} {total:>14.4f}"
            )
    else:
        lines.append("  No completed height cohorts found.")

    if len(active) > 0:
        n = len(active)
        pct = (active["profitable"].sum() / n * 100) if n > 0 else 0
        avg = active["live_pnl"].mean()
        total = active["live_pnl"].sum()
        lines.append(
            f"\n  {'(active)':<10} {n:>8,} {pct:>12.1f}% {avg:>12.4f} {total:>14.4f}"
        )

    lines.append("")
    return "\n".join(lines)


def section_symbol_breakdown(df):
    """Section 3: Per-symbol statistics."""
    lines = []
    lines.append("=" * 70)
    lines.append("3. PER-SYMBOL BREAKDOWN")
    lines.append("=" * 70)

    if df["symbol"].isna().all():
        lines.append("  No symbol data available.")
        lines.append("")
        return "\n".join(lines)

    lines.append(
        f"\n  {'Symbol':<20} {'Runs':>8} {'Profitable%':>13} {'Avg PnL':>12} {'Sum PnL':>14}"
    )
    lines.append(f"  {'-' * 20} {'-' * 8} {'-' * 13} {'-' * 12} {'-' * 14}")

    for symbol, group in sorted(df.groupby("symbol"), key=lambda x: x[0]):
        n = len(group)
        pct = (group["profitable"].sum() / n * 100) if n > 0 else 0
        avg = group["live_pnl"].mean()
        total = group["live_pnl"].sum()
        lines.append(
            f"  {symbol:<20} {n:>8,} {pct:>12.1f}% {avg:>12.4f} {total:>14.4f}"
        )

    lines.append("")
    return "\n".join(lines)


def percentile_table(series, label=""):
    """Compute percentile row for a series."""
    if len(series) == 0 or series.isna().all():
        return None
    return {
        "label": label,
        "count": int(series.count()),
        "P10": series.quantile(0.10),
        "P25": series.quantile(0.25),
        "P50": series.quantile(0.50),
        "P75": series.quantile(0.75),
        "P90": series.quantile(0.90),
        "mean": series.mean(),
    }


def section_parameter_analysis(df):
    """Section 4: Per-parameter percentile comparison."""
    lines = []
    lines.append("=" * 70)
    lines.append("4. PARAMETER ANALYSIS")
    lines.append("=" * 70)

    profitable = df[df["profitable"]]
    unprofitable = df[~df["profitable"]]

    params_to_analyze = TUNABLE_PARAMS + ["reversal_divisor"]

    for param in params_to_analyze:
        if param not in df.columns:
            continue
        col = df[param].dropna()
        if len(col) == 0:
            continue

        lines.append(f"\n  --- {param} ---")

        all_stats = percentile_table(df[param], "All")
        prof_stats = percentile_table(profitable[param], "Profitable")
        unprof_stats = percentile_table(unprofitable[param], "Unprofitable")

        header = f"  {'Group':<14} {'Count':>7} {'P10':>12} {'P25':>12} {'P50':>12} {'P75':>12} {'P90':>12} {'Mean':>12}"
        lines.append(header)
        lines.append(
            f"  {'-' * 14} {'-' * 7} {'-' * 12} {'-' * 12} {'-' * 12} {'-' * 12} {'-' * 12} {'-' * 12}"
        )

        for stats in [all_stats, prof_stats, unprof_stats]:
            if stats:
                lines.append(
                    f"  {stats['label']:<14} {stats['count']:>7,} "
                    f"{stats['P10']:>12.6f} {stats['P25']:>12.6f} {stats['P50']:>12.6f} "
                    f"{stats['P75']:>12.6f} {stats['P90']:>12.6f} {stats['mean']:>12.6f}"
                )

        # Delta (profitable median - all median)
        if all_stats and prof_stats:
            delta = prof_stats["P50"] - all_stats["P50"]
            pct_delta = (delta / all_stats["P50"] * 100) if all_stats["P50"] != 0 else 0
            lines.append(f"  Delta (P50):  {delta:>+.6f} ({pct_delta:>+.1f}%)")

    lines.append("")
    return "\n".join(lines)


def compute_pareto_frontier(df):
    """
    Compute Pareto frontier per symbol: maximize live_pnl.
    Returns DataFrame of frontier runs.
    """
    if len(df) == 0:
        return pd.DataFrame()

    frontier_runs = []
    for symbol, group in df.groupby("symbol"):
        if group["live_pnl"].isna().all():
            continue
        # Sort by PnL descending, take non-dominated (top performers)
        sorted_group = group.sort_values("live_pnl", ascending=False)
        # Frontier: runs with PnL strictly better than all previously seen
        # Since we're single-objective (maximize PnL), the frontier is the top tier
        # Use top 5% or at least 10 runs as the frontier
        n_frontier = max(10, int(len(sorted_group) * 0.05))
        frontier = sorted_group.head(n_frontier)
        frontier_runs.append(frontier)

    if frontier_runs:
        return pd.concat(frontier_runs, ignore_index=True)
    return pd.DataFrame()


def section_pareto_frontier(df):
    """Section 5: Pareto frontier analysis."""
    lines = []
    lines.append("=" * 70)
    lines.append("5. PARETO FRONTIER (Top 5% PnL per Symbol)")
    lines.append("=" * 70)

    frontier = compute_pareto_frontier(df)
    if len(frontier) == 0:
        lines.append("  Insufficient data for Pareto analysis.")
        lines.append("")
        return "\n".join(lines), frontier

    lines.append(f"\n  Frontier size: {len(frontier):,} runs (from {len(df):,} total)")
    lines.append(f"  Frontier avg PnL:     {frontier['live_pnl'].mean():.4f}")
    lines.append(f"  Population avg PnL:   {df['live_pnl'].mean():.4f}")

    params_to_compare = TUNABLE_PARAMS + ["reversal_divisor"]
    lines.append(
        f"\n  {'Parameter':<38} {'Population P10-P90':>22} {'Frontier P10-P90':>22}"
    )
    lines.append(f"  {'-' * 38} {'-' * 22} {'-' * 22}")

    for param in params_to_compare:
        if param not in df.columns or param not in frontier.columns:
            continue
        pop_col = df[param].dropna()
        front_col = frontier[param].dropna()
        if len(pop_col) == 0 or len(front_col) == 0:
            continue

        pop_range = f"{pop_col.quantile(0.10):.4f} - {pop_col.quantile(0.90):.4f}"
        front_range = f"{front_col.quantile(0.10):.4f} - {front_col.quantile(0.90):.4f}"
        lines.append(f"  {param:<38} {pop_range:>22} {front_range:>22}")

    lines.append("")
    return "\n".join(lines), frontier


def section_suggested_ranges(df, cfg):
    """Section 6: Suggested config ranges from profitable runs."""
    lines = []
    lines.append("=" * 70)
    lines.append("6. SUGGESTED CONFIG RANGES")
    lines.append("=" * 70)
    lines.append(
        "  Based on P10-P90 of profitable runs, reverse-transformed to config space."
    )
    lines.append("  Compare with current config.yml ranges.\n")

    profitable = df[df["profitable"]]
    if len(profitable) < 10:
        lines.append("  WARNING: Too few profitable runs for reliable suggestions.")
        lines.append("")
        return "\n".join(lines)

    ann_cfg = cfg.get("providence", {}).get("ann_params", {})
    apr_multiplier = get_apr_target_multiplier(cfg)
    # Set runtime multiplier for apr_target
    REVERSE_TRANSFORMS["apr_target"]["multiplier"] = apr_multiplier

    lines.append("  # Suggested ann_params ranges for config.yml")
    lines.append("  ann_params:")

    params_to_suggest = TUNABLE_PARAMS + ["reversal_divisor"]

    for param in params_to_suggest:
        if param not in profitable.columns:
            continue
        col = profitable[param].dropna()
        if len(col) == 0:
            continue

        p10 = col.quantile(0.10)
        p90 = col.quantile(0.90)

        transform = REVERSE_TRANSFORMS.get(param)
        if transform:
            multiplier = transform["multiplier"]
            config_key = transform["config_key"]
        else:
            multiplier = 1
            config_key = param

        # Apply reverse transform
        cfg_p10 = p10 * multiplier
        cfg_p90 = p90 * multiplier

        # Get current config range for comparison
        current = ann_cfg.get(config_key)
        if isinstance(current, list) and len(current) == 2:
            current_str = f"[{current[0]}, {current[1]}]"
        elif current is not None:
            current_str = str(current)
        else:
            current_str = "not set"

        # Format as integers if the config values are integers
        if param in (
            "max_duration",
            "max_goal",
            "volatility_entropy_window_minutes",
            "volatility_entropy_window_samples",
            "rolling_apr_minutes",
        ):
            lines.append(
                f"    {config_key}: [{int(round(cfg_p10))}, {int(round(cfg_p90))}]"
                f"  # current: {current_str}"
            )
        elif param == "reversal_divisor":
            lines.append(
                f"    {config_key}: [{cfg_p10:.1f}, {cfg_p90:.1f}]"
                f"  # current: {current_str}"
            )
        else:
            lines.append(
                f"    {config_key}: [{cfg_p10:.3f}, {cfg_p90:.3f}]"
                f"  # current: {current_str}"
            )

    lines.append("")
    return "\n".join(lines)


def section_recommendations(df, frontier):
    """Section 7: Actionable recommendations."""
    lines = []
    lines.append("=" * 70)
    lines.append("7. ACTIONABLE RECOMMENDATIONS")
    lines.append("=" * 70)

    profitable = df[df["profitable"]]
    unprofitable = df[~df["profitable"]]

    if len(profitable) == 0:
        lines.append("  No profitable runs found. Cannot generate recommendations.")
        lines.append("")
        return "\n".join(lines)

    # Find parameters with largest profitable shift (median delta as % of population median)
    shifts = []
    params_to_check = TUNABLE_PARAMS + ["reversal_divisor"]

    for param in params_to_check:
        if param not in df.columns:
            continue
        all_med = df[param].median()
        prof_med = profitable[param].median()
        if all_med != 0 and not pd.isna(all_med) and not pd.isna(prof_med):
            delta_pct = abs((prof_med - all_med) / all_med * 100)
            direction = "higher" if prof_med > all_med else "lower"
            shifts.append((param, delta_pct, direction, prof_med, all_med))

    shifts.sort(key=lambda x: x[1], reverse=True)

    lines.append("\n  Parameters with LARGEST profitable shift (median):")
    for param, delta, direction, prof_val, all_val in shifts[:5]:
        lines.append(
            f"    {param:<38} {delta:>6.1f}% {direction:<8} "
            f"(profitable: {prof_val:.6f}, all: {all_val:.6f})"
        )

    # Narrowing opportunities: params where profitable P10-P90 is much narrower than population
    lines.append("\n  Narrowing opportunities (profitable range / population range):")
    narrowing = []
    for param in params_to_check:
        if param not in df.columns:
            continue
        pop_range = df[param].quantile(0.90) - df[param].quantile(0.10)
        prof_range = profitable[param].quantile(0.90) - profitable[param].quantile(0.10)
        if pop_range > 0:
            ratio = prof_range / pop_range
            narrowing.append((param, ratio, pop_range, prof_range))

    narrowing.sort(key=lambda x: x[1])
    for param, ratio, pop_r, prof_r in narrowing[:5]:
        lines.append(
            f"    {param:<38} ratio: {ratio:.2f}x "
            f"(pop range: {pop_r:.6f}, prof range: {prof_r:.6f})"
        )

    # Sample size warnings
    lines.append("\n  Sample size warnings:")
    total = len(df)
    n_prof = len(profitable)
    n_frontier = len(frontier) if frontier is not None and len(frontier) > 0 else 0

    if total < 1000:
        lines.append(
            f"    WARNING: Only {total:,} total runs. Results may be unreliable."
        )
    if n_prof < 50:
        lines.append(
            f"    WARNING: Only {n_prof:,} profitable runs. Narrow ranges are speculative."
        )
    if n_frontier < 20:
        lines.append(f"    WARNING: Pareto frontier has only {n_frontier:,} runs.")
    if total >= 1000 and n_prof >= 50:
        lines.append(
            f"    OK: {total:,} total runs, {n_prof:,} profitable. Sample size adequate."
        )

    # Symbol-specific notes
    lines.append("\n  Per-symbol profitability ranking:")
    sym_stats = []
    for symbol, group in df.groupby("symbol"):
        n = len(group)
        pct = (group["profitable"].sum() / n * 100) if n > 0 else 0
        avg_pnl = group["live_pnl"].mean()
        sym_stats.append((symbol, n, pct, avg_pnl))
    sym_stats.sort(key=lambda x: x[2], reverse=True)
    for symbol, n, pct, avg_pnl in sym_stats:
        lines.append(
            f"    {symbol:<20} {pct:>6.1f}% profitable ({n:>6,} runs, avg PnL: {avg_pnl:.4f})"
        )

    lines.append("")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Analyze Providence ANN parameters for optimization"
    )
    parser.add_argument(
        "--host", default="localhost", help="Database host (default: localhost)"
    )
    parser.add_argument("--symbol", default=None, help="Filter to one symbol")
    parser.add_argument(
        "--min-runs", type=int, default=100, help="Minimum runs required (default: 100)"
    )
    parser.add_argument("-o", "--output", default=None, help="Write report to file")
    args = parser.parse_args()

    cfg, secrets = load_configs()

    print(f"Connecting to MariaDB at {args.host}:3306...")
    rows = fetch_runs(args.host, cfg, secrets)
    print(f"Fetched {len(rows):,} runs with ann_params.")

    df = parse_runs(rows)
    if len(df) == 0:
        print("ERROR: No valid runs found after parsing.", file=sys.stderr)
        sys.exit(1)

    if args.symbol:
        df = df[df["symbol"] == args.symbol]
        if len(df) == 0:
            print(f"ERROR: No runs found for symbol '{args.symbol}'.", file=sys.stderr)
            sys.exit(1)

    if len(df) < args.min_runs:
        print(
            f"ERROR: Only {len(df):,} runs found, minimum is {args.min_runs}. "
            f"Use --min-runs to lower the threshold.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Build report
    report_parts = []
    report_parts.append("Providence ANN Parameter Analysis Report")
    report_parts.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if args.symbol:
        report_parts.append(f"Filtered to symbol: {args.symbol}")
    report_parts.append("")

    report_parts.append(section_overview(df))
    report_parts.append(section_height_analysis(df))
    report_parts.append(section_symbol_breakdown(df))
    report_parts.append(section_parameter_analysis(df))

    pareto_text, frontier = section_pareto_frontier(df)
    report_parts.append(pareto_text)
    report_parts.append(section_suggested_ranges(df, cfg))
    report_parts.append(section_recommendations(df, frontier))

    report = "\n".join(report_parts)

    if args.output:
        with open(args.output, "w") as f:
            f.write(report)
        print(f"Report written to {args.output}")
    else:
        print(report)


if __name__ == "__main__":
    main()
