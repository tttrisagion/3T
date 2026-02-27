"""
Providence Trading Logic - Core Brains
"""

import random


def generate_ann_params(
    symb, leverage, virtual_balance=7000, choose=None, ann_ranges=None
):
    """
    Generates a new set of randomized ANN parameters for a trading run.

    Args:
        symb: List of symbols.
        leverage: The max leverage for the chosen symbol (from products table).
        virtual_balance: Starting virtual balance for VOMS.
        choose: Index into symb for the chosen symbol. Random if None.
        ann_ranges: Optional dict of parameter ranges from config. Each range
            param is [min, max]; scalar params are used directly. Falls back
            to hardcoded defaults when keys are missing.
    """
    r = ann_ranges or {}

    if choose is None:
        choose = random.randint(0, len(symb) - 1)
    system_swing = bool(random.randint(0, 1))

    dur = r.get("max_duration", [300, 432000])
    max_duration = random.randint(dur[0], dur[1])

    rev = r.get("reversal_divisor", [1, 10])
    max_direction_reversal = int(max_duration / random.randint(rev[0], rev[1]))

    balance_divisor = r.get("balance_divisor", 5)

    ps = r.get("pos_scaler", [1.618, 3.236])
    ps_div = r.get("pos_scaler_divisor", 1000)
    pos_scaler = random.uniform(ps[0], ps[1]) / ps_div

    apr_rng = r.get("apr_target", [20, 100])
    apr_div = r.get("apr_target_divisor", 1.618)
    apr_target = random.randint(apr_rng[0], apr_rng[1]) / balance_divisor / apr_div

    mg = r.get("min_goal", [1, 9])
    min_goal = random.randint(mg[0], mg[1]) / 10

    xg = r.get("max_goal", [1, 10])
    max_goal = random.randint(xg[0], xg[1])

    gw_div = r.get("goal_weight_divisor", 10000)
    mgw = r.get("min_goal_weight", [1, 38])
    xgw = r.get("max_goal_weight", [69, 200])
    min_goal_weight = random.randint(mgw[0], mgw[1]) / gw_div
    max_goal_weight = random.randint(xgw[0], xgw[1]) / gw_div

    ew = r.get("entropy_window_minutes", [1, 15])
    es = r.get("entropy_window_samples", [5, 20])
    volatility_entropy_window_minutes = random.randint(ew[0], ew[1])
    volatility_entropy_window_samples = random.randint(es[0], es[1])

    mve = r.get("machine_vision_entropy_max", [5, 15])
    mve_div = r.get("machine_vision_entropy_divisor", 100)
    machine_vision_entropy_max = random.randint(mve[0], mve[1]) / mve_div

    ra = r.get("rolling_apr_minutes", [1, 30])
    rolling_apr_minutes = random.randint(ra[0], ra[1])
    decision_distance_seconds = 0

    mcmr = r.get("max_cross_margin_ratio", 0.05)
    pw = r.get("pos_weight", 0.032732855219630354)
    pw_div = r.get("pos_weight_divisor", 3)
    max_cross_margin_ratio = mcmr / balance_divisor
    pos_weight = pw / pw_div

    ann_params = {
        "virtual_balance": virtual_balance,
        "choose": choose,
        "leverage": leverage,
        "max_duration": max_duration,
        "max_direction_reversal": max_direction_reversal,
        "balance_divisor": balance_divisor,
        "pos_scaler": pos_scaler,
        "max_cross_margin_ratio": max_cross_margin_ratio,
        "apr_target": apr_target,
        "pos_weight": pos_weight,
        "min_goal": min_goal,
        "max_goal": max_goal,
        "min_goal_weight": min_goal_weight,
        "max_goal_weight": max_goal_weight,
        "volatility_entropy_window_minutes": volatility_entropy_window_minutes,
        "volatility_entropy_window_samples": volatility_entropy_window_samples,
        "machine_vision_entropy_max": machine_vision_entropy_max,
        "rolling_apr_minutes": rolling_apr_minutes,
        "decision_distance_seconds": decision_distance_seconds,
        "system_swing": system_swing,
    }
    return ann_params


def should_approve_trade(position_direction, apr_change, side, system_swing):
    """
    Core decision logic to approve or reject a trade.
    """
    if system_swing:
        side = "buy" if side == "sell" else "sell"

    approve_trade = (
        (position_direction == 0)
        or (
            apr_change > 0
            and (
                (position_direction > 0 and side == "buy")
                or (position_direction < 0 and side == "sell")
            )
        )
        or (
            apr_change <= 0
            and (
                (position_direction > 0 and side == "sell")
                or (position_direction < 0 and side == "buy")
            )
        )
    )

    return approve_trade, side
