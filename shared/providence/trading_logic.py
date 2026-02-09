"""
Providence Trading Logic - Core Brains
"""

import random


def generate_ann_params(symb, leverage, virtual_balance=7000, choose=None):
    """
    Generates a new set of randomized ANN parameters for a trading run.

    Args:
        symb: List of symbols.
        leverage: The max leverage for the chosen symbol (from products table).
        virtual_balance: Starting virtual balance for VOMS.
        choose: Index into symb for the chosen symbol. Random if None.
    """
    if choose is None:
        choose = random.randint(0, len(symb) - 1)
    system_swing = bool(random.randint(0, 1))
    max_duration = random.randint(300, 86400 * 5)
    max_direction_reversal = int(max_duration / random.randint(1, 10))
    balance_divisor, pos_scaler = 5, random.uniform(1.618, 3.236) / 1000
    apr_target = random.randint(20, 100) / balance_divisor / 1.618
    min_goal, max_goal = random.randint(1, 9) / 10, random.randint(1, 10)
    min_goal_weight, max_goal_weight = (
        random.randint(1, 38) / 10000,
        random.randint(69, 200) / 10000,
    )
    volatility_entropy_window_minutes, volatility_entropy_window_samples = (
        random.randint(1, 15),
        random.randint(5, 20),
    )
    machine_vision_entropy_max = random.randint(5, 15) / 100
    rolling_apr_minutes, decision_distance_seconds = random.randint(1, 30), 0
    max_cross_margin_ratio, pos_weight = (
        0.05 / balance_divisor,
        0.032732855219630354 / 3,
    )

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
