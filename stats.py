# ═══════════════════════════════════════════════════════════════════════
#  stats.py — Trade statistics tracker
#
#  Records every trade outcome and provides summary for heartbeat.
#  Persists across the session — reset only on bot restart.
# ═══════════════════════════════════════════════════════════════════════

import time

# ── Session statistics ────────────────────────────────────────────────
_session_start   = time.time()

total_trades     = 0    # total martingale cycles started
winning_trades   = 0    # cycles that ended in WIN
losing_trades    = 0    # cycles that ended in EXHAUSTED
early_wins       = 0    # cycles won via floating > 0.5
early_losses     = 0    # levels skipped via floating < -0.5

total_staked     = 0.0  # total amount staked across all levels
total_profit     = 0.0  # sum of profits from winning cycles
total_loss       = 0.0  # sum of losses from losing cycles
net_pnl          = 0.0  # total_profit - total_loss

skipped_markets  = 0    # signals discarded by ADX filter
best_streak_win  = 0    # highest level at which we won (1-indexed)


def record_win(profit: float, level: int, staked: float, was_early: bool = False):
    global total_trades, winning_trades, early_wins
    global total_staked, total_profit, net_pnl, best_streak_win

    total_trades   += 1
    winning_trades += 1
    if was_early:
        early_wins += 1

    total_staked += staked
    total_profit += profit
    net_pnl      += profit

    if level + 1 > best_streak_win:
        best_streak_win = level + 1


def record_loss(amount: float, level: int):
    global total_trades, losing_trades
    global total_staked, total_loss, net_pnl

    total_trades  += 1
    losing_trades += 1

    total_staked += amount
    total_loss   += amount
    net_pnl      -= amount


def record_early_loss():
    global early_losses
    early_losses += 1


def record_skipped():
    global skipped_markets
    skipped_markets += 1


def win_rate() -> float:
    if total_trades == 0:
        return 0.0
    return round(100.0 * winning_trades / total_trades, 1)


def session_duration() -> str:
    elapsed = int(time.time() - _session_start)
    h = elapsed // 3600
    m = (elapsed % 3600) // 60
    return f"{h}h {m}m"


def summary() -> list:
    """Returns list of log lines for heartbeat display."""
    return [
        f"   {'─'*38}",
        f"   📊 SESSION STATISTICS",
        f"   {'─'*38}",
        f"   Session duration : {session_duration()}",
        f"   Total cycles     : {total_trades}",
        f"   Wins             : {winning_trades}  (Early wins: {early_wins})",
        f"   Losses           : {losing_trades}",
        f"   Win rate         : {win_rate()}%",
        f"   {'─'*38}",
        f"   Total staked     : ${round(total_staked, 2)}",
        f"   Total profit     : +${round(total_profit, 2)}",
        f"   Total loss       : -${round(total_loss, 2)}",
        f"   Net P&L          : {'+'if net_pnl>=0 else ''}${round(net_pnl, 2)}",
        f"   {'─'*38}",
        f"   Early losses     : {early_losses}",
        f"   Best win level   : L{best_streak_win}" if best_streak_win > 0 else "   Best win level   : —",
        f"   Skipped markets  : {skipped_markets}",
        f"   {'─'*38}",
    ]