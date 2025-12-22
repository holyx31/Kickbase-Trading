from kickbase_api.league import get_league_players_on_market
from kickbase_api.user import get_players_in_squad
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd
import numpy as np


def live_data_predictions(today_df, model, features):
    """Make live data predictions for today_df using the trained model"""

    today_df_features = today_df[features]
    today_df_results = today_df.copy()

    # Predict market value target
    today_df_results["predicted_mv_target"] = np.round(
        model.predict(today_df_features), 2
    )

    # Sort by predicted value
    today_df_results = today_df_results.sort_values(
        "predicted_mv_target", ascending=False
    )

    # Handle Kickbase MV update timing
    now = datetime.now(ZoneInfo("Europe/Berlin"))
    cutoff_time = now.replace(hour=22, minute=15, second=0, microsecond=0)
    date = (now - timedelta(days=1)) if now <= cutoff_time else now
    today_df_results["date"] = date.date()

    # Drop invalid rows
    today_df_results = today_df_results.dropna(subset=["mv"])

    # Keep relevant columns
    today_df_results = today_df_results[
        [
            "player_id",
            "first_name",
            "last_name",
            "position",
            "team_name",
            "date",
            "mv_change_1d",
            "mv_trend_1d",
            "mv",
            "predicted_mv_target",
        ]
    ]

    return today_df_results


def join_current_squad(token, league_id, today_df_results):
    squad_players = get_players_in_squad(token, league_id)

    # --- RESET-SAFE: empty squad ---
    if not squad_players or "it" not in squad_players or not squad_players["it"]:
        print("No squad players found. Skipping squad recommendations.")
        return pd.DataFrame(
            columns=[
                "last_name",
                "team_name",
                "mv",
                "mv_change_yesterday",
                "predicted_mv_target",
                "s_11_prob",
            ]
        )

    squad_df = pd.DataFrame(squad_players["it"])

    # --- Robust detection of player id column ---
    squad_id_col = (
        "i" if "i" in squad_df.columns else
        "pi" if "pi" in squad_df.columns else
        None
    )

    if squad_id_col is None:
        raise RuntimeError(
            f"Cannot determine squad player id column. Columns: {squad_df.columns.tolist()}"
        )

    # Merge squad with predictions
    squad_df = (
        pd.merge(
            today_df_results,
            squad_df,
            left_on="player_id",
            right_on=squad_id_col,
        )
        .drop(columns=[squad_id_col])
    )

    # Rename columns
    if "prob" not in squad_df.columns:
        squad_df["prob"] = np.nan
    squad_df = squad_df.rename(columns={"prob": "s_11_prob"})
    squad_df = squad_df.rename(columns={"mv_change_1d": "mv_change_yesterday"})

    if "mv_x" in squad_df.columns:
        squad_df = squad_df.rename(columns={"mv_x": "mv"})

    squad_df = squad_df[
        [
            "last_name",
            "team_name",
            "mv",
            "mv_change_yesterday",
            "predicted_mv_target",
            "s_11_prob",
        ]
    ]

    return squad_df


def join_current_market(token, league_id, today_df_results):
    """Join predictions with current market data"""

    players_on_market = get_league_players_on_market(token, league_id)
    market_df = pd.DataFrame(players_on_market)

    bid_df = (
        pd.merge(today_df_results, market_df, left_on="player_id", right_on="id")
        .drop(columns=["id"])
    )

    # Expiration handling
    bid_df["hours_to_exp"] = np.round(bid_df["exp"] / 3600, 2)

    now = datetime.now(ZoneInfo("Europe/Berlin"))
    next_22 = now.replace(hour=22, minute=0, second=0, microsecond=0)
    diff = np.round((next_22 - now).total_seconds() / 3600, 2)

    bid_df["expiring_today"] = bid_df["hours_to_exp"] < diff

    # Filter noise
    bid_df = bid_df[bid_df["predicted_mv_target"] > 5000]
    bid_df = bid_df.sort_values("predicted_mv_target", ascending=False)

    if "prob" not in bid_df.columns:
        bid_df["prob"] = np.nan
    bid_df = bid_df.rename(columns={"prob": "s_11_prob"})
    bid_df = bid_df.rename(columns={"mv_change_1d": "mv_change_yesterday"})

    bid_df = bid_df[
        [
            "last_name",
            "team_name",
            "mv",
            "mv_change_yesterday",
            "predicted_mv_target",
            "s_11_prob",
            "hours_to_exp",
            "expiring_today",
        ]
    ]

    return bid_df
