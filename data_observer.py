import hashlib
from datetime import datetime
import pandas as pd

SALES_LOG_FILE = "sales_log.csv"
DELIVERIES_LOG_FILE = "deliveries_log.csv"
OBSERVATIONS_FILE = "ai_observations.csv"


def _load_sales() -> pd.DataFrame:
    try:
        sdf = pd.read_csv(SALES_LOG_FILE)
    except Exception:
        return pd.DataFrame(columns=["Timestamp", "Store", "Revenue"])
    for col in ["Timestamp", "Store", "Revenue"]:
        if col not in sdf.columns:
            sdf[col] = "" if col in ["Timestamp", "Store"] else 0
    sdf["Timestamp"] = pd.to_datetime(sdf["Timestamp"], errors="coerce")
    sdf["Revenue"] = pd.to_numeric(sdf["Revenue"], errors="coerce").fillna(0.0)
    return sdf.dropna(subset=["Timestamp"])


def _load_deliveries() -> pd.DataFrame:
    try:
        ddf = pd.read_csv(DELIVERIES_LOG_FILE)
    except Exception:
        return pd.DataFrame(columns=["timestamp", "store", "total_sales"])
    for col in ["timestamp", "store", "total_sales"]:
        if col not in ddf.columns:
            ddf[col] = "" if col in ["timestamp", "store"] else 0
    ddf["timestamp"] = pd.to_datetime(ddf["timestamp"], errors="coerce")
    ddf["total_sales"] = pd.to_numeric(ddf["total_sales"], errors="coerce").fillna(0.0)
    return ddf.dropna(subset=["timestamp"])


def _load_observations() -> pd.DataFrame:
    try:
        odf = pd.read_csv(OBSERVATIONS_FILE, dtype=str).fillna("")
    except Exception:
        odf = pd.DataFrame()
    required = [
        "observation_id",
        "pattern_key",
        "created_at",
        "store",
        "observation_type",
        "details",
        "suggested_action",
        "status",
        "source",
    ]
    for col in required:
        if col not in odf.columns:
            odf[col] = ""
    return odf[required]


def _save_observations(odf: pd.DataFrame) -> None:
    odf.to_csv(OBSERVATIONS_FILE, index=False, encoding="utf-8")


def detect_and_write_observations() -> int:
    """
    Recurring pattern detector:
    If the same store has a delivery value gap (delivery < 85% of sales)
    for 3 consecutive store-days, write an automated observation.
    """
    sales_df = _load_sales()
    del_df = _load_deliveries()
    if sales_df.empty:
        return 0

    sales_daily = sales_df.copy()
    sales_daily["date"] = sales_daily["Timestamp"].dt.date
    sales_daily = sales_daily.groupby(["Store", "date"], as_index=False)["Revenue"].sum().rename(
        columns={"Store": "store", "Revenue": "sales_revenue"}
    )

    if del_df.empty:
        del_daily = pd.DataFrame(columns=["store", "date", "delivered_value"])
    else:
        del_daily = del_df.copy()
        del_daily["date"] = del_daily["timestamp"].dt.date
        del_daily = del_daily.groupby(["store", "date"], as_index=False)["total_sales"].sum().rename(
            columns={"total_sales": "delivered_value"}
        )

    merged = pd.merge(sales_daily, del_daily, on=["store", "date"], how="left")
    merged["delivered_value"] = pd.to_numeric(merged["delivered_value"], errors="coerce").fillna(0.0)
    merged["discrepancy_flag"] = merged["delivered_value"] < (merged["sales_revenue"] * 0.85)

    existing = _load_observations()
    existing_keys = set(existing["pattern_key"].astype(str).tolist())
    new_rows = []

    for store_name, grp in merged.sort_values("date").groupby("store", dropna=False):
        streak = 0
        for _, row in grp.sort_values("date").iterrows():
            if bool(row.get("discrepancy_flag", False)):
                streak += 1
            else:
                streak = 0

            if streak >= 3:
                date_str = row["date"].isoformat() if hasattr(row["date"], "isoformat") else str(row["date"])
                pattern_key = f"store_gap3|{store_name}|{date_str}"
                if pattern_key in existing_keys:
                    continue
                sales_val = float(row.get("sales_revenue", 0.0))
                delivered_val = float(row.get("delivered_value", 0.0))
                gap_val = sales_val - delivered_val
                obs_id = hashlib.md5(pattern_key.encode("utf-8")).hexdigest()[:16]
                new_rows.append(
                    {
                        "observation_id": obs_id,
                        "pattern_key": pattern_key,
                        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "store": str(store_name),
                        "observation_type": "Recurring Delivery Gap",
                        "details": f"3-day recurring mismatch for {store_name}. Sales={sales_val:.2f}, Delivered={delivered_val:.2f}, Gap={gap_val:.2f}.",
                        "suggested_action": "Review delivery cadence and reconcile route handover checks.",
                        "status": "open",
                        "source": "data_observer_script",
                    }
                )
                existing_keys.add(pattern_key)

    if not new_rows:
        return 0
    out = pd.concat([existing, pd.DataFrame(new_rows)], ignore_index=True)
    _save_observations(out)
    return len(new_rows)


if __name__ == "__main__":
    created = detect_and_write_observations()
    print(f"Automated observations created: {created}")
