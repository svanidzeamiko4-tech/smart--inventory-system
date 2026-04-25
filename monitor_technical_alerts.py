import hashlib
from datetime import datetime
from pathlib import Path
import pandas as pd

APP_ERROR_LOG_FILE = "app_errors.log"
TECH_ALERTS_FILE = "tech_alerts.csv"


def _load_alerts() -> pd.DataFrame:
    try:
        adf = pd.read_csv(TECH_ALERTS_FILE, dtype=str).fillna("")
    except Exception:
        adf = pd.DataFrame()
    cols = ["alert_id", "created_at", "severity", "message", "status", "source"]
    for c in cols:
        if c not in adf.columns:
            adf[c] = ""
    return adf[cols]


def _save_alerts(adf: pd.DataFrame) -> None:
    adf.to_csv(TECH_ALERTS_FILE, index=False, encoding="utf-8")


def scan_runtime_errors() -> int:
    log_path = Path(APP_ERROR_LOG_FILE)
    if not log_path.exists():
        return 0
    text = log_path.read_text(encoding="utf-8", errors="ignore").strip()
    if not text:
        return 0
    blocks = [b.strip() for b in text.split("=" * 72) if b.strip()]
    if not blocks:
        return 0
    alerts = _load_alerts()
    existing = set(alerts["alert_id"].astype(str).tolist())
    new_rows = []
    for b in blocks[-10:]:
        first_line = b.splitlines()[0] if b.splitlines() else ""
        seed = b[:300]
        aid = hashlib.md5(seed.encode("utf-8")).hexdigest()[:14]
        if aid in existing:
            continue
        new_rows.append(
            {
                "alert_id": aid,
                "created_at": first_line if first_line else datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "severity": "high",
                "message": "Technical Issue Detected. Reporting to Dev Team...",
                "status": "open",
                "source": "log_monitor",
            }
        )
        existing.add(aid)
    if not new_rows:
        return 0
    out = pd.concat([alerts, pd.DataFrame(new_rows)], ignore_index=True)
    _save_alerts(out)
    return len(new_rows)


if __name__ == "__main__":
    created = scan_runtime_errors()
    print(f"Technical alerts created: {created}")
