import hashlib
import os
from datetime import datetime
import pandas as pd
import streamlit as st
from pdf_generator import generate_report


USERS_FILE = "users.csv"
DELIVERY_LOG_FILE = "deliveries_log.csv"
PENDING_DELIVERY_FILE = "pending_deliveries.csv"
DISCREPANCY_LOG_FILE = "discrepancy_log.csv"


def _safe_read_csv(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _inject_theme() -> None:
    st.markdown(
        """
        <style>
            .stApp {
                background: radial-gradient(1200px 700px at 8% -20%, #1c3156 0%, #0b1425 45%);
                color: #e8eefc;
            }
            .fin-card {
                border: 1px solid #2f4f86;
                border-radius: 14px;
                padding: 0.9rem 1rem;
                background: linear-gradient(145deg, #132744 0%, #101b30 100%);
                box-shadow: 0 0 16px rgba(74, 125, 235, 0.22);
                margin-bottom: 0.55rem;
            }
            .rank-badge {
                display: inline-block;
                padding: 0.35rem 0.7rem;
                border-radius: 999px;
                font-weight: 700;
                letter-spacing: 0.2px;
                border: 1px solid #47659a;
                box-shadow: 0 0 14px rgba(111, 150, 246, 0.35);
                background: linear-gradient(120deg, #1f3d72 0%, #122446 100%);
                color: #eaf1ff;
            }
            .avatar-circle {
                width: 86px;
                height: 86px;
                border-radius: 50%;
                background: linear-gradient(145deg, #214072, #0f1d35);
                border: 1px solid #4368a5;
                box-shadow: 0 0 16px rgba(67, 115, 210, 0.35);
                display: flex;
                align-items: center;
                justify-content: center;
                font-weight: 700;
                font-size: 1.25rem;
                color: #e8eefc;
            }
            .skill-tag {
                display: inline-block;
                margin: 0.2rem 0.32rem 0.2rem 0;
                padding: 0.25rem 0.56rem;
                border-radius: 999px;
                border: 1px solid #38588f;
                background: #132744;
                color: #d6e4ff;
                font-size: 0.85rem;
            }
            .radial-wrap {
                width: 220px;
                height: 220px;
                border-radius: 50%;
                display: flex;
                align-items: center;
                justify-content: center;
                margin: 0 auto;
                border: 1px solid #2f4f86;
                box-shadow: 0 0 18px rgba(78, 127, 231, 0.3);
            }
            .radial-inner {
                width: 152px;
                height: 152px;
                border-radius: 50%;
                background: #0f1b31;
                display: flex;
                align-items: center;
                justify-content: center;
                color: #e8eefc;
                font-weight: 700;
                font-size: 1.6rem;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _get_rank(score: float):
    if score >= 90:
        return "Legend", "🥇"
    if score >= 75:
        return "Professional", "🥈"
    if score >= 50:
        return "Rookie", "🥉"
    return "Under Review", "🔴"


def _load_user(username: str):
    users = _safe_read_csv(USERS_FILE).fillna("")
    if users.empty:
        return {"username": username, "role": "Distributor", "company": "", "commission_rate": 0.0}
    row = users[users["username"].astype(str) == str(username)]
    if row.empty:
        return {"username": username, "role": "Distributor", "company": "", "commission_rate": 0.0}
    r = row.iloc[0]
    return {
        "username": str(r.get("username", username)),
        "role": str(r.get("role", "Distributor")),
        "company": str(r.get("company", "")),
        "commission_rate": float(pd.to_numeric(r.get("commission_rate", 0), errors="coerce") or 0.0),
    }


def _compute_metrics(user):
    uname = user["username"]
    company = user["company"]

    ddf = _safe_read_csv(DELIVERY_LOG_FILE).fillna("")
    if not ddf.empty:
        ddf = ddf[
            (ddf.get("username", "").astype(str) == uname)
            & (ddf.get("company", "").astype(str) == company)
        ].copy()
        ddf["timestamp"] = pd.to_datetime(ddf.get("timestamp", ""), errors="coerce")
        ddf["qty"] = pd.to_numeric(ddf.get("qty", 0), errors="coerce").fillna(0).astype(int)
        ddf["unit_price"] = pd.to_numeric(ddf.get("unit_price", 0), errors="coerce").fillna(0.0)
        ddf["total_sales"] = pd.to_numeric(ddf.get("total_sales", 0), errors="coerce").fillna(0.0)
    total_deliveries = int(len(ddf))

    pdf = _safe_read_csv(PENDING_DELIVERY_FILE).fillna("")
    if not pdf.empty:
        pdf = pdf[
            (pdf.get("username", "").astype(str) == uname)
            & (pdf.get("company", "").astype(str) == company)
        ].copy()
        pdf["timestamp"] = pd.to_datetime(pdf.get("timestamp", ""), errors="coerce")
        pdf["ordered_qty"] = pd.to_numeric(pdf.get("ordered_qty", 0), errors="coerce").fillna(0).astype(int)

    perfect_deliveries = 0
    if total_deliveries > 0:
        d = ddf.copy()
        d["d"] = d["timestamp"].dt.date.astype(str)
        d_agg = d.groupby(["store", "product", "username", "d"], as_index=False)["qty"].sum().rename(columns={"qty": "delivered_qty"})
        if not pdf.empty:
            p = pdf.copy()
            p["d"] = p["timestamp"].dt.date.astype(str)
            p_agg = p.groupby(["store", "product", "username", "d"], as_index=False)["ordered_qty"].sum()
            m = pd.merge(d_agg, p_agg, on=["store", "product", "username", "d"], how="left")
            m["ordered_qty"] = pd.to_numeric(m["ordered_qty"], errors="coerce").fillna(m["delivered_qty"])
            perfect_deliveries = int((m["ordered_qty"] == m["delivered_qty"]).sum())
        else:
            perfect_deliveries = total_deliveries
    accuracy_rate = float((perfect_deliveries / max(1, total_deliveries)) * 100.0)

    punctuality = 100.0
    late_deliveries = 0
    if total_deliveries > 1:
        c = ddf.sort_values("timestamp").copy()
        if "rs_status" in c.columns:
            c_done = c[c["rs_status"].astype(str).str.contains("completed", case=False, na=False)]
            if not c_done.empty:
                c = c_done
        c["delta_min"] = c["timestamp"].diff().dt.total_seconds().fillna(0) / 60.0
        late_deliveries = int((c["delta_min"] > 15).sum())
        late_pct = float((late_deliveries / max(1, len(c))) * 100.0)
        punctuality = max(0.0, min(100.0, 100.0 - (late_pct * 2.0)))

    years_of_service = max(1, datetime.now().year - 2024)
    reputation_score = max(0.0, min(100.0, (accuracy_rate * 0.65) + (punctuality * 0.35)))
    rank, badge = _get_rank(reputation_score)

    # Earnings + savings
    total_distrocoins = float(ddf["total_sales"].sum()) * float(user["commission_rate"])
    avg_price = float(ddf["unit_price"].mean()) if total_deliveries > 0 else 0.0
    dlog = _safe_read_csv(DISCREPANCY_LOG_FILE).fillna("")
    if not dlog.empty:
        dlog = dlog[dlog.get("company", "").astype(str) == company]
        dlog["ordered_qty"] = pd.to_numeric(dlog.get("ordered_qty", 0), errors="coerce").fillna(0).astype(int)
        dlog["confirmed_qty"] = pd.to_numeric(dlog.get("confirmed_qty", 0), errors="coerce").fillna(0).astype(int)
        discrepancies_caught = int((dlog["ordered_qty"] != dlog["confirmed_qty"]).sum())
    else:
        discrepancies_caught = 0
    total_savings_company = float(discrepancies_caught) * float(avg_price)

    return {
        "total_deliveries": total_deliveries,
        "perfect_deliveries": perfect_deliveries,
        "accuracy_rate": round(accuracy_rate, 2),
        "punctuality": round(punctuality, 2),
        "years_of_service": years_of_service,
        "reputation_score": round(reputation_score, 2),
        "rank": rank,
        "badge": badge,
        "total_distrocoins": round(total_distrocoins, 2),
        "total_savings_company": round(total_savings_company, 2),
        "late_deliveries": late_deliveries,
    }


def _skills_from_metrics(m):
    tags = []
    if m["accuracy_rate"] >= 98:
        tags.append("Precision Expert")
    if m["punctuality"] >= 90:
        tags.append("On-Time Operator")
    if m["total_deliveries"] >= 1000:
        tags.append("Logistics Veteran")
    if m["total_savings_company"] >= 5000:
        tags.append("Cost Guardian")
    if m["reputation_score"] >= 90:
        tags.append("Legendary Performer")
    return tags or ["Developing Specialist"]


def main():
    st.set_page_config(page_title="Living CV | Distro", layout="wide")
    _inject_theme()
    st.title("Professional Identity — Living CV")

    users = _safe_read_csv(USERS_FILE).fillna("")
    usernames = users["username"].astype(str).tolist() if not users.empty and "username" in users.columns else ["distributor_a"]
    session_user = ""
    try:
        au = st.session_state.get("auth_user")
        if isinstance(au, dict):
            session_user = str(au.get("username", "")).strip()
    except Exception:
        session_user = ""
    default_user = session_user if session_user else (sorted(set(usernames))[0] if usernames else "distributor_a")
    username = default_user
    if not session_user:
        username = st.selectbox("Select Profile", sorted(set(usernames)) if usernames else ["distributor_a"])
    user = _load_user(username)
    m = _compute_metrics(user)
    skills = _skills_from_metrics(m)

    initials = "".join([x[:1].upper() for x in str(user["username"]).split("_") if x])[:2] or "U"
    st.markdown(
        f"""
        <div class="fin-card" style="display:flex;align-items:center;justify-content:space-between;gap:1rem;">
            <div style="display:flex;align-items:center;gap:1rem;">
                <div class="avatar-circle">{initials}</div>
                <div>
                    <div style="font-size:1.25rem;font-weight:700;">{user['username']}</div>
                    <div style="color:#9fb2d8;">Role: {user['role']} · Company: {user['company'] or 'N/A'}</div>
                </div>
            </div>
            <div class="rank-badge">{m['badge']} {m['rank']}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("### Reputation Dashboard")
    score_int = max(0, min(100, int(round(m["reputation_score"]))))
    st.markdown(
        f"""
        <div class="radial-wrap" style="background:conic-gradient(#2f80ff {score_int}%, #22314f {score_int}% 100%);">
            <div class="radial-inner">{score_int}%</div>
        </div>
        <div style="text-align:center;color:#9fb2d8;margin-top:0.5rem;">Current Reputation Score</div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("### Proof of Work")
    left, right = st.columns(2)
    with left:
        st.markdown('<div class="fin-card">', unsafe_allow_html=True)
        st.metric("Total Deliveries", f"{m['total_deliveries']}")
        st.metric("Accuracy Rate", f"{m['accuracy_rate']:.2f}%")
        st.metric("Punctuality", f"{m['punctuality']:.2f}%")
        st.markdown("</div>", unsafe_allow_html=True)
    with right:
        st.markdown('<div class="fin-card">', unsafe_allow_html=True)
        st.metric("Total DistroCoins Earned", f"{m['total_distrocoins']:.2f}")
        st.metric("Total Savings for Company", f"{m['total_savings_company']:.2f} ₾")
        st.metric("Years of Service", f"{m['years_of_service']}")
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("### Verified Skills")
    st.markdown(
        "".join([f'<span class="skill-tag">{s}</span>' for s in skills]),
        unsafe_allow_html=True,
    )

    ai_summary = (
        f"{user['username']} maintains {m['accuracy_rate']:.2f}% delivery accuracy and "
        f"{m['punctuality']:.2f}% punctuality with a {m['rank']} rank profile."
    )
    report_payload = {
        "name": user["username"],
        "role": user["role"],
        "rank": f"{m['rank']} Tier",
        "accuracy": m["accuracy_rate"],
        "total_deliveries": m["total_deliveries"],
        "punctuality": m["punctuality"],
        "savings": f"{m['total_savings_company']:.2f} ₾",
        "ai_summary": ai_summary,
        "milestones": skills,
    }

    payload_hash = hashlib.md5(str(report_payload).encode("utf-8")).hexdigest()
    current_hash = st.session_state.get("career_pdf_payload_hash", "")
    if ("career_pdf_data" not in st.session_state) or (current_hash != payload_hash):
        try:
            generated_path = generate_report(report_payload)
            with open(generated_path, "rb") as f:
                st.session_state["career_pdf_data"] = f.read()
            st.session_state["career_pdf_name"] = os.path.basename(generated_path)
            st.session_state["career_pdf_payload_hash"] = payload_hash
        except Exception as exc:
            st.error(f"Failed to generate report: {exc}")
            st.session_state.pop("career_pdf_data", None)

    if "career_pdf_data" in st.session_state:
        st.download_button(
            "Download Verified Career Report",
            data=st.session_state["career_pdf_data"],
            file_name=st.session_state.get("career_pdf_name", f"Career_Proof_{user['username']}.pdf"),
            mime="application/pdf",
            use_container_width=True,
        )


if __name__ == "__main__":
    main()
