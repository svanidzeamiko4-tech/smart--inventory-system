import pandas as pd
import streamlit as st
from datetime import datetime
import numpy as np
import random
import os

st.set_page_config(page_title="ERP Smart System", layout="wide")
FILE_NAME = "Product.csv.txt"
AUDIT_LOG_FILE = "audit_log.csv"

# 1. მონაცემების მართვა
def ensure_data_structure(df):
    if df.empty:
        return df

    if "Cost_Price" not in df.columns:
        df["Cost_Price"] = 0.0
    if "Selling_Price" not in df.columns:
        # Backward-compatible default for old rows.
        if "Price" in df.columns:
            df["Selling_Price"] = pd.to_numeric(df["Price"], errors="coerce").fillna(0.0)
        else:
            df["Selling_Price"] = 0.0

    df["Cost_Price"] = pd.to_numeric(df["Cost_Price"], errors="coerce").fillna(0.0)
    df["Selling_Price"] = pd.to_numeric(df["Selling_Price"], errors="coerce").fillna(0.0)
    return df


def load_data():
    if os.path.exists(FILE_NAME):
        df = pd.read_csv(FILE_NAME)
        df['Expiry_Date'] = pd.to_datetime(df['Expiry_Date'])
        df = ensure_data_structure(df)
        return df
    return pd.DataFrame()


def save_data(df):
    df.to_csv(FILE_NAME, index=False)


def append_audit_log(product, old_stock, new_stock, difference, reason, cost_price):
    log_row = {
        "Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Product": str(product),
        "Old Stock": int(old_stock),
        "New Stock": int(new_stock),
        "Difference": int(difference),
        "Reason": str(reason),
        "Cost Price": float(cost_price)
    }
    log_df = pd.DataFrame([log_row])
    write_header = not os.path.exists(AUDIT_LOG_FILE)
    log_df.to_csv(AUDIT_LOG_FILE, mode="a", header=write_header, index=False)


def load_audit_log():
    if os.path.exists(AUDIT_LOG_FILE):
        log_df = pd.read_csv(AUDIT_LOG_FILE)
        required_cols = ["Date", "Product", "Old Stock", "New Stock", "Difference", "Reason", "Cost Price"]
        for col in required_cols:
            if col not in log_df.columns:
                log_df[col] = 0 if col in ["Old Stock", "New Stock", "Difference", "Cost Price"] else ""
        log_df["Difference"] = pd.to_numeric(log_df["Difference"], errors="coerce").fillna(0)
        log_df["Cost Price"] = pd.to_numeric(log_df["Cost Price"], errors="coerce").fillna(0.0)
        log_df["Shrinkage_Value"] = (-log_df["Difference"]).clip(lower=0) * log_df["Cost Price"]
        return log_df
    return pd.DataFrame(columns=["Date", "Product", "Old Stock", "New Stock", "Difference", "Reason", "Cost Price", "Shrinkage_Value"])


def recalc_metrics(df):
    sales_cols = [f"Sales_Day{i}" for i in range(1, 8)]
    for col in sales_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        else:
            df[col] = 0

    df["საშ. დღიური გაყიდვა"] = df[sales_cols].mean(axis=1).round(1)
    df["დარჩენილი დღე"] = (df["Current_Stock"] / df["საშ. დღიური გაყიდვა"].replace(0, 0.1)).round(1)
    df["დღე ვადის გასვლამდე"] = (pd.to_datetime(df["Expiry_Date"]) - datetime.now()).dt.days
    return df

if 'df' not in st.session_state:
    st.session_state.df = load_data()

# Profit tracker resets automatically each day.
today_str = datetime.now().strftime("%Y-%m-%d")
if 'profit_date' not in st.session_state:
    st.session_state.profit_date = today_str
if 'daily_profit' not in st.session_state:
    st.session_state.daily_profit = 0.0
if st.session_state.profit_date != today_str:
    st.session_state.profit_date = today_str
    st.session_state.daily_profit = 0.0

# Ensure metrics exist after loading
st.session_state.df = ensure_data_structure(st.session_state.df)
st.session_state.df = recalc_metrics(st.session_state.df)

# RS.GE-ს სიმულაციური ზედნადებები
if 'rs_invoices' not in st.session_state:
    st.session_state.rs_invoices = [
        {"id": "ზედნადები #9901", "product": "stafilo", "qty": 100, "store": "Gldani_Branch", "price": 1.20},
        {"id": "ზედნადები #9902", "product": "Apple", "qty": 50, "store": "Vake_Branch", "price": 2.50}
    ]

df = st.session_state.df

# 2. ანალიტიკური გათვლები
sales_cols = [f"Sales_Day{i}" for i in range(1, 8)]
for col in sales_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

df["საშ. დღიური გაყიდვა"] = df[sales_cols].mean(axis=1).round(1)
df["დარჩენილი დღე"] = (df["Current_Stock"] / df["საშ. დღიური გაყიდვა"].replace(0, 0.1)).round(1)
df["დღე ვადის გასვლამდე"] = (pd.to_datetime(df["Expiry_Date"]) - datetime.now()).dt.days

# --- მთავარი გვერდითა მენიუ ---
st.sidebar.title("🎛️ მართვის პანელი")
page = st.sidebar.radio(
    "გადადი გვერდზე:",
    ["📊 Dashboard & ანალიტიკა", "📥 მარაგების დამატება", "🧾 Inventory Audit", "🔗 RS.GE ინტეგრაცია"]
)

# --- გვერდი 1: DASHBOARD ---
if page == "📊 Dashboard & ანალიტიკა":
    st.title("📊 მაღაზიების ანალიტიკური Dashboard")

    zero_stock = df[df["Current_Stock"] <= 0]
    expiring_soon = df[df["დღე ვადის გასვლამდე"] <= 3]

    # Always render critical stock alerts at the top of Dashboard.
    if not zero_stock.empty:
        zero_stock_names = ", ".join(zero_stock["Product_Name"].astype(str).unique())
        st.error(
            f"❌ ამოიწურა მარაგი: {zero_stock_names}"
        )
        st.warning(
            f"⚠️ გადაუდებელი შევსება საჭიროა: {zero_stock_names}"
        )
    if not expiring_soon.empty:
        st.warning(
            f"⏳ ვადა ამოიწურება 3 დღეზე ნაკლში: {', '.join(expiring_soon['Product_Name'].astype(str).unique())}"
        )

    if st.sidebar.button("🕒 დღის დასრულება (გაყიდვა)"):
        for index, row in df.iterrows():
            avg = row["საშ. დღიური გაყიდვა"] if row["საშ. დღიური გაყიდვა"] > 0 else 5
            sale = round(avg * random.uniform(0.7, 1.3))
            df.at[index, "Current_Stock"] = max(0, row["Current_Stock"] - sale)
        df = recalc_metrics(df)
        save_data(df)
        st.session_state.df = df
        st.rerun()

    # ინდიკატორები
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("📦 სულ პროდუქცია", len(df))
    c2.metric("⚠️ კრიტიკული ვადა", len(df[df["დღე ვადის გასვლამდე"] < 5]))
    c3.metric("📉 დაბალი მარაგი", len(df[df["დარჩენილი დღე"] < 3]))
    c4.metric("💰 Daily Profit", f"{st.session_state.daily_profit:.2f}")

    st.divider()
    st.subheader("📋 დეტალური ნაშთები")

    header_cols = st.columns([2, 2, 1, 1, 1, 1])
    for col, title in zip(header_cols, [
        "მაღაზია", "პროდუქტი", "მარაგი", "დარჩენილი დღე", "თუ ვადა (დღე)", "მოქმედება"
    ]):
        col.markdown(f"**{title}**")

    for index, row in df.iterrows():
        c0, c1, c2, c3, c4, c5 = st.columns([2, 2, 1, 1, 1, 1])
        c0.write(row["Store_Name"])
        c1.write(row["Product_Name"])
        c2.write(row["Current_Stock"])
        c3.write(row["დარჩენილი დღე"])
        c4.write(row["დღე ვადის გასვლამდე"])
        can_sell = row["Current_Stock"] > 0
        if c5.button("Sell", key=f"sell_{index}", disabled=not can_sell):
            unit_profit = float(row.get("Selling_Price", 0)) - float(row.get("Cost_Price", 0))
            st.session_state.daily_profit += unit_profit
            df.at[index, "Current_Stock"] = max(0, int(row["Current_Stock"]) - 1)
            df = recalc_metrics(df)
            save_data(df)
            st.session_state.df = df
            st.rerun()
        if not can_sell:
            c5.caption("Out")

    st.divider()
    st.bar_chart(df.set_index("Product_Name")["Current_Stock"])

# --- გვერდი 2: მარაგების დამატება (ხელით) ---
elif page == "📥 მარაგების დამატება":
    st.title("📥 პროდუქციის ხელით აღრიცხვა")
    with st.form("manual_add"):
        col1, col2 = st.columns(2)
        f_store = col1.selectbox("მაღაზია", df["Store_Name"].unique() if not df.empty else ["Gldani_Branch"])
        f_name = col2.text_input("პროდუქტი")
        f_qty = col1.number_input("რაოდენობა", min_value=1)
        f_cost_price = col1.number_input("Cost Price", min_value=0.0, format="%.2f")
        f_selling_price = col2.number_input("Selling Price", min_value=0.0, format="%.2f")
        f_expiry = col2.date_input("ვადა")
        submitted = st.form_submit_button("შენახვა")

    if submitted:
        new_row = {
            "Store_Name": f_store,
            "Product_Name": f_name,
            "Current_Stock": int(f_qty),
            "Cost_Price": float(f_cost_price),
            "Selling_Price": float(f_selling_price),
            "Price": float(f_selling_price),
            "Sales_Day1": 0,
            "Sales_Day2": 0,
            "Sales_Day3": 0,
            "Sales_Day4": 0,
            "Sales_Day5": 0,
            "Sales_Day6": 0,
            "Sales_Day7": 0,
            "Expiry_Date": pd.to_datetime(f_expiry)
        }
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        df = recalc_metrics(df)
        save_data(df)
        st.session_state.df = df
        st.success("პროდუქტი შენახულია")
        st.experimental_rerun()

elif page == "🧾 Inventory Audit":
    st.title("🧾 Inventory Audit")

    if df.empty:
        st.info("პროდუქტები არ არის დამატებული.")
    else:
        options = {
            f"{row['Store_Name']} | {row['Product_Name']} (Stock: {int(row['Current_Stock'])})": idx
            for idx, row in df.iterrows()
        }
        reasons = ["Damaged", "Expired", "Inventory Mismatch", "Other"]

        with st.form("inventory_audit_form"):
            selected_label = st.selectbox("აირჩიე პროდუქტი", list(options.keys()))
            selected_idx = options[selected_label]
            current_stock = int(df.at[selected_idx, "Current_Stock"])
            new_stock = st.number_input("ახალი ნაშთი", min_value=0, value=current_stock, step=1)
            reason = st.selectbox("Reason", reasons)
            submitted_audit = st.form_submit_button("განახლება და ლოგირება")

        if submitted_audit:
            old_stock = current_stock
            updated_stock = int(new_stock)
            difference = updated_stock - old_stock
            product_name = df.at[selected_idx, "Product_Name"]
            cost_price = float(df.at[selected_idx, "Cost_Price"]) if "Cost_Price" in df.columns else 0.0

            if difference == 0:
                st.warning("ცვლილება არ დაფიქსირდა - ნაშთი უცვლელია.")
            else:
                df.at[selected_idx, "Current_Stock"] = updated_stock
                df = recalc_metrics(df)
                save_data(df)
                st.session_state.df = df
                append_audit_log(product_name, old_stock, updated_stock, difference, reason, cost_price)
                st.success("ინვენტარის ცვლილება შენახულია და audit log განახლდა.")
                st.rerun()

    st.divider()
    st.subheader("ზარალის ანგარიში")
    audit_df = load_audit_log()
    total_shrinkage_value = float(audit_df["Shrinkage_Value"].sum()) if not audit_df.empty else 0.0
    st.metric("ჯამური ზარალი (Cost საფუძველზე)", f"{total_shrinkage_value:.2f}")

    if not audit_df.empty:
        st.dataframe(
            audit_df[["Date", "Product", "Old Stock", "New Stock", "Difference", "Reason", "Shrinkage_Value"]],
            use_container_width=True
        )
    else:
        st.info("Audit ჩანაწერები ჯერ არ არსებობს.")

elif page == "🔗 RS.GE ინტეგრაცია":
    st.title("🔗 RS.GE ინტეგრაცია")
    st.write("RS.GE ინფოსიმულაცია და ინტეგრაციის მაგალითი")
    for invoice in st.session_state.rs_invoices:
        st.write(f"**{invoice['id']}** — პროდუქტი: {invoice['product']}, რაოდენობა: {invoice['qty']}, ფასი: {invoice['price']}")
