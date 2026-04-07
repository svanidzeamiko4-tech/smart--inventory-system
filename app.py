import pandas as pd
import streamlit as st
from datetime import datetime
import numpy as np
import random
import os

st.set_page_config(page_title="ERP Smart System", layout="wide")
FILE_NAME = "Product.csv.txt"

# 1. მონაცემების მართვა
def load_data():
    if os.path.exists(FILE_NAME):
        df = pd.read_csv(FILE_NAME)
        df['Expiry_Date'] = pd.to_datetime(df['Expiry_Date'])
        return df
    return pd.DataFrame()


def save_data(df):
    df.to_csv(FILE_NAME, index=False)


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

# Ensure metrics exist after loading
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
page = st.sidebar.radio("გადადი გვერდზე:", ["📊 Dashboard & ანალიტიკა", "📥 მარაგების დამატება", "🔗 RS.GE ინტეგრაცია"])

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
    c1, c2, c3 = st.columns(3)
    c1.metric("📦 სულ პროდუქცია", len(df))
    c2.metric("⚠️ კრიტიკული ვადა", len(df[df["დღე ვადის გასვლამდე"] < 5]))
    c3.metric("📉 დაბალი მარაგი", len(df[df["დარჩენილი დღე"] < 3]))

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
        f_price = col2.number_input("ფასი", min_value=0.0, format="%.2f")
        f_expiry = col2.date_input("ვადა")
        submitted = st.form_submit_button("შენახვა")

    if submitted:
        new_row = {
            "Store_Name": f_store,
            "Product_Name": f_name,
            "Current_Stock": int(f_qty),
            "Price": float(f_price),
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

elif page == "🔗 RS.GE ინტეგრაცია":
    st.title("🔗 RS.GE ინტეგრაცია")
    st.write("RS.GE ინფოსიმულაცია და ინტეგრაციის მაგალითი")
    for invoice in st.session_state.rs_invoices:
        st.write(f"**{invoice['id']}** — პროდუქტი: {invoice['product']}, რაოდენობა: {invoice['qty']}, ფასი: {invoice['price']}")
