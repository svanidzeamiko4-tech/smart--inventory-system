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

if 'df' not in st.session_state:
    st.session_state.df = load_data()

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
    
    if st.sidebar.button("🕒 დღის დასრულება (გაყიდვა)"):
        for index, row in df.iterrows():
            avg = row["საშ. დღიური გაყიდვა"] if row["საშ. დღიური გაყიდვა"] > 0 else 5
            sale = round(avg * random.uniform(0.7, 1.3))
            df.at[index, "Current_Stock"] = max(0, row["Current_Stock"] - sale)
        df.to_csv(FILE_NAME, index=False)
        st.rerun()

    # ინდიკატორები
    c1, c2, c3 = st.columns(3)
    c1.metric("📦 სულ პროდუქცია", len(df))
    c2.metric("⚠️ კრიტიკული ვადა", len(df[df["დღე ვადის გასვლამდე"] < 5]))
    c3.metric("📉 დაბალი მარაგი", len(df[df["დარჩენილი დღე"] < 3]))

    st.divider()
    st.subheader("📋 დეტალური ნაშთები")
    st.dataframe(df[["Store_Name", "Product_Name", "Current_Stock", "დარჩენილი დღე", "დღე ვადის გასვლამდე"]], use_container_width=True)
    st.bar_chart(df.set_index("Product_Name")["Current_Stock"])

# --- გვერდი 2: მარაგების დამატება (ხელით) ---
elif page == "📥 მარაგების დამატება":
    st.title("📥 პროდუქციის ხელით აღრიცხვა")
    with st.form("manual_add"):
        col1, col2 = st.columns(2)
        f_store = col1.selectbox("მაღაზია", df["Store_Name"].unique() if not df.empty else ["Gldani_Branch"])
        f_name = col2.text_input("პროდუქტი")
        f_qty = col1.number_input("რაოდენობა", min_value=1)
        f_expiry = col2.date_input("ვადა")