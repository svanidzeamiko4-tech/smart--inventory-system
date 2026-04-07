import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import os
import random

st.set_page_config(page_title="ERP Smart System", layout="wide")
FILE_NAME = "Product.csv.txt"
AUDIT_LOG_FILE = "audit_log.csv"
SALES_LOG_FILE = "sales_log.csv"

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


def append_sales_log(sale_date, store, product, qty, selling_price, cost_price):
    revenue = float(qty) * float(selling_price)
    profit = float(qty) * (float(selling_price) - float(cost_price))
    sale_ts = pd.to_datetime(sale_date)
    log_row = {
        "Timestamp": sale_ts.strftime("%Y-%m-%d %H:%M:%S"),
        "Date": sale_ts.strftime("%Y-%m-%d"),
        "Store": str(store),
        "Product": str(product),
        "Qty": int(qty),
        "Selling_Price": float(selling_price),
        "Cost_Price": float(cost_price),
        "Revenue": revenue,
        "Profit": profit,
    }
    log_df = pd.DataFrame([log_row])
    write_header = not os.path.exists(SALES_LOG_FILE)
    log_df.to_csv(SALES_LOG_FILE, mode="a", header=write_header, index=False)


def load_sales_log():
    if os.path.exists(SALES_LOG_FILE):
        sales_df = pd.read_csv(SALES_LOG_FILE)
        required_cols = ["Timestamp", "Date", "Store", "Product", "Qty", "Selling_Price", "Cost_Price", "Revenue", "Profit"]
        for col in required_cols:
            if col not in sales_df.columns:
                sales_df[col] = 0 if col in ["Qty", "Selling_Price", "Cost_Price", "Revenue", "Profit"] else ""

        # Backward compatibility: old logs may only have Date without time.
        if "Timestamp" in sales_df.columns:
            sales_df["Timestamp"] = pd.to_datetime(sales_df["Timestamp"], errors="coerce")
        else:
            sales_df["Timestamp"] = pd.NaT

        fallback_ts = pd.to_datetime(sales_df["Date"], errors="coerce")
        sales_df["Timestamp"] = sales_df["Timestamp"].fillna(fallback_ts)
        sales_df["Date"] = sales_df["Timestamp"].dt.date

        for numeric_col in ["Qty", "Selling_Price", "Cost_Price", "Revenue", "Profit"]:
            sales_df[numeric_col] = pd.to_numeric(sales_df[numeric_col], errors="coerce").fillna(0)
        return sales_df.dropna(subset=["Timestamp"])
    return pd.DataFrame(columns=["Timestamp", "Date", "Store", "Product", "Qty", "Selling_Price", "Cost_Price", "Revenue", "Profit"])


def reset_system_data():
    for file_path in [FILE_NAME, SALES_LOG_FILE]:
        if os.path.exists(file_path):
            os.remove(file_path)


def generate_month_sales_simulation(current_df, transactions=10000):
    stores = ["Gldani_Branch", "Vake_Branch", "Saburtalo_Branch", "Didube_Branch"]
    products = [
        ("Apple", 1.20, 2.10),
        ("Banana", 0.80, 1.60),
        ("Milk", 1.10, 1.95),
        ("Bread", 0.70, 1.30),
        ("Rice", 1.50, 2.40),
        ("Pasta", 1.00, 1.90),
        ("Cheese", 2.20, 3.80),
        ("Yogurt", 0.90, 1.70),
        ("Eggs", 1.60, 2.70),
        ("Chicken", 3.20, 4.90),
        ("Beef", 4.20, 6.20),
        ("Potato", 0.50, 1.10),
        ("Tomato", 0.90, 1.80),
        ("Onion", 0.40, 0.95),
        ("Orange Juice", 1.70, 2.90),
    ]

    expiry_base = datetime.now() + timedelta(days=45)
    inventory_rows = []
    for store in stores:
        for product_name, cost_price, selling_price in products:
            inventory_rows.append(
                {
                    "Store_Name": store,
                    "Product_Name": product_name,
                    "Current_Stock": 1200,
                    "Cost_Price": cost_price,
                    "Selling_Price": selling_price,
                    "Price": selling_price,
                    "Sales_Day1": 0,
                    "Sales_Day2": 0,
                    "Sales_Day3": 0,
                    "Sales_Day4": 0,
                    "Sales_Day5": 0,
                    "Sales_Day6": 0,
                    "Sales_Day7": 0,
                    "Expiry_Date": pd.to_datetime(expiry_base),
                }
            )
    inventory_df = pd.DataFrame(inventory_rows)
    inventory_df = ensure_data_structure(inventory_df)
    inventory_df = recalc_metrics(inventory_df)
    save_data(inventory_df)

    now = datetime.now()
    start_dt = now - timedelta(days=30)
    sales_rows = []
    for _ in range(transactions):
        store = random.choice(stores)
        product_name, cost_price, selling_price = random.choice(products)
        qty = random.randint(1, 3)
        seconds_offset = random.randint(0, int((now - start_dt).total_seconds()))
        sale_ts = start_dt + timedelta(seconds=seconds_offset)
        revenue = qty * selling_price
        profit = qty * (selling_price - cost_price)
        sales_rows.append(
            {
                "Timestamp": sale_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "Date": sale_ts.strftime("%Y-%m-%d"),
                "Store": store,
                "Product": product_name,
                "Qty": qty,
                "Selling_Price": selling_price,
                "Cost_Price": cost_price,
                "Revenue": revenue,
                "Profit": profit,
            }
        )
    simulated_sales_df = pd.DataFrame(sales_rows)
    simulated_sales_df.to_csv(SALES_LOG_FILE, index=False)
    return inventory_df


def recalc_metrics(df):
    required_base_cols = ["Store_Name", "Product_Name", "Current_Stock", "Cost_Price", "Selling_Price", "Expiry_Date"]
    for col in required_base_cols:
        if col not in df.columns:
            if col in ["Current_Stock", "Cost_Price", "Selling_Price"]:
                df[col] = 0
            elif col == "Expiry_Date":
                df[col] = pd.NaT
            else:
                df[col] = ""

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
    [
        "🏠 მთავარი პანელი",
        "📦 პროდუქციის სია",
        "📥 საქონლის მიღება",
        "🔍 ინვენტარიზაცია",
        "📈 დეტალური ანალიტიკა",
        "📊 ანგარიშები",
    ]
)

st.sidebar.markdown("---")
st.sidebar.subheader("🚀 საჩვენებელი რეჟიმი")
if st.sidebar.button("10,000 გაყიდვის გენერირება (სიმულაცია)", use_container_width=True):
    st.session_state.df = generate_month_sales_simulation(st.session_state.df, transactions=10000)
    st.session_state.daily_profit = 0.0
    st.session_state.profit_date = datetime.now().strftime("%Y-%m-%d")
    st.rerun()

if st.sidebar.button("🗑️ სისტემის გასუფთავება", use_container_width=True):
    reset_system_data()
    st.session_state.df = pd.DataFrame()
    st.session_state.daily_profit = 0.0
    st.session_state.profit_date = datetime.now().strftime("%Y-%m-%d")
    st.rerun()

# --- გვერდი 1: DASHBOARD ---
if page == "🏠 მთავარი პანელი":
    st.title("🏠 მთავარი პანელი")
    st.caption("მარაგებისა და მომგებიანობის მოკლე მიმოხილვა.")

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

    # High-level metrics only.
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("📦 სულ პროდუქცია", len(df))
    c2.metric("📉 დაბალი ნაშთის გაფრთხილება", len(df[df["დარჩენილი დღე"] < 3]))
    c3.metric("⚠️ ვადის გაფრთხილება", len(expiring_soon))
    c4.metric("💰 დღიური მოგება", f"{st.session_state.daily_profit:.2f}")

elif page == "📦 პროდუქციის სია":
    st.title("📦 პროდუქციის სია")
    st.caption("იპოვე პროდუქტი სწრაფად და განახორციელე გაყიდვა.")
    search_term = st.text_input("🔎 პროდუქტის ძიება", placeholder="შეიყვანე პროდუქტის ან ფილიალის სახელი...")

    filtered_df = df.copy()
    if search_term.strip():
        mask = (
            filtered_df["Product_Name"].astype(str).str.contains(search_term, case=False, na=False)
            | filtered_df["Store_Name"].astype(str).str.contains(search_term, case=False, na=False)
        )
        filtered_df = filtered_df[mask]

    header_cols = st.columns([2, 2, 1, 1, 1, 1, 1, 1])
    for col, title in zip(header_cols, [
        "ფილიალი", "პროდუქტი", "ნაშთი", "თვითღირებულება", "გასაყიდი ფასი", "დარჩენილი დღე", "ვადა (დღე)", "მოქმედება"
    ]):
        col.markdown(f"**{title}**")

    for index, row in filtered_df.iterrows():
        c0, c1, c2, c3, c4, c5, c6, c7 = st.columns([2, 2, 1, 1, 1, 1, 1, 1])
        c0.write(row["Store_Name"])
        c1.write(row["Product_Name"])
        c2.write(int(row["Current_Stock"]))
        c3.write(f"{float(row.get('Cost_Price', 0)):.2f}")
        c4.write(f"{float(row.get('Selling_Price', 0)):.2f}")
        c5.write(row["დარჩენილი დღე"])
        c6.write(row["დღე ვადის გასვლამდე"])
        can_sell = row["Current_Stock"] > 0
        sell_key = f"sell_btn_{int(index)}_{str(row.get('Product_Name', ''))}_{str(row.get('Store_Name', ''))}"
        if c7.button("გაყიდვა", key=sell_key, disabled=not can_sell):
            unit_profit = float(row.get("Selling_Price", 0)) - float(row.get("Cost_Price", 0))
            st.session_state.daily_profit += unit_profit
            append_sales_log(
                sale_date=datetime.now(),
                store=row.get("Store_Name", ""),
                product=row.get("Product_Name", ""),
                qty=1,
                selling_price=row.get("Selling_Price", 0),
                cost_price=row.get("Cost_Price", 0),
            )
            df.at[index, "Current_Stock"] = max(0, int(row["Current_Stock"]) - 1)
            df = recalc_metrics(df)
            save_data(df)
            st.session_state.df = df
            st.rerun()
        if not can_sell:
            c7.caption("ამოიწურა")

    if filtered_df.empty:
        st.info("მოცემული ძიებით პროდუქტი ვერ მოიძებნა.")

elif page == "📥 საქონლის მიღება":
    st.title("📥 საქონლის მიღება")
    st.caption("ახალი საქონლის მიღება ზედნადების ფორმატში.")

    with st.container(border=True):
        st.markdown("### 🧾 ზედნადების შევსება")
        st.markdown("შეავსე ზედნადების ინფორმაცია და შეინახე პროდუქტის ჩანაწერი.")

    with st.form("manual_add"):
        col1, col2 = st.columns(2, gap="large")
        f_store = col1.selectbox("მაღაზია", df["Store_Name"].unique() if not df.empty else ["Gldani_Branch"])
        f_name = col2.text_input("პროდუქტი")
        f_qty = col1.number_input("რაოდენობა", min_value=1)
        f_cost_price = col1.number_input("თვითღირებულება", min_value=0.0, format="%.2f")
        f_selling_price = col2.number_input("გასაყიდი ფასი", min_value=0.0, format="%.2f")
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

elif page == "🔍 ინვენტარიზაცია":
    st.title("🔍 ინვენტარიზაცია")
    st.caption("ნაშთის კორექტირება მიზეზის მითითებით ბუღალტრული კონტროლისთვის.")

    if df.empty:
        st.info("პროდუქტები არ არის დამატებული.")
    else:
        options = {
            f"{row['Store_Name']} | {row['Product_Name']} (ნაშთი: {int(row['Current_Stock'])})": idx
            for idx, row in df.iterrows()
        }
        reasons = ["დაზიანებული", "ვადაგასული", "ინვენტარიზაციის აცდენა", "სხვა"]

        with st.form("inventory_audit_form"):
            selected_label = st.selectbox("აირჩიე პროდუქტი", list(options.keys()))
            selected_idx = options[selected_label]
            current_stock = int(df.at[selected_idx, "Current_Stock"])
            new_stock = st.number_input("ახალი ნაშთი", min_value=0, value=current_stock, step=1)
            reason = st.selectbox("მიზეზი", reasons)
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
                st.success("ინვენტარის ცვლილება შენახულია და აუდიტის ჟურნალი განახლდა.")
                st.rerun()

elif page == "📈 დეტალური ანალიტიკა":
    st.title("📈 დეტალური ანალიტიკა")
    st.caption("შემოსავლის, მოგებისა და გაყიდვების მოცულობის ანალიზი პერიოდებისა და ფილიალების მიხედვით.")

    df = st.session_state.df
    sales_df = load_sales_log()

    if "analytics_start" not in st.session_state:
        st.session_state.analytics_start = (datetime.now().date() - timedelta(days=6))
    if "analytics_end" not in st.session_state:
        st.session_state.analytics_end = datetime.now().date()

    q1, q2, q3 = st.columns(3)
    if q1.button("ბოლო 7 დღე", use_container_width=True):
        st.session_state.analytics_start = datetime.now().date() - timedelta(days=6)
        st.session_state.analytics_end = datetime.now().date()
    if q2.button("ბოლო 15 დღე", use_container_width=True):
        st.session_state.analytics_start = datetime.now().date() - timedelta(days=14)
        st.session_state.analytics_end = datetime.now().date()
    if q3.button("ბოლო 30 დღე", use_container_width=True):
        st.session_state.analytics_start = datetime.now().date() - timedelta(days=29)
        st.session_state.analytics_end = datetime.now().date()

    f1, f2 = st.columns([2, 1])
    selected_range = f1.date_input(
        "თარიღების დიაპაზონი",
        value=(st.session_state.analytics_start, st.session_state.analytics_end),
    )
    if isinstance(selected_range, tuple) and len(selected_range) == 2:
        start_date, end_date = selected_range
        st.session_state.analytics_start = start_date
        st.session_state.analytics_end = end_date
    else:
        start_date = st.session_state.analytics_start
        end_date = st.session_state.analytics_end

    store_options = ["ყველა ფილიალი"] + sorted(df["Store_Name"].astype(str).unique().tolist()) if not df.empty else ["ყველა ფილიალი"]
    selected_store = f2.selectbox("ფილიალის ფილტრი", store_options)

    if sales_df.empty:
        st.info("ჯერ გაყიდვები არ ფიქსირდება")
    else:
        sales_df["DateOnly"] = sales_df["Timestamp"].dt.date
        filtered_sales = sales_df[
            (sales_df["DateOnly"] >= start_date) &
            (sales_df["DateOnly"] <= end_date)
        ]
        if selected_store != "ყველა ფილიალი":
            filtered_sales = filtered_sales[filtered_sales["Store"] == selected_store]

        m1, m2, m3 = st.columns(3)
        m1.metric("💵 მთლიანი შემოსავალი", f"{filtered_sales['Revenue'].sum():.2f}")
        m2.metric("📈 მთლიანი მოგება", f"{filtered_sales['Profit'].sum():.2f}")
        m3.metric("🧾 გაყიდული ერთეულები", int(filtered_sales["Qty"].sum()))

        if filtered_sales.empty:
            st.info("ჯერ გაყიდვები არ ფიქსირდება")
        else:
            st.divider()
            st.subheader("დღიური გაყიდვების ტრენდი")
            daily_sales = filtered_sales.groupby("DateOnly", as_index=False)["Revenue"].sum()
            daily_sales = daily_sales.rename(columns={"DateOnly": "Date"})
            st.line_chart(daily_sales.set_index("Date")["Revenue"])

            c_chart1, c_chart2 = st.columns(2)
            with c_chart1:
                st.subheader("გაყიდვები ფილიალების მიხედვით")
                sales_by_store = filtered_sales.groupby("Store", as_index=False)["Revenue"].sum()
                st.bar_chart(sales_by_store.set_index("Store")["Revenue"])
            with c_chart2:
                st.subheader("მოგება ფილიალების მიხედვით")
                profit_by_store = filtered_sales.groupby("Store", as_index=False)["Profit"].sum()
                st.bar_chart(profit_by_store.set_index("Store")["Profit"])

elif page == "📊 ანგარიშები":
    st.title("📊 ანგარიშები")
    st.caption("მოწესრიგებული ანგარიშები მენეჯმენტისა და ბუღალტრული კონტროლისთვის.")

    report_tab1, report_tab2 = st.tabs(["📉 ზარალის ანგარიში", "📦 მარაგების მიმოხილვა"])
    audit_df = load_audit_log()

    with report_tab1:
        total_shrinkage_value = float(audit_df["Shrinkage_Value"].sum()) if not audit_df.empty else 0.0
        total_shrinkage_units = int((-audit_df["Difference"]).clip(lower=0).sum()) if not audit_df.empty else 0
        c1, c2 = st.columns(2)
        c1.metric("ჯამური ზარალი (თვითღირებულებით)", f"{total_shrinkage_value:.2f}")
        c2.metric("დაკარგული ერთეულები", total_shrinkage_units)
        if not audit_df.empty:
            audit_display = audit_df[["Date", "Product", "Old Stock", "New Stock", "Difference", "Reason", "Shrinkage_Value"]].rename(
                columns={
                    "Date": "თარიღი",
                    "Product": "პროდუქტი",
                    "Old Stock": "ძველი ნაშთი",
                    "New Stock": "ახალი ნაშთი",
                    "Difference": "სხვაობა",
                    "Reason": "მიზეზი",
                    "Shrinkage_Value": "ზარალის ღირებულება",
                }
            )
            st.dataframe(
                audit_display,
                use_container_width=True
            )
        else:
            st.info("აუდიტის ჩანაწერები ჯერ არ არსებობს.")

    with report_tab2:
        st.metric("მარაგის ღირებულება (თვითღირებულებით)", f"{(df['Current_Stock'] * df['Cost_Price']).sum():.2f}" if not df.empty else "0.00")
        if not df.empty:
            st.bar_chart(df.set_index("Product_Name")["Current_Stock"])
            overview_cols = ["Store_Name", "Product_Name", "Current_Stock", "Cost_Price", "Selling_Price", "დღე ვადის გასვლამდე"]
            overview_display = df[overview_cols].rename(
                columns={
                    "Store_Name": "ფილიალი",
                    "Product_Name": "პროდუქტი",
                    "Current_Stock": "ნაშთი",
                    "Cost_Price": "თვითღირებულება",
                    "Selling_Price": "გასაყიდი ფასი",
                }
            )
            st.dataframe(overview_display, use_container_width=True)
        else:
            st.info("პროდუქტები არ არის დამატებული.")
