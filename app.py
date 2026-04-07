import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import os
import random
import math
import json
import hashlib

st.set_page_config(page_title="ERP Smart System", layout="wide")
FILE_NAME = "Product.csv.txt"
AUDIT_LOG_FILE = "audit_log.csv"
SALES_LOG_FILE = "sales_log.csv"
USERS_FILE = "users.csv"
DISTRIBUTOR_MAP_FILE = "distributor_mapping.json"
MAPPING_FILE = "mapping.csv"
BALANCE_FILE = "balances.csv"
STORE_NAME_MAP = {
    "Gldani_Branch": "გლდანის ფილიალი",
    "Vake_Branch": "ვაკის ფილიალი",
    "Saburtalo_Branch": "საბურთალოს ფილიალი",
    "Didube_Branch": "დიდუბის ფილიალი",
}

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


def hash_password(raw_password):
    return hashlib.sha256(raw_password.encode("utf-8")).hexdigest()


def ensure_auth_files():
    if not os.path.exists(USERS_FILE):
        default_users_df = pd.DataFrame(
            [
                {
                    "username": "super_admin",
                    "password_hash": hash_password("admin123"),
                    "role": "Super_Admin",
                    "company": "Global",
                    "store": "",
                    "allowed_stores": "",
                    "allowed_products": "",
                },
                {
                    "username": "manager_gldani",
                    "password_hash": hash_password("manager123"),
                    "role": "Store_Manager",
                    "company": "Ifkli",
                    "store": "გლდანის ფილიალი",
                    "assigned_branch": "გლდანის ფილიალი",
                    "allowed_stores": "გლდანის ფილიალი",
                    "allowed_products": "",
                },
                {
                    "username": "ifkli_admin",
                    "password_hash": hash_password("ifkli123"),
                    "role": "Company_Admin",
                    "company": "Ifkli",
                    "store": "",
                    "assigned_branch": "",
                    "allowed_stores": "გლდანის ფილიალი|ვაკის ფილიალი",
                    "allowed_products": "Apple|Banana|Milk|Bread|Rice",
                },
                {
                    "username": "distributor_a",
                    "password_hash": hash_password("dist123"),
                    "role": "Distributor",
                    "company": "Ifkli",
                    "store": "",
                    "assigned_branch": "",
                    "allowed_stores": "გლდანის ფილიალი|ვაკის ფილიალი",
                    "allowed_products": "Apple|Banana|Milk|Bread|Rice",
                },
            ]
        )
        default_users_df.to_csv(USERS_FILE, index=False)

    if not os.path.exists(DISTRIBUTOR_MAP_FILE):
        default_map = {
            "distributor_a": {
                "company": "Supplier_A",
                "stores": ["გლდანის ფილიალი", "ვაკის ფილიალი"],
                "products": ["Apple", "Banana", "Milk", "Bread", "Rice"]
            }
        }
        with open(DISTRIBUTOR_MAP_FILE, "w", encoding="utf-8") as f:
            json.dump(default_map, f, ensure_ascii=False, indent=2)

    if not os.path.exists(MAPPING_FILE):
        default_mapping = pd.DataFrame(
            [
                {"mapping_type": "user_company", "key": "super_admin", "value": "Global"},
                {"mapping_type": "user_company", "key": "admin", "value": "Global"},
                {"mapping_type": "user_company", "key": "ifkli_admin", "value": "Ifkli"},
                {"mapping_type": "user_company", "key": "distributor_a", "value": "Ifkli"},
                {"mapping_type": "company_product", "key": "Ifkli", "value": "Apple"},
                {"mapping_type": "company_product", "key": "Ifkli", "value": "Banana"},
                {"mapping_type": "company_product", "key": "Ifkli", "value": "Milk"},
                {"mapping_type": "company_product", "key": "Ifkli", "value": "Bread"},
                {"mapping_type": "company_product", "key": "Ifkli", "value": "Rice"},
                {"mapping_type": "distributor_store", "key": "distributor_a", "value": "გლდანის ფილიალი"},
                {"mapping_type": "distributor_store", "key": "distributor_a", "value": "ვაკის ფილიალი"},
            ]
        )
        default_mapping.to_csv(MAPPING_FILE, index=False)

    if not os.path.exists(BALANCE_FILE):
        default_balances = pd.DataFrame(
            [
                {"company": "Ifkli", "store": "გლდანის ფილიალი", "balance": -1250.0},
                {"company": "Ifkli", "store": "ვაკის ფილიალი", "balance": 320.0},
            ]
        )
        default_balances.to_csv(BALANCE_FILE, index=False)


def load_mapping():
    ensure_auth_files()
    mapping_df = pd.read_csv(MAPPING_FILE, dtype=str).fillna("")
    user_company = {}
    company_products = {}
    distributor_stores = {}

    for _, row in mapping_df.iterrows():
        m_type = row.get("mapping_type", "")
        key = row.get("key", "")
        value = row.get("value", "")
        if not key or not value:
            continue
        if m_type == "user_company":
            user_company[key] = value
        elif m_type == "company_product":
            company_products.setdefault(key, set()).add(value)
        elif m_type == "distributor_store":
            distributor_stores.setdefault(key, set()).add(value)

    return {
        "df": mapping_df,
        "user_company": user_company,
        "company_products": company_products,
        "distributor_stores": distributor_stores,
    }


def load_balances_for_company(company_name):
    ensure_auth_files()
    balances_df = pd.read_csv(BALANCE_FILE, dtype={"company": str, "store": str, "balance": float}).fillna("")
    balances_df["balance"] = pd.to_numeric(balances_df["balance"], errors="coerce").fillna(0.0)
    return balances_df[balances_df["company"].astype(str) == str(company_name)].copy()


def load_users():
    ensure_auth_files()
    users_df = pd.read_csv(USERS_FILE, dtype=str).fillna("")

    # Safety fallback: always guarantee default admin access.
    if "username" not in users_df.columns:
        users_df["username"] = ""
    if "password_hash" not in users_df.columns:
        users_df["password_hash"] = ""
    if "role" not in users_df.columns:
        users_df["role"] = ""
    if "company" not in users_df.columns:
        users_df["company"] = ""
    if "store" not in users_df.columns:
        users_df["store"] = ""
    if "assigned_branch" not in users_df.columns:
        users_df["assigned_branch"] = users_df["store"] if "store" in users_df.columns else ""
    if "allowed_stores" not in users_df.columns:
        users_df["allowed_stores"] = ""
    if "allowed_products" not in users_df.columns:
        users_df["allowed_products"] = ""

    admin_exists = (users_df["username"].astype(str) == "admin").any()
    if not admin_exists:
        admin_row = pd.DataFrame(
            [
                {
                    "username": "admin",
                    "password_hash": hash_password("admin123"),
                    "role": "Admin",
                    "company": "Global",
                    "store": "",
                    "assigned_branch": "",
                    "allowed_stores": "",
                    "allowed_products": "",
                }
            ]
        )
        users_df = pd.concat([users_df, admin_row], ignore_index=True)
        users_df.to_csv(USERS_FILE, index=False)

    users = users_df.to_dict("records")
    for user in users:
        # Backward compatibility if old Admin role exists
        if user.get("role") == "Admin":
            user["role"] = "Super_Admin"
    return users


def load_distributor_map():
    ensure_auth_files()
    with open(DISTRIBUTOR_MAP_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def authenticate_user(username, password):
    users = load_users()
    for user in users:
        if user.get("username") == username and user.get("password_hash") == hash_password(password):
            return user
    return None


def apply_role_filters(df, sales_df, auth_user, mapping_data):
    role = auth_user.get("role", "")
    username = auth_user.get("username", "")
    filtered_df = df.copy()
    filtered_sales = sales_df.copy()
    user_company = mapping_data.get("user_company", {})
    company_products = mapping_data.get("company_products", {})
    distributor_stores = mapping_data.get("distributor_stores", {})
    company = user_company.get(username, auth_user.get("company", ""))

    if role == "Super_Admin":
        return filtered_df, filtered_sales

    if role == "Store_Manager":
        store_name = auth_user.get("assigned_branch", "") or auth_user.get("store", "")
        filtered_df = filtered_df[filtered_df["Store_Name"].astype(str) == str(store_name)]
        if not filtered_sales.empty:
            filtered_sales = filtered_sales[filtered_sales["Store"].astype(str) == str(store_name)]
        return filtered_df, filtered_sales

    if role == "Distributor":
        allowed_stores = set(distributor_stores.get(username, set()))
        allowed_products = set(company_products.get(company, set()))
        filtered_df = filtered_df[
            filtered_df["Store_Name"].astype(str).isin(allowed_stores)
            & filtered_df["Product_Name"].astype(str).isin(allowed_products)
        ]
        if not filtered_sales.empty:
            filtered_sales = filtered_sales[
                filtered_sales["Store"].astype(str).isin(allowed_stores)
                & filtered_sales["Product"].astype(str).isin(allowed_products)
            ]
        return filtered_df, filtered_sales

    if role == "Company_Admin":
        allowed_products = set(company_products.get(company, set()))
        if allowed_products:
            filtered_df = filtered_df[filtered_df["Product_Name"].astype(str).isin(allowed_products)]
            if not filtered_sales.empty:
                filtered_sales = filtered_sales[filtered_sales["Product"].astype(str).isin(allowed_products)]
        return filtered_df, filtered_sales

    return filtered_df.iloc[0:0], filtered_sales.iloc[0:0]


def save_users(users):
    pd.DataFrame(users).to_csv(USERS_FILE, index=False)


def get_role_scope(auth_user, mapping_data):
    role = auth_user.get("role", "")
    username = auth_user.get("username", "")
    user_company = mapping_data.get("user_company", {})
    company_products = mapping_data.get("company_products", {})
    distributor_stores = mapping_data.get("distributor_stores", {})
    company = user_company.get(username, auth_user.get("company", ""))

    if role == "Super_Admin":
        return {"role": role, "stores": None, "products": None}
    if role == "Store_Manager":
        branch = auth_user.get("assigned_branch", "") or auth_user.get("store", "")
        return {"role": role, "stores": set([branch]) if branch else set(), "products": None}
    if role == "Company_Admin":
        return {"role": role, "stores": None, "products": set(company_products.get(company, set()))}
    if role == "Distributor":
        return {
            "role": role,
            "stores": set(distributor_stores.get(username, set())),
            "products": set(company_products.get(company, set())),
        }
    return {"role": role, "stores": set(), "products": set()}


def apply_global_data_lock(df, sales_df, audit_df, scope):
    if scope["role"] == "Super_Admin":
        return df, sales_df, audit_df

    locked_df = df.copy()
    locked_sales = sales_df.copy()
    locked_audit = audit_df.copy()

    stores = scope.get("stores")
    products = scope.get("products")

    if stores is not None and len(stores) > 0:
        locked_df = locked_df[locked_df["Store_Name"].astype(str).isin(stores)]
        if not locked_sales.empty:
            locked_sales = locked_sales[locked_sales["Store"].astype(str).isin(stores)]
        if not locked_audit.empty and "Store" in locked_audit.columns:
            locked_audit = locked_audit[locked_audit["Store"].astype(str).isin(stores)]

    if products is not None and len(products) > 0:
        locked_df = locked_df[locked_df["Product_Name"].astype(str).isin(products)]
        if not locked_sales.empty:
            locked_sales = locked_sales[locked_sales["Product"].astype(str).isin(products)]
        if not locked_audit.empty and "Product" in locked_audit.columns:
            locked_audit = locked_audit[locked_audit["Product"].astype(str).isin(products)]

    return locked_df, locked_sales, locked_audit


def get_default_page_for_role(role):
    if role == "Super_Admin":
        return "🏢 კომპანიის მართვა"
    if role == "Company_Admin":
        return "🤝 დისტრიბუტორების მართვა"
    if role == "Distributor":
        return "🚚 აუცილებელი მიწოდებები"
    if role == "Store_Manager":
        return "🏠 მაღაზიის პანელი"
    return "🏢 კომპანიის მართვა"


def save_data(df):
    df.to_csv(FILE_NAME, index=False)


def append_audit_log(store, product, old_stock, new_stock, difference, reason, cost_price):
    log_row = {
        "Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Store": str(store),
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
        required_cols = ["Date", "Store", "Product", "Old Stock", "New Stock", "Difference", "Reason", "Cost Price"]
        for col in required_cols:
            if col not in log_df.columns:
                log_df[col] = 0 if col in ["Old Stock", "New Stock", "Difference", "Cost Price"] else ""
        log_df["Difference"] = pd.to_numeric(log_df["Difference"], errors="coerce").fillna(0)
        log_df["Cost Price"] = pd.to_numeric(log_df["Cost Price"], errors="coerce").fillna(0.0)
        log_df["Shrinkage_Value"] = (-log_df["Difference"]).clip(lower=0) * log_df["Cost Price"]
        return log_df
    return pd.DataFrame(columns=["Date", "Store", "Product", "Old Stock", "New Stock", "Difference", "Reason", "Cost Price", "Shrinkage_Value"])


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

        # One-time migration for historical English store names.
        original_store = sales_df["Store"].astype(str)
        sales_df["Store"] = original_store.replace(STORE_NAME_MAP)
        if not sales_df["Store"].astype(str).equals(original_store):
            sales_df.to_csv(SALES_LOG_FILE, index=False)

        for numeric_col in ["Qty", "Selling_Price", "Cost_Price", "Revenue", "Profit"]:
            sales_df[numeric_col] = pd.to_numeric(sales_df[numeric_col], errors="coerce").fillna(0)
        return sales_df.dropna(subset=["Timestamp"])
    return pd.DataFrame(columns=["Timestamp", "Date", "Store", "Product", "Qty", "Selling_Price", "Cost_Price", "Revenue", "Profit"])


def reset_system_data():
    for file_path in [FILE_NAME, SALES_LOG_FILE]:
        if os.path.exists(file_path):
            os.remove(file_path)


def generate_month_sales_simulation(current_df, transactions=10000):
    stores = ["გლდანის ფილიალი", "ვაკის ფილიალი", "საბურთალოს ფილიალი", "დიდუბის ფილიალი"]
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
    now = datetime.now()
    peak_days = {
        (now.date() - timedelta(days=random.randint(1, 28))).isoformat()
        for _ in range(3)
    }
    weighted_days = []
    for i in range(30):
        day = (now.date() - timedelta(days=i)).isoformat()
        weight = 10 if day in peak_days else 1
        weighted_days.extend([day] * weight)
    sales_rows = []
    for _ in range(transactions):
        store = random.choice(stores)
        product_name, cost_price, selling_price = random.choice(products)
        qty = random.randint(1, 3)
        random_day = datetime.fromisoformat(random.choice(weighted_days))
        sale_ts = datetime.combine(random_day.date(), datetime.min.time()) + timedelta(seconds=random.randint(0, 86399))
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

    inventory_df = pd.DataFrame(inventory_rows)
    sold_by_item = simulated_sales_df.groupby(["Store", "Product"], as_index=False)["Qty"].sum().rename(
        columns={"Store": "Store_Name", "Product": "Product_Name", "Qty": "Sold_Qty"}
    )
    inventory_df = inventory_df.merge(sold_by_item, on=["Store_Name", "Product_Name"], how="left")
    inventory_df["Sold_Qty"] = inventory_df["Sold_Qty"].fillna(0)
    inventory_df["Current_Stock"] = (1200 - inventory_df["Sold_Qty"]).clip(lower=0).astype(int)
    inventory_df = inventory_df.drop(columns=["Sold_Qty"])

    # Stress Test: distribution delays on 3-4 products.
    disrupted_products = random.sample([p[0] for p in products], k=random.randint(3, 4))
    delay_mask = inventory_df["Product_Name"].isin(disrupted_products)
    inventory_df.loc[delay_mask, "Current_Stock"] = [random.choice([0, 1, 2]) for _ in range(delay_mask.sum())]

    # Stress Test: deliberate discrepancies between stock and sales history.
    discrepancy_indexes = random.sample(inventory_df.index.tolist(), k=min(10, len(inventory_df)))
    for idx in discrepancy_indexes:
        inventory_df.at[idx, "Current_Stock"] = max(
            0, int(inventory_df.at[idx, "Current_Stock"]) + random.choice([-80, -50, 40, 70])
        )

    inventory_df = ensure_data_structure(inventory_df)
    inventory_df = recalc_metrics(inventory_df)
    save_data(inventory_df)
    return inventory_df


def get_low_stock_alerts(df, threshold=5):
    if df.empty:
        return pd.DataFrame()
    return df[df["Current_Stock"] < threshold].copy()


def get_weekly_demand_stats(sales_df, product_name, store_name):
    if sales_df.empty:
        return 0.0, 0
    last_7_days = datetime.now() - timedelta(days=7)
    product_sales = sales_df[
        (sales_df["Timestamp"] >= last_7_days)
        & (sales_df["Product"].astype(str) == str(product_name))
        & (sales_df["Store"].astype(str) == str(store_name))
    ]
    total_qty_7d = int(product_sales["Qty"].sum()) if not product_sales.empty else 0
    avg_daily_qty = total_qty_7d / 7
    recommended_qty = int(math.ceil(avg_daily_qty * 7))
    return avg_daily_qty, recommended_qty


def get_restock_recommendation_qty(sales_df, product_name, store_name, current_stock):
    avg_daily_qty, weekly_need = get_weekly_demand_stats(sales_df, product_name, store_name)
    needed_qty = max(0, int(math.ceil(weekly_need - float(current_stock))))
    return avg_daily_qty, needed_qty


def compute_branch_performance(sales_df):
    if sales_df.empty:
        return pd.DataFrame(columns=["Store", "Actual", "Predicted", "PerformancePct", "DownPct"])

    now = datetime.now()
    last_7_start = now - timedelta(days=7)
    prev_7_start = now - timedelta(days=14)

    last_7 = sales_df[sales_df["Timestamp"] >= last_7_start]
    prev_7 = sales_df[(sales_df["Timestamp"] >= prev_7_start) & (sales_df["Timestamp"] < last_7_start)]

    actual = last_7.groupby("Store", as_index=False)["Revenue"].sum().rename(columns={"Revenue": "Actual"})
    predicted = prev_7.groupby("Store", as_index=False)["Revenue"].sum().rename(columns={"Revenue": "Predicted"})
    perf = pd.merge(actual, predicted, on="Store", how="outer").fillna(0)
    perf["Predicted"] = perf["Predicted"].replace(0, 1)
    perf["PerformancePct"] = (perf["Actual"] / perf["Predicted"] * 100).round(1)
    perf["DownPct"] = (100 - perf["PerformancePct"]).clip(lower=0).round(1)
    return perf


def get_underperforming_product(sales_df, store_name):
    if sales_df.empty:
        return None
    now = datetime.now()
    last_7_start = now - timedelta(days=7)
    prev_7_start = now - timedelta(days=14)
    last_7 = sales_df[(sales_df["Timestamp"] >= last_7_start) & (sales_df["Store"] == store_name)]
    prev_7 = sales_df[(sales_df["Timestamp"] >= prev_7_start) & (sales_df["Timestamp"] < last_7_start) & (sales_df["Store"] == store_name)]
    if last_7.empty or prev_7.empty:
        return None
    last_prod = last_7.groupby("Product", as_index=False)["Qty"].sum().rename(columns={"Qty": "LastQty"})
    prev_prod = prev_7.groupby("Product", as_index=False)["Qty"].sum().rename(columns={"Qty": "PrevQty"})
    merged = pd.merge(prev_prod, last_prod, on="Product", how="left").fillna(0)
    merged["Drop"] = merged["PrevQty"] - merged["LastQty"]
    top = merged.sort_values("Drop", ascending=False).iloc[0]
    if top["Drop"] > 0:
        return str(top["Product"])
    return None


def get_branches_with_no_sales_last_3h(sales_df, df):
    if df.empty:
        return []
    all_stores = sorted(df["Store_Name"].astype(str).unique().tolist())
    if sales_df.empty:
        return all_stores
    cutoff = datetime.now() - timedelta(hours=3)
    recent_stores = sales_df[sales_df["Timestamp"] >= cutoff]["Store"].astype(str).unique().tolist()
    return [store for store in all_stores if store not in recent_stores]


def detect_stock_discrepancy(df, sales_df, baseline_stock=1200, tolerance=15):
    if df.empty or sales_df.empty:
        return False
    sold = sales_df.groupby(["Store", "Product"], as_index=False)["Qty"].sum().rename(
        columns={"Store": "Store_Name", "Product": "Product_Name", "Qty": "Sold_Qty"}
    )
    merged = df[["Store_Name", "Product_Name", "Current_Stock"]].merge(
        sold, on=["Store_Name", "Product_Name"], how="left"
    )
    merged["Sold_Qty"] = merged["Sold_Qty"].fillna(0)
    merged["Expected_Stock"] = (baseline_stock - merged["Sold_Qty"]).clip(lower=0)
    merged["Delta"] = (merged["Current_Stock"] - merged["Expected_Stock"]).abs()
    return bool((merged["Delta"] > tolerance).any())


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

if "auth_user" not in st.session_state:
    st.session_state.auth_user = None

if st.session_state.auth_user is None:
    st.title("🔐 ავტორიზაცია")
    st.caption("შეიყვანე მომხმარებლის სახელი და პაროლი სისტემაში შესასვლელად.")
    with st.form("login_form"):
        username = st.text_input("მომხმარებელი")
        password = st.text_input("პაროლი", type="password")
        login_submitted = st.form_submit_button("შესვლა")
    if login_submitted:
        user = authenticate_user(username.strip(), password.strip())
        if user:
            st.session_state.auth_user = user
            st.session_state.pending_page = get_default_page_for_role(user.get("role", ""))
            st.rerun()
        else:
            st.error("არასწორი მომხმარებელი ან პაროლი.")
    st.stop()

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

auth_user = st.session_state.auth_user
sales_df_all = load_sales_log()
audit_df_all = load_audit_log()
mapping_data = load_mapping()
master_df = st.session_state.df
df, sales_df_all = apply_role_filters(master_df, sales_df_all, auth_user, mapping_data)
role_scope = get_role_scope(auth_user, mapping_data)
df, sales_df_all, audit_df_all = apply_global_data_lock(df, sales_df_all, audit_df_all, role_scope)

# 2. ანალიტიკური გათვლები
sales_cols = [f"Sales_Day{i}" for i in range(1, 8)]
for col in sales_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

df["საშ. დღიური გაყიდვა"] = df[sales_cols].mean(axis=1).round(1)
df["დარჩენილი დღე"] = (df["Current_Stock"] / df["საშ. დღიური გაყიდვა"].replace(0, 0.1)).round(1)
df["დღე ვადის გასვლამდე"] = (pd.to_datetime(df["Expiry_Date"]) - datetime.now()).dt.days

# --- მთავარი გვერდითა მენიუ ---
st.sidebar.title("🎛️ მართვის პანელი")
st.sidebar.caption(
    f"მომხმარებელი: {auth_user.get('username')} | როლი: {auth_user.get('role')}"
)
if st.sidebar.button("გასვლა", use_container_width=True):
    st.session_state.auth_user = None
    st.rerun()

role = auth_user.get("role")
if role == "Super_Admin":
    pages = [
        "🏢 კომპანიის მართვა",
        "🌍 გლობალური ანალიტიკა",
        "🏠 მაღაზიის პანელი",
        "🛒 გაყიდვები",
        "📥 მარაგების მიღება",
        "🚚 აუცილებელი მიწოდებები",
        "🔔 ინვენტარის გაფრთხილებები",
        "🤝 დისტრიბუტორების მართვა",
        "📈 პროდუქტის ეფექტიანობა",
        "📊 ანგარიშები",
    ]
elif role == "Company_Admin":
    pages = ["🤝 დისტრიბუტორების მართვა", "📈 პროდუქტის ეფექტიანობა"]
elif role == "Store_Manager":
    pages = ["🏠 მაღაზიის პანელი", "🛒 გაყიდვები", "📥 მარაგების მიღება"]
elif role == "Distributor":
    pages = ["📍 ვიზიტები", "🚚 აუცილებელი მიწოდებები", "🔔 ინვენტარის გაფრთხილებები"]
else:
    pages = ["🏢 კომპანიის მართვა"]
# Stable navigation: keep radio state separate from programmatic redirects.
if "current_page" not in st.session_state or st.session_state.current_page not in pages:
    st.session_state.current_page = pages[0]
if "pending_page" in st.session_state and st.session_state.pending_page in pages:
    st.session_state.current_page = st.session_state.pending_page
    del st.session_state["pending_page"]

page = st.sidebar.radio(
    "გადადი გვერდზე:",
    pages,
    index=pages.index(st.session_state.current_page),
)
st.session_state.current_page = page

if role == "Super_Admin":
    st.sidebar.markdown("---")
    st.sidebar.subheader("🚀 საჩვენებელი რეჟიმი")
    if st.sidebar.button("10,000 გაყიდვის გენერირება (სიმულაცია)", use_container_width=True):
        st.session_state.df = generate_month_sales_simulation(st.session_state.df, transactions=10000)
        st.session_state.simulation_stress_mode = True
        st.session_state.daily_profit = 0.0
        st.session_state.profit_date = datetime.now().strftime("%Y-%m-%d")
        st.rerun()

    if st.sidebar.button("🗑️ სისტემის გასუფთავება", use_container_width=True):
        reset_system_data()
        st.session_state.simulation_stress_mode = False
        st.session_state.df = pd.DataFrame()
        st.session_state.daily_profit = 0.0
        st.session_state.profit_date = datetime.now().strftime("%Y-%m-%d")
        st.rerun()

# --- გვერდი 1: DASHBOARD ---
if page == "🏢 კომპანიის მართვა":
    st.title("🏢 კომპანიის მართვა")
    st.caption("სუპერ-ადმინისტრატორის სამუშაო სივრცე: კომპანიები, მარაგები და ოპერაციული სიგნალები.")

    st.subheader("მომხმარებლის ფილიალზე მიბმა")
    users_for_branch = load_users()
    branch_users = [u for u in users_for_branch if u.get("role") in ["Store_Manager", "Distributor"]]
    all_branches = sorted(master_df["Store_Name"].astype(str).unique().tolist()) if not master_df.empty else []
    with st.form("assign_branch_form"):
        selected_user = st.selectbox("მომხმარებელი", [u.get("username", "") for u in branch_users] if branch_users else [])
        selected_branch = st.selectbox("ფილიალი", all_branches if all_branches else [""])
        assign_submitted = st.form_submit_button("მიბმა")
    if assign_submitted and selected_user and selected_branch:
        updated_users = []
        selected_role = ""
        for user in users_for_branch:
            if user.get("username") == selected_user:
                selected_role = user.get("role", "")
                user["assigned_branch"] = selected_branch
                user["store"] = selected_branch
                if user.get("role") == "Distributor":
                    user["allowed_stores"] = selected_branch
            updated_users.append(user)
        save_users(updated_users)
        if selected_role == "Distributor":
            map_df = mapping_data.get("df", pd.DataFrame(columns=["mapping_type", "key", "value"]))
            map_df = map_df[
                ~((map_df["mapping_type"] == "distributor_store") & (map_df["key"] == selected_user))
            ]
            map_df = pd.concat(
                [map_df, pd.DataFrame([{"mapping_type": "distributor_store", "key": selected_user, "value": selected_branch}])],
                ignore_index=True,
            )
            map_df.to_csv(MAPPING_FILE, index=False)
        st.success(f"{selected_user} მიბმულია ფილიალზე: {selected_branch}")
        st.rerun()

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

    st.divider()
    st.subheader("გასათვალისწინებელი შეტყობინებები")
    sales_df = sales_df_all.copy()
    low_stock_alerts = get_low_stock_alerts(df, threshold=5)
    daily_actions = []
    if low_stock_alerts.empty:
        st.success("კრიტიკული დაბალი ნაშთის პროდუქტები არ არის.")
    else:
        for idx, row in low_stock_alerts.iterrows():
            store_name = str(row["Store_Name"])
            product_name = str(row["Product_Name"])
            current_stock = int(row["Current_Stock"])
            daily_actions.append(f"[მაღალი] შეამოწმე ნაშთი: {store_name} - {product_name}")
            if current_stock <= 0:
                st.error(f"{store_name}: {product_name} ამოიწურა! (ნაშთი: {current_stock})")
            else:
                st.warning(f"{store_name}: {product_name} იწურება! (ნაშთი: {current_stock})")
            if current_stock <= 2:
                st.warning(f"{product_name} კრიტიკულად ცოტაა! დისტრიბუტორმა დაიგვიანა. სასწრაფოდ შეუკვეთეთ.")
                daily_actions.append(f"[უმაღლესი] {product_name} კრიტიკულად ცოტაა! დისტრიბუტორმა დაიგვიანა. სასწრაფოდ შეუკვეთეთ.")
            with st.expander("რეკომენდაცია"):
                avg_daily_qty, recommended_qty = get_restock_recommendation_qty(
                    sales_df, product_name, store_name, current_stock
                )
                st.info(f"{product_name} იწურება. ბოლო კვირის გაყიდვებით გირჩევთ დაამატოთ {recommended_qty} რაოდენობა")
                if st.button("მიღება", key=f"goto_add_stock_{idx}"):
                    st.session_state.pending_page = "📥 საქონლის მიღება"
                    st.session_state.prefill_product = product_name
                    st.session_state.prefill_store = store_name
                    st.session_state.prefill_qty = max(1, recommended_qty)
                    st.rerun()

    inactive_branches = get_branches_with_no_sales_last_3h(sales_df, df)
    for branch in inactive_branches:
        st.warning(f"{branch}: ყურადღება, გაყიდვები შეჩერებულია! შეამოწმეთ სტატუსი")
        daily_actions.append(f"[მაღალი] შეამოწმე გაყიდვების სტატუსი: {branch}")

    if st.session_state.get("simulation_stress_mode", False) and detect_stock_discrepancy(df, sales_df):
        st.warning("აღმოჩენილია აცდენა ნაშთებში. ჩაატარეთ ინვენტარიზაცია.")
        daily_actions.append("[მაღალი] აღმოჩენილია აცდენა ნაშთებში. ჩაატარეთ ინვენტარიზაცია.")

    perf_df = compute_branch_performance(sales_df)
    if not perf_df.empty:
        for _, p_row in perf_df.iterrows():
            if float(p_row["PerformancePct"]) < 85:
                branch = str(p_row["Store"])
                down_pct = float(p_row["DownPct"])
                weak_product = get_underperforming_product(sales_df, branch) or "პროდუქტი"
                daily_actions.append(f"[საშუალო] დაადასტურე შეკვეთა: {branch}")
                st.error(
                    f"{branch}: ეფექტურობა შემცირებულია {down_pct:.0f}% -ით. "
                    f"რეკომენდაცია: ფასდაკლება პროდუქტზე {weak_product} გაყიდვების გასაზრდელად."
                )

    st.divider()
    st.subheader("დღიური სამოქმედო გეგმა")
    if not daily_actions:
        st.info("დღეს კრიტიკული დავალებები არ არის.")
    else:
        for i, action in enumerate(daily_actions[:8], start=1):
            st.write(f"{i}. {action}")

elif page == "📍 ვიზიტები":
    st.title("📍 ვიზიტები")
    st.caption("კლიენტების სია პრიორიტეტით - პირველ რიგში ყველაზე გადაუდებელი ვიზიტები.")
    low_stock_alerts = get_low_stock_alerts(df, threshold=5)
    user_company = mapping_data.get("user_company", {}).get(auth_user.get("username", ""), auth_user.get("company", ""))
    balances_df = load_balances_for_company(user_company)

    if low_stock_alerts.empty:
        st.info("ვიზიტისთვის გადაუდებელი ობიექტები არ მოიძებნა.")
    else:
        rows = []
        for store_name, group in low_stock_alerts.groupby("Store_Name"):
            critical = int((group["Current_Stock"] <= 0).sum())
            low = int((group["Current_Stock"] > 0).sum())
            shortage_points = int((5 - group["Current_Stock"].clip(upper=5)).sum())
            urgency_score = critical * 10 + low * 4 + shortage_points
            store_balance = balances_df[balances_df["store"].astype(str) == str(store_name)]["balance"].sum()
            rows.append(
                {
                    "store": str(store_name),
                    "critical": critical,
                    "low": low,
                    "urgency": urgency_score,
                    "balance": float(store_balance),
                }
            )
        visits_df = pd.DataFrame(rows).sort_values("urgency", ascending=False)

        for _, visit in visits_df.iterrows():
            store_name = visit["store"]
            balance = float(visit["balance"])
            balance_text = f"დავალიანება: {abs(balance):.2f}" if balance < 0 else f"კრედიტი: {balance:.2f}"
            with st.container(border=True):
                st.markdown(f"### {store_name}")
                st.markdown(
                    f"**გადაუდებლობის ქულა:** {int(visit['urgency'])} | "
                    f"**კრიტიკული პროდუქტი:** {int(visit['critical'])} | "
                    f"**დაბალი ნაშთი:** {int(visit['low'])}"
                )
                st.markdown(f"**ბალანსი:** {balance_text}")
                if st.button("შეკვეთის გახსნა", key=f"visit_open_{store_name}"):
                    st.session_state["distributor_selected_store"] = store_name
                    st.rerun()

        selected_store = st.session_state.get("distributor_selected_store", "")
        if selected_store:
            st.divider()
            st.subheader(f"შეკვეთის ფორმა: {selected_store}")
            store_items = low_stock_alerts[low_stock_alerts["Store_Name"].astype(str) == str(selected_store)]
            if store_items.empty:
                st.info("არჩეული ფილიალისთვის დაბალი ნაშთის რეკომენდაციები არ არის.")
            else:
                sales_df = sales_df_all.copy()
                with st.form("distributor_order_form"):
                    order_rows = []
                    for i, (_, item) in enumerate(store_items.iterrows()):
                        product_name = str(item["Product_Name"])
                        current_stock = int(item["Current_Stock"])
                        _, recommended_qty = get_restock_recommendation_qty(
                            sales_df, product_name, selected_store, current_stock
                        )
                        qty = st.number_input(
                            f"{product_name} (ნაშთი: {current_stock})",
                            min_value=0,
                            value=max(0, int(recommended_qty)),
                            step=1,
                            key=f"dist_order_qty_{i}_{product_name}",
                        )
                        order_rows.append({"product": product_name, "qty": int(qty)})
                    submit_order = st.form_submit_button("შეკვეთის დადასტურება")
                if submit_order:
                    confirmed = [r for r in order_rows if r["qty"] > 0]
                    if not confirmed:
                        st.warning("შეკვეთაში რაოდენობა არ არის მითითებული.")
                    else:
                        st.success(f"{selected_store}-ისთვის შეკვეთა მომზადდა ({len(confirmed)} პროდუქტი).")

elif page == "🚚 აუცილებელი მიწოდებები":
    st.title("🚚 აუცილებელი მიწოდებები")
    st.caption("დისტრიბუტორის მარტივი ხედვა - რა უნდა მიეწოდოს ახლავე.")
    sales_df = sales_df_all.copy()
    low_stock_alerts = get_low_stock_alerts(df, threshold=5)
    if low_stock_alerts.empty:
        st.info("ამ ეტაპზე გადაუდებელი მიწოდება არ არის.")
    else:
        for _, row in low_stock_alerts.iterrows():
            store_name = str(row["Store_Name"])
            product_name = str(row["Product_Name"])
            current_stock = int(row["Current_Stock"])
            _, recommended_qty = get_restock_recommendation_qty(sales_df, product_name, store_name, current_stock)
            if current_stock <= 0:
                st.error(f"{store_name} - {product_name}: ნაშთი 0. მიაწოდეთ მინიმუმ {recommended_qty} ერთეული.")
            else:
                st.warning(f"{store_name} - {product_name}: ნაშთი {current_stock}. რეკომენდაცია: +{recommended_qty} ერთეული.")

elif page == "🔔 ინვენტარის გაფრთხილებები":
    st.title("🔔 ინვენტარის გაფრთხილებები")
    low_stock_alerts = get_low_stock_alerts(df, threshold=5)
    if low_stock_alerts.empty:
        st.info("აქტიური გაფრთხილება არ არის.")
    else:
        for _, row in low_stock_alerts.iterrows():
            store_name = str(row["Store_Name"])
            product_name = str(row["Product_Name"])
            current_stock = int(row["Current_Stock"])
            if current_stock <= 0:
                st.error(f"{store_name}: {product_name} ამოიწურა.")
            else:
                st.warning(f"{store_name}: {product_name} იწურება. ნაშთი: {current_stock}")

elif page == "🏠 მაღაზიის პანელი":
    st.title("🏠 მაღაზიის პანელი")
    assigned_branch = auth_user.get("assigned_branch", "") or auth_user.get("store", "")
    st.caption(f"ფილიალი: {assigned_branch}")
    sales_df = sales_df_all.copy()
    low_stock_alerts = get_low_stock_alerts(df, threshold=5)
    c1, c2, c3 = st.columns(3)
    c1.metric("პროდუქტი", len(df))
    c2.metric("დაბალი ნაშთი", len(low_stock_alerts))
    c3.metric("დღიური მოგება", f"{st.session_state.daily_profit:.2f}")
    if low_stock_alerts.empty:
        st.info("დაბალი ნაშთის გაფრთხილება არ არის.")
    else:
        for _, row in low_stock_alerts.iterrows():
            product_name = str(row["Product_Name"])
            current_stock = int(row["Current_Stock"])
            _, recommended_qty = get_restock_recommendation_qty(sales_df, product_name, assigned_branch, current_stock)
            st.warning(f"რეკომენდაცია: მოითხოვეთ {product_name} დისტრიბუტორისგან (+{recommended_qty}).")

elif page == "🛒 გაყიდვები":
    st.title("🛒 გაყიდვები")
    if df.empty:
        st.info("ფილიალისთვის პროდუქცია არ არის ხელმისაწვდომი.")
    else:
        options = [f"{row['Product_Name']} (ნაშთი: {int(row['Current_Stock'])})" for _, row in df.iterrows()]
        with st.form("pos_form"):
            selected_label = st.selectbox("პროდუქტი", options)
            sell_qty = st.number_input("რაოდენობა", min_value=1, value=1, step=1)
            submitted_pos = st.form_submit_button("გაყიდვა")
        if submitted_pos:
            selected_idx = options.index(selected_label)
            index = df.index[selected_idx]
            row = df.loc[index]
            current_stock = int(row["Current_Stock"])
            if int(sell_qty) > current_stock:
                st.warning("ნაშთზე მეტი გაყიდვა შეუძლებელია. გამოიყენეთ ინვენტარიზაცია.")
            else:
                full_df = st.session_state.df.copy()
                full_df.at[index, "Current_Stock"] = max(0, current_stock - int(sell_qty))
                full_df = recalc_metrics(full_df)
                save_data(full_df)
                st.session_state.df = full_df
                append_sales_log(
                    sale_date=datetime.now(),
                    store=row.get("Store_Name", ""),
                    product=row.get("Product_Name", ""),
                    qty=int(sell_qty),
                    selling_price=row.get("Selling_Price", 0),
                    cost_price=row.get("Cost_Price", 0),
                )
                st.success("გაყიდვა შენახულია.")
                st.rerun()

elif page == "📥 მარაგების მიღება":
    st.title("📥 მარაგების მიღება")
    assigned_branch = auth_user.get("assigned_branch", "") or auth_user.get("store", "")
    st.caption(f"ფილიალი: {assigned_branch}")
    with st.form("store_receive_stock_form"):
        f_name = st.text_input("პროდუქტი")
        f_qty = st.number_input("რაოდენობა", min_value=1, value=1, step=1)
        f_cost_price = st.number_input("თვითღირებულება", min_value=0.0, format="%.2f")
        f_selling_price = st.number_input("გასაყიდი ფასი", min_value=0.0, format="%.2f")
        f_expiry = st.date_input("ვადა")
        receive_submitted = st.form_submit_button("შენახვა")
    if receive_submitted:
        full_df = st.session_state.df.copy()
        new_row = {
            "Store_Name": assigned_branch,
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
            "Expiry_Date": pd.to_datetime(f_expiry),
        }
        full_df = pd.concat([full_df, pd.DataFrame([new_row])], ignore_index=True)
        full_df = recalc_metrics(full_df)
        save_data(full_df)
        st.session_state.df = full_df
        st.success("მარაგი მიღებულია.")
        st.rerun()

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
        current_stock = int(row["Current_Stock"])
        can_sell = current_stock > 0
        qty_key = f"sell_qty_{int(index)}"
        c7.number_input("რაოდ.", min_value=1, value=1, step=1, key=qty_key, label_visibility="collapsed")
        sell_key = f"sell_btn_{int(index)}_{str(row.get('Product_Name', ''))}_{str(row.get('Store_Name', ''))}"
        if c7.button("გაყიდვა", key=sell_key):
            sell_qty = int(st.session_state.get(qty_key, 1))
            if not can_sell or sell_qty > current_stock:
                st.warning("ნაშთზე მეტი გაყიდვა ვერ შესრულდება. არის თუ არა დათვლის შეცდომა? პირველ რიგში გაასწორე ინვენტარი.")
                if st.button("ნაშთის გასწორება", key=f"fix_inventory_{index}"):
                    st.session_state.pending_page = "🔍 ინვენტარიზაცია"
                    st.rerun()
                continue
            unit_profit = float(row.get("Selling_Price", 0)) - float(row.get("Cost_Price", 0))
            st.session_state.daily_profit += unit_profit * sell_qty
            append_sales_log(
                sale_date=datetime.now(),
                store=row.get("Store_Name", ""),
                product=row.get("Product_Name", ""),
                qty=sell_qty,
                selling_price=row.get("Selling_Price", 0),
                cost_price=row.get("Cost_Price", 0),
            )
            full_df = st.session_state.df.copy()
            full_df.at[index, "Current_Stock"] = max(0, int(row["Current_Stock"]) - sell_qty)
            full_df = recalc_metrics(full_df)
            save_data(full_df)
            st.session_state.df = full_df
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

    store_options = df["Store_Name"].unique() if not df.empty else ["გლდანის ფილიალი"]
    prefill_product = st.session_state.pop("prefill_product", "")
    prefill_store = st.session_state.pop("prefill_store", "")
    prefill_qty = st.session_state.pop("prefill_qty", None)
    if prefill_product:
        st.session_state["add_stock_product"] = prefill_product
    if prefill_store and prefill_store in store_options:
        st.session_state["add_stock_store"] = prefill_store
    elif "add_stock_store" not in st.session_state:
        st.session_state["add_stock_store"] = store_options[0]
    if prefill_qty is not None:
        st.session_state["add_stock_qty"] = int(prefill_qty)
    elif "add_stock_qty" not in st.session_state:
        st.session_state["add_stock_qty"] = 1

    selected_store_for_reco = st.session_state.get("add_stock_store", store_options[0])
    selected_product_for_reco = st.session_state.get("add_stock_product", "")
    if selected_product_for_reco:
        sales_df = sales_df_all.copy()
        store_df = df[
            (df["Store_Name"].astype(str) == str(selected_store_for_reco))
            & (df["Product_Name"].astype(str) == str(selected_product_for_reco))
        ]
        current_stock = int(store_df["Current_Stock"].iloc[0]) if not store_df.empty else 0
        avg_daily_qty, needed_qty = get_restock_recommendation_qty(
            sales_df, selected_product_for_reco, selected_store_for_reco, current_stock
        )
        st.info(
            f"რეკომენდაცია: {selected_product_for_reco} ({selected_store_for_reco}) - "
            f"კვირაში საშუალოდ {avg_daily_qty:.1f} მოთხოვნაა; დაამატე {needed_qty} ერთეული."
        )
        if st.button("რეკომენდაციის მიღება"):
            st.session_state["add_stock_qty"] = max(1, needed_qty)
            st.rerun()

    with st.form("manual_add"):
        col1, col2 = st.columns(2, gap="large")
        f_store = col1.selectbox("მაღაზია", store_options, key="add_stock_store")
        f_name = col2.text_input("პროდუქტი", key="add_stock_product")
        f_qty = col1.number_input("რაოდენობა", min_value=1, key="add_stock_qty")
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
        full_df = st.session_state.df.copy()
        full_df = pd.concat([full_df, pd.DataFrame([new_row])], ignore_index=True)
        full_df = recalc_metrics(full_df)
        save_data(full_df)
        st.session_state.df = full_df
        st.session_state["add_stock_product"] = ""
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
                full_df = st.session_state.df.copy()
                full_df.at[selected_idx, "Current_Stock"] = updated_stock
                full_df = recalc_metrics(full_df)
                save_data(full_df)
                st.session_state.df = full_df
                append_audit_log(
                    store=df.at[selected_idx, "Store_Name"],
                    product=product_name,
                    old_stock=old_stock,
                    new_stock=updated_stock,
                    difference=difference,
                    reason=reason,
                    cost_price=cost_price,
                )
                st.success("ინვენტარის ცვლილება შენახულია და აუდიტის ჟურნალი განახლდა.")
                st.rerun()

elif page == "🤝 დისტრიბუტორების მართვა":
    st.title("🤝 დისტრიბუტორების მართვა")
    st.caption("Company Admin ქმნის Distributor ანგარიშებს მხოლოდ საკუთარი კომპანიისთვის.")

    users = load_users()
    my_role = auth_user.get("role", "")
    my_company = mapping_data.get("user_company", {}).get(auth_user.get("username", ""), auth_user.get("company", ""))

    if my_role == "Super_Admin":
        visible_users = users
    else:
        visible_users = [u for u in users if str(u.get("company", "")) == str(my_company)]

    users_display = pd.DataFrame(visible_users)[["username", "role", "company", "store", "allowed_stores", "allowed_products"]]
    users_display = users_display.rename(
        columns={
            "username": "მომხმარებელი",
            "role": "როლი",
            "company": "კომპანია",
            "store": "ფილიალი",
            "allowed_stores": "ნებადართული ფილიალები",
            "allowed_products": "ნებადართული პროდუქტები",
        }
    )
    st.dataframe(users_display, use_container_width=True)

    st.divider()
    st.subheader("ახალი Distributor ანგარიშის შექმნა")
    available_stores = sorted(master_df["Store_Name"].astype(str).unique().tolist()) if not master_df.empty else []
    available_products = sorted(master_df["Product_Name"].astype(str).unique().tolist()) if not master_df.empty else []

    with st.form("create_distributor_form"):
        new_username = st.text_input("მომხმარებლის სახელი")
        new_password = st.text_input("პაროლი", type="password")

        if my_role == "Super_Admin":
            company_options = sorted(pd.DataFrame(users)["company"].replace("", pd.NA).dropna().unique().tolist())
            company_options = company_options if company_options else ["Ifkli"]
            new_company = st.selectbox("კომპანია", company_options)
        else:
            new_company = my_company
            st.info(f"კომპანია: {new_company}")

        assign_stores = st.multiselect("მისაწვდომი ფილიალები", options=available_stores)
        assign_products = st.multiselect("მისაწვდომი პროდუქტები", options=available_products)
        create_submitted = st.form_submit_button("Distributor ანგარიშის შექმნა")

    if create_submitted:
        if not new_username.strip() or not new_password.strip():
            st.error("შეავსე მომხმარებლის სახელი და პაროლი.")
        elif any(u.get("username") == new_username.strip() for u in users):
            st.error("ასეთი მომხმარებელი უკვე არსებობს.")
        else:
            new_user = {
                "username": new_username.strip(),
                "password_hash": hash_password(new_password.strip()),
                "role": "Distributor",
                "company": new_company,
                "store": "",
                "allowed_stores": "|".join(assign_stores),
                "allowed_products": "|".join(assign_products),
            }
            users.append(new_user)
            save_users(users)
            map_df = mapping_data.get("df", pd.DataFrame(columns=["mapping_type", "key", "value"]))
            map_rows = [{"mapping_type": "user_company", "key": new_username.strip(), "value": new_company}]
            map_rows += [{"mapping_type": "distributor_store", "key": new_username.strip(), "value": s} for s in assign_stores]
            map_df = pd.concat([map_df, pd.DataFrame(map_rows)], ignore_index=True)
            map_df.to_csv(MAPPING_FILE, index=False)
            st.success("Distributor ანგარიში წარმატებით შეიქმნა.")
            st.rerun()

elif page in ["🌍 გლობალური ანალიტიკა", "📈 პროდუქტის ეფექტიანობა"]:
    if page == "🌍 გლობალური ანალიტიკა":
        st.title("🌍 გლობალური ანალიტიკა")
        st.caption("სუპერ-ადმინისტრატორის ჯამური ანალიტიკა ყველა კომპანიის მიხედვით.")
    else:
        st.title("📈 პროდუქტის ეფექტიანობა")
        st.caption("Company Admin ხედავს მხოლოდ საკუთარი კომპანიის პროდუქტების ანალიტიკას.")

    sales_df = sales_df_all.copy()

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
    audit_df = audit_df_all.copy()

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
