import pandas as pd
import streamlit as st
from datetime import datetime, timedelta, date
import os
import random
import math
import json
import hashlib
import traceback
from rs_connector import parse_rs_payload, simulate_fetch_waybill

st.set_page_config(page_title="ERP Smart System", layout="wide")
FILE_NAME = "Product.csv.txt"
AUDIT_LOG_FILE = "audit_log.csv"
SALES_LOG_FILE = "sales_log.csv"
USERS_FILE = "users.csv"
DISTRIBUTOR_MAP_FILE = "distributor_mapping.json"
MAPPING_FILE = "mapping.csv"
BALANCE_FILE = "balances.csv"
DELIVERY_LOG_FILE = "deliveries_log.csv"
PENDING_DELIVERY_FILE = "pending_deliveries.csv"
DISCREPANCY_LOG_FILE = "discrepancy_log.csv"
CORRECTION_LOG_FILE = "correction_log.csv"
ADJUSTMENT_REQUEST_FILE = "adjustment_requests.csv"
TRUCK_STOCK_FILE = "truck_stock.csv"
RETURNS_LOG_FILE = "returns_log.csv"
APP_ERROR_LOG_FILE = "app_errors.log"
STORES_DIRECTORY_FILE = "stores_directory.csv"
# ერთიანი მარაგის ფაილი (UI/დოკ.: inventory.csv → იგივე Product.csv.txt)
SHARED_INVENTORY_FILE = FILE_NAME


def log_exception_to_app_errors(exc: BaseException) -> None:
    """Append uncaught errors to app_errors.log for post-mortem debugging."""
    block = (
        f"\n{'=' * 72}\n"
        f"{datetime.now().isoformat(timespec='seconds')}\n"
        f"{type(exc).__name__}: {exc}\n"
        f"{traceback.format_exc()}\n"
    )
    try:
        with open(APP_ERROR_LOG_FILE, "a", encoding="utf-8") as lf:
            lf.write(block)
    except OSError:
        pass


STORE_NAME_MAP = {
    "Gldani_Branch": "გლდანის ფილიალი",
    "Vake_Branch": "ვაკის ფილიალი",
    "Saburtalo_Branch": "საბურთალოს ფილიალი",
    "Didube_Branch": "დიდუბის ფილიალი",
}

LEGACY_STORE_CONTACTS = {
    "გლდანის ფილიალი": ("თბილისი, გლდანი", ""),
    "ვაკის ფილიალი": ("თბილისი, ვაკე", ""),
    "საბურთალოს ფილიალი": ("თბილისი, საბურთალო", ""),
    "დიდუბის ფილიალი": ("თბილისი, დიდუბე", ""),
}

# 1. მონაცემების მართვა
PRODUCT_NUMERIC_COLUMNS = [
    "Current_Stock",
    "Price",
    "Cost_Price",
    "Selling_Price",
    "Sales_Day1",
    "Sales_Day2",
    "Sales_Day3",
    "Sales_Day4",
    "Sales_Day5",
    "Sales_Day6",
    "Sales_Day7",
]


def normalize_product_df(df):
    if df is None:
        return pd.DataFrame()
    if df.empty:
        base_cols = ["Store_Name", "Product_Name", "Expiry_Date"] + PRODUCT_NUMERIC_COLUMNS
        for col in base_cols:
            if col not in df.columns:
                df[col] = [] if col in ["Store_Name", "Product_Name"] else pd.Series(dtype="float64")
        return df
    for col in ["Store_Name", "Product_Name", "Expiry_Date"]:
        if col not in df.columns:
            df[col] = ""
    for col in PRODUCT_NUMERIC_COLUMNS:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["Expiry_Date"] = pd.to_datetime(df["Expiry_Date"], errors="coerce")
    return df


def ensure_data_structure(df):
    if df.empty:
        return normalize_product_df(df)

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
    return normalize_product_df(df)


def ensure_directory_stores_have_inventory_rows():
    """stores_directory.csv-ში არსებული მაღაზიები, რომლებსაც Product.csv.txt-ში ხაზი არ აქვს — ივსება საწყისი ინვენტარით."""
    ensure_auth_files()
    if not os.path.exists(FILE_NAME):
        return
    sdf = load_stores_directory_df()
    if sdf.empty:
        return
    df = pd.read_csv(FILE_NAME)
    if "Store_Name" not in df.columns:
        return
    existing = set(df["Store_Name"].astype(str).str.strip())
    new_rows = []
    exp = (datetime.now() + timedelta(days=50)).strftime("%Y-%m-%d")
    seed = [
        ("Apple", 42, 3.0, [10, 9, 11, 10, 12, 9, 11]),
        ("Milk", 65, 3.2, [16, 15, 17, 16, 18, 15, 17]),
        ("Bread", 38, 1.4, [9, 10, 8, 9, 10, 9, 8]),
        ("Rice", 28, 4.0, [6, 5, 7, 6, 5, 6, 7]),
    ]
    for _, sr in sdf.iterrows():
        sn = str(sr.get("Store_Name", "")).strip()
        if not sn or sn in existing:
            continue
        for pname, stock, price, days in seed:
            row = {
                "Store_Name": sn,
                "Product_Name": pname,
                "Current_Stock": stock,
                "Price": price,
                "Sales_Day1": days[0],
                "Sales_Day2": days[1],
                "Sales_Day3": days[2],
                "Sales_Day4": days[3],
                "Sales_Day5": days[4],
                "Sales_Day6": days[5],
                "Sales_Day7": days[6],
                "Expiry_Date": exp,
            }
            new_rows.append(row)
        existing.add(sn)
    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        safe_write_csv(df, FILE_NAME)


def ensure_directory_stores_in_mapping_and_users():
    """დირექტორიის ყველა მაღაზია უკავშირდება დისტრიბუტორებს (mapping + allowed_stores), რომ მარშრუტის სია არ იყოს შეზღუდული მხოლოდ 2–3 ჩანაწერით."""
    ensure_auth_files()
    sdf = load_stores_directory_df()
    if sdf.empty or not os.path.exists(MAPPING_FILE):
        return
    store_names = [str(x).strip() for x in sdf["Store_Name"].tolist() if str(x).strip()]
    if not store_names:
        return
    users_list = load_users()
    dist_users = sorted({str(u.get("username", "")) for u in users_list if str(u.get("role", "")) == "Distributor" and str(u.get("username", ""))})
    if not dist_users:
        dist_users = ["distributor_a"]
    mdf = pd.read_csv(MAPPING_FILE, dtype=str).fillna("")
    add_map = []
    for du in dist_users:
        for sn in store_names:
            m = (
                (mdf["mapping_type"].astype(str) == "distributor_store")
                & (mdf["key"].astype(str) == du)
                & (mdf["value"].astype(str) == sn)
            )
            if not m.any():
                add_map.append({"mapping_type": "distributor_store", "key": du, "value": sn})
    if add_map:
        mdf = pd.concat([mdf, pd.DataFrame(add_map)], ignore_index=True)
        safe_write_csv(mdf, MAPPING_FILE)
    extra = set(store_names)
    updated = load_users()
    changed = False
    for u in updated:
        if str(u.get("role", "")) != "Distributor":
            continue
        cur = set(x.strip() for x in str(u.get("allowed_stores", "")).split("|") if x.strip())
        merged = cur | extra
        if merged != cur:
            u["allowed_stores"] = "|".join(sorted(merged))
            changed = True
    if changed:
        save_users(updated)


def ensure_demo_sales_log_entries_today():
    """დემო: თუ sales_log-ში დღევანდელი ჩანაწერი არ არის — ემატება 4 გაყიდვა (UI-ს საჩვენებლად)."""
    ensure_auth_files()
    today = datetime.now().date()
    today_str = today.strftime("%Y-%m-%d")
    if os.path.exists(SALES_LOG_FILE):
        try:
            raw = pd.read_csv(SALES_LOG_FILE)
            if not raw.empty and "Date" in raw.columns:
                if raw["Date"].astype(str).str.startswith(today_str).any():
                    return
        except Exception:
            pass
    now = datetime.now()
    samples = [
        ("ნიკორა - ვაკე", "Apple", 8, 3.1, 1.7),
        ("სპარი - გლდანი", "Milk", 10, 3.25, 1.8),
        ("ორი ნაბიჯი - საბურთალო", "Bread", 20, 1.35, 0.75),
        ("მაგნიტი - ისანი", "Cheese", 6, 12.0, 6.5),
    ]
    for store, prod, qty, sp, cp in samples:
        append_sales_log(now, store, prod, qty, sp, cp)


def ensure_demo_delivery_samples_today(username, company):
    """2–3 სიმულაციური მიწოდება დღევანდელი თარიღით — დღიური ჯამის ტესტისთვის (ერთხელ დღეში, თითო მომხმარებელზე)."""
    uname = str(username).strip()
    co = str(company or "Ifkli").strip()
    if not uname:
        return
    ddf = load_delivery_log()
    today = datetime.now().date()
    flag = "Demo-Today-Seed"
    if not ddf.empty and "timestamp" in ddf.columns and "username" in ddf.columns:
        ddf["_d"] = pd.to_datetime(ddf["timestamp"], errors="coerce").dt.date
        rs = ddf["rs_status"].astype(str) if "rs_status" in ddf.columns else pd.Series([""] * len(ddf))
        if (
            (ddf["username"].astype(str) == uname)
            & (ddf["_d"] == today)
            & (rs == flag)
        ).any():
            return
    samples = [
        ("ნიკორა - ვაკე", "Apple", 10, 3.0),
        ("სპარი - გლდანი", "Milk", 6, 3.25),
        ("ორი ნაბიჯი - საბურთალო", "Bread", 14, 1.35),
    ]
    for st, prod, qty, up in samples:
        append_delivery_log(uname, co, st, prod, int(qty), float(up), rs_status=flag)


def load_data():
    ensure_auth_files()
    ensure_directory_stores_have_inventory_rows()
    ensure_directory_stores_in_mapping_and_users()
    if os.path.exists(FILE_NAME):
        df = pd.read_csv(FILE_NAME)
        df = normalize_product_df(df)
        df = ensure_data_structure(df)
        return df
    return pd.DataFrame()


def load_stores_directory_df():
    ensure_auth_files()
    if not os.path.exists(STORES_DIRECTORY_FILE):
        return pd.DataFrame(columns=["Store_Name", "Address", "Phone"])
    sdf = pd.read_csv(STORES_DIRECTORY_FILE, dtype=str, encoding="utf-8").fillna("")
    return sdf


@st.cache_data(ttl=120)
def get_cached_store_directory_names():
    """stores_directory.csv-ის უნიკალური სახელები (სწრადი სია). CSV-ის შეცვლის შემდეგ — .clear() ან TTL."""
    sdf = load_stores_directory_df()
    if sdf.empty or "Store_Name" not in sdf.columns:
        return tuple()
    names = sorted({str(x).strip() for x in sdf["Store_Name"].tolist() if str(x).strip()})
    return tuple(names)


def get_store_contact_info(store_name):
    """დაბრუნებს (მისამართი, ტელეფონი) ფილიალისთვის — CSV დირექტორია ან ლეგაცია."""
    sn = str(store_name).strip()
    sdf = load_stores_directory_df()
    if not sdf.empty:
        m = sdf["Store_Name"].astype(str).str.strip() == sn
        if m.any():
            r = sdf.loc[m].iloc[0]
            return str(r["Address"]).strip(), str(r["Phone"]).strip()
    mapped = STORE_NAME_MAP.get(sn, sn)
    if mapped != sn and not sdf.empty:
        m = sdf["Store_Name"].astype(str).str.strip() == str(mapped).strip()
        if m.any():
            r = sdf.loc[m].iloc[0]
            return str(r["Address"]).strip(), str(r["Phone"]).strip()
    if sn in LEGACY_STORE_CONTACTS:
        a, p = LEGACY_STORE_CONTACTS[sn]
        return str(a), str(p or "")
    if mapped in LEGACY_STORE_CONTACTS:
        a, p = LEGACY_STORE_CONTACTS[mapped]
        return str(a), str(p or "")
    return "მისამართი არ არის მითითებული", ""


def store_matches_search(store_name, term_lower: str) -> bool:
    if not term_lower:
        return True
    addr, phone = get_store_contact_info(store_name)
    blob = f"{store_name} {addr} {phone}".lower()
    return term_lower in blob


def safe_write_csv(df, file_path):
    try:
        temp_path = f"{file_path}.tmp"
        df.to_csv(temp_path, index=False)
        os.replace(temp_path, file_path)
        return True
    except Exception:
        return False


def safe_append_row(file_path, row, columns):
    try:
        if os.path.exists(file_path):
            existing = pd.read_csv(file_path)
        else:
            existing = pd.DataFrame(columns=columns)
        updated = pd.concat([existing, pd.DataFrame([row])], ignore_index=True)
        return safe_write_csv(updated, file_path)
    except Exception:
        return False


# Data Controller layer (CSV today, DB tomorrow)
def get_products():
    return load_data()


def save_products(df):
    return save_data(df)


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
                    "retail_chain": "Global",
                    "store": "",
                    "commission_rate": 0.0,
                    "allowed_stores": "",
                    "allowed_products": "",
                },
                {
                    "username": "manager_gldani",
                    "password_hash": hash_password("manager123"),
                    "role": "Store_Manager",
                    "company": "Ifkli",
                    "retail_chain": "Ifkli_Retail",
                    "store": "გლდანის ფილიალი",
                    "assigned_branch": "გლდანის ფილიალი",
                    "commission_rate": 0.0,
                    "allowed_stores": "გლდანის ფილიალი",
                    "allowed_products": "",
                },
                {
                    "username": "ifkli_admin",
                    "password_hash": hash_password("ifkli123"),
                    "role": "Company_Admin",
                    "company": "Ifkli",
                    "retail_chain": "Ifkli_Retail",
                    "store": "",
                    "assigned_branch": "",
                    "commission_rate": 0.0,
                    "allowed_stores": "გლდანის ფილიალი|ვაკის ფილიალი",
                    "allowed_products": "Apple|Banana|Milk|Bread|Rice",
                },
                {
                    "username": "ifkli_operator",
                    "password_hash": hash_password("operator123"),
                    "role": "Company_Operator",
                    "company": "Ifkli",
                    "retail_chain": "Ifkli_Retail",
                    "store": "",
                    "assigned_branch": "",
                    "commission_rate": 0.0,
                    "allowed_stores": "გლდანის ფილიალი|ვაკის ფილიალი",
                    "allowed_products": "Apple|Banana|Milk|Bread|Rice",
                },
                {
                    "username": "ifkli_retail_operator",
                    "password_hash": hash_password("retail123"),
                    "role": "Retail_Operator",
                    "company": "Ifkli",
                    "retail_chain": "Ifkli_Retail",
                    "store": "",
                    "assigned_branch": "",
                    "commission_rate": 0.0,
                    "allowed_stores": "გლდანის ფილიალი|ვაკის ფილიალი|საბურთალოს ფილიალი|დიდუბის ფილიალი",
                    "allowed_products": "Apple|Banana|Milk|Bread|Rice",
                },
                {
                    "username": "ifkli_supplier_operator",
                    "password_hash": hash_password("supplier123"),
                    "role": "Supplier_Operator",
                    "company": "Ifkli",
                    "retail_chain": "Ifkli_Retail",
                    "store": "",
                    "assigned_branch": "",
                    "commission_rate": 0.0,
                    "allowed_stores": "გლდანის ფილიალი|ვაკის ფილიალი|საბურთალოს ფილიალი|დიდუბის ფილიალი",
                    "allowed_products": "Apple|Banana|Milk|Bread|Rice",
                },
                {
                    "username": "distributor_a",
                    "password_hash": hash_password("dist123"),
                    "role": "Distributor",
                    "company": "Ifkli",
                    "retail_chain": "Ifkli_Retail",
                    "store": "",
                    "assigned_branch": "",
                    "commission_rate": 0.05,
                    "allowed_stores": "გლდანის ფილიალი|ვაკის ფილიალი|ნიკორა - ვაკე|ორი ნაბიჯი - საბურთალო|სპარი - გლდანი|მაგნიტი - ისანი|აგრო ჰაბი - დიდუბე|ფუდმარტი - ვარკეთილი|სუპერფუდი - ნაძალადევი|გურმე სახლი - მარჯანიშვილი|ევრო პროდუქტი - ჩუღურეთი|მარკეტი 24 - დიღომი",
                    "allowed_products": "Apple|Banana|Milk|Bread|Rice|Cheese|Yogurt|Water|Juice|Pasta|Honey|Chocolate",
                },
                {
                    "username": "market_mgr",
                    "password_hash": hash_password("market123"),
                    "role": "Market",
                    "company": "Ifkli",
                    "retail_chain": "Ifkli_Retail",
                    "store": "",
                    "assigned_branch": "",
                    "commission_rate": 0.0,
                    "allowed_stores": "",
                    "allowed_products": "",
                },
            ]
        )
        default_users_df.to_csv(USERS_FILE, index=False)

    if not os.path.exists(DISTRIBUTOR_MAP_FILE):
        default_map = {
            "distributor_a": {
                "company": "Supplier_A",
                "stores": [
                    "გლდანის ფილიალი",
                    "ვაკის ფილიალი",
                    "ნიკორა - ვაკე",
                    "ორი ნაბიჯი - საბურთალო",
                    "სპარი - გლდანი",
                    "მაგნიტი - ისანი",
                    "აგრო ჰაბი - დიდუბე",
                ],
                "products": [
                    "Apple",
                    "Banana",
                    "Milk",
                    "Bread",
                    "Rice",
                    "Cheese",
                    "Yogurt",
                    "Water",
                    "Juice",
                    "Pasta",
                    "Honey",
                    "Chocolate",
                ],
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
                {"mapping_type": "user_company", "key": "ifkli_operator", "value": "Ifkli"},
                {"mapping_type": "user_company", "key": "distributor_a", "value": "Ifkli"},
                {"mapping_type": "user_company", "key": "market_mgr", "value": "Ifkli"},
                {"mapping_type": "company_product", "key": "Ifkli", "value": "Apple"},
                {"mapping_type": "company_product", "key": "Ifkli", "value": "Banana"},
                {"mapping_type": "company_product", "key": "Ifkli", "value": "Milk"},
                {"mapping_type": "company_product", "key": "Ifkli", "value": "Bread"},
                {"mapping_type": "company_product", "key": "Ifkli", "value": "Rice"},
                {"mapping_type": "company_product", "key": "Ifkli", "value": "Cheese"},
                {"mapping_type": "company_product", "key": "Ifkli", "value": "Yogurt"},
                {"mapping_type": "company_product", "key": "Ifkli", "value": "Water"},
                {"mapping_type": "company_product", "key": "Ifkli", "value": "Juice"},
                {"mapping_type": "company_product", "key": "Ifkli", "value": "Pasta"},
                {"mapping_type": "company_product", "key": "Ifkli", "value": "Honey"},
                {"mapping_type": "company_product", "key": "Ifkli", "value": "Chocolate"},
                {"mapping_type": "distributor_store", "key": "distributor_a", "value": "გლდანის ფილიალი"},
                {"mapping_type": "distributor_store", "key": "distributor_a", "value": "ვაკის ფილიალი"},
                {"mapping_type": "distributor_store", "key": "distributor_a", "value": "ნიკორა - ვაკე"},
                {"mapping_type": "distributor_store", "key": "distributor_a", "value": "ორი ნაბიჯი - საბურთალო"},
                {"mapping_type": "distributor_store", "key": "distributor_a", "value": "სპარი - გლდანი"},
                {"mapping_type": "distributor_store", "key": "distributor_a", "value": "მაგნიტი - ისანი"},
                {"mapping_type": "distributor_store", "key": "distributor_a", "value": "აგრო ჰაბი - დიდუბე"},
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

    if not os.path.exists(DELIVERY_LOG_FILE):
        pd.DataFrame(
            columns=[
                "id",
                "timestamp",
                "date",
                "username",
                "company",
                "store",
                "product",
                "qty",
                "unit_price",
                "total_sales",
                "waybill_no",
                "rs_status",
            ]
        ).to_csv(DELIVERY_LOG_FILE, index=False)

    if not os.path.exists(PENDING_DELIVERY_FILE):
        pd.DataFrame(
            columns=[
                "id",
                "timestamp",
                "date",
                "username",
                "company",
                "store",
                "product",
                "ordered_qty",
                "unit_price",
                "status",
            ]
        ).to_csv(PENDING_DELIVERY_FILE, index=False)

    if not os.path.exists(DISCREPANCY_LOG_FILE):
        pd.DataFrame(
            columns=[
                "timestamp",
                "distributor",
                "company",
                "store",
                "product",
                "ordered_qty",
                "confirmed_qty",
                "difference",
                "reason",
            ]
        ).to_csv(DISCREPANCY_LOG_FILE, index=False)

    if not os.path.exists(CORRECTION_LOG_FILE):
        pd.DataFrame(
            columns=[
                "timestamp",
                "delivery_id",
                "distributor",
                "company",
                "store",
                "product",
                "original_qty",
                "updated_qty",
                "difference",
                "reason",
                "updated_by",
            ]
        ).to_csv(CORRECTION_LOG_FILE, index=False)

    if not os.path.exists(ADJUSTMENT_REQUEST_FILE):
        pd.DataFrame(
            columns=[
                "request_id",
                "timestamp",
                "delivery_id",
                "store_manager",
                "distributor",
                "company",
                "store",
                "product",
                "current_qty",
                "requested_qty",
                "reason",
                "status",
                "reviewed_by",
            ]
        ).to_csv(ADJUSTMENT_REQUEST_FILE, index=False)

    if not os.path.exists(TRUCK_STOCK_FILE):
        pd.DataFrame(columns=["username", "product", "qty", "updated_at"]).to_csv(TRUCK_STOCK_FILE, index=False)

    if not os.path.exists(RETURNS_LOG_FILE):
        pd.DataFrame(
            columns=[
                "id",
                "timestamp",
                "date",
                "username",
                "company",
                "store",
                "product",
                "qty",
                "unit_price",
                "total_return",
                "reason",
                "note",
            ]
        ).to_csv(RETURNS_LOG_FILE, index=False)

    if not os.path.exists(STORES_DIRECTORY_FILE):
        pd.DataFrame(
            [
                {"Store_Name": "ნიკორა - ვაკე", "Address": "თბილისი, ჭონქაძის ქ. 12", "Phone": "+995 577 111 223"},
                {"Store_Name": "ორი ნაბიჯი - საბურთალო", "Address": "თბილისი, ვაჟა-ფშაველას გამზირი 45", "Phone": "+995 577 222 334"},
                {"Store_Name": "სპარი - გლდანი", "Address": "თბილისი, გლდანის მასივი, III მიკრო, ბლოკი 5", "Phone": "+995 577 333 445"},
                {"Store_Name": "მაგნიტი - ისანი", "Address": "თბილისი, ისნის უბანი, ქეთევან დედოფლის ქ. 8", "Phone": "+995 577 444 556"},
                {"Store_Name": "აგრო ჰაბი - დიდუბე", "Address": "თბილისი, დიდუბე, აღმაშენებლის გამზირი 102", "Phone": "+995 577 555 667"},
                {"Store_Name": "ფუდმარტი - ვარკეთილი", "Address": "თბილისი, ვარკეთილი, ხინკლის ქ. 14", "Phone": "+995 577 666 778"},
                {"Store_Name": "სუპერფუდი - ნაძალადევი", "Address": "თბილისი, ნაძალადევი, ხეჩაშვილის ქ. 33", "Phone": "+995 577 777 889"},
                {"Store_Name": "გურმე სახლი - მარჯანიშვილი", "Address": "თბილისი, მარჯანიშვილის მოედანი, ბოკიას ქ. 2", "Phone": "+995 577 888 990"},
                {"Store_Name": "ევრო პროდუქტი - ჩუღურეთი", "Address": "თბილისი, ჩუღურეთი, პოლიტკოვსკაიას ქ. 19", "Phone": "+995 579 101 112"},
                {"Store_Name": "მარკეტი 24 - დიღომი", "Address": "თბილისი, დიღმის მასივი, მესხიშვილის ქ. 7", "Phone": "+995 579 202 223"},
            ]
        ).to_csv(STORES_DIRECTORY_FILE, index=False, encoding="utf-8")


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


def append_delivery_log(
    username,
    company,
    store,
    product,
    qty,
    unit_price,
    delivery_id=None,
    waybill_no="",
    rs_status="Pending on RS.GE",
    at_time=None,
):
    ts = datetime.now() if at_time is None else at_time
    if not isinstance(ts, datetime):
        ts = pd.to_datetime(ts, errors="coerce")
        if pd.isna(ts):
            ts = datetime.now()
        else:
            ts = ts.to_pydatetime()
    row = {
        "id": delivery_id if delivery_id else f"del_{int(ts.timestamp() * 1000)}_{random.randint(1000, 9999)}",
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "date": ts.strftime("%Y-%m-%d"),
        "username": str(username),
        "company": str(company),
        "store": str(store),
        "product": str(product),
        "qty": int(qty),
        "unit_price": float(unit_price),
        "total_sales": float(qty) * float(unit_price),
        "waybill_no": str(waybill_no),
        "rs_status": str(rs_status),
    }
    safe_append_row(
        DELIVERY_LOG_FILE,
        row,
        ["id", "timestamp", "date", "username", "company", "store", "product", "qty", "unit_price", "total_sales", "waybill_no", "rs_status"],
    )


def append_pending_delivery(username, company, store, product, ordered_qty, unit_price, notes=""):
    ts = datetime.now()
    row = {
        "id": f"{int(ts.timestamp() * 1000)}_{random.randint(1000, 9999)}",
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "date": ts.strftime("%Y-%m-%d"),
        "username": str(username),
        "company": str(company),
        "store": str(store),
        "product": str(product),
        "ordered_qty": int(ordered_qty),
        "unit_price": float(unit_price),
        "status": "pending",
        "notes": str(notes),
        "issue": "",
    }
    safe_append_row(
        PENDING_DELIVERY_FILE,
        row,
        ["id", "timestamp", "date", "username", "company", "store", "product", "ordered_qty", "unit_price", "status", "notes", "issue"],
    )


def create_pending_delivery(username, company, store, product, ordered_qty, unit_price, notes="", issue=""):
    ts = datetime.now()
    row = {
        "id": f"{int(ts.timestamp() * 1000)}_{random.randint(1000, 9999)}",
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "date": ts.strftime("%Y-%m-%d"),
        "username": str(username),
        "company": str(company),
        "store": str(store),
        "product": str(product),
        "ordered_qty": int(ordered_qty),
        "unit_price": float(unit_price),
        "status": "pending",
        "notes": str(notes),
        "issue": str(issue),
    }
    safe_append_row(
        PENDING_DELIVERY_FILE,
        row,
        ["id", "timestamp", "date", "username", "company", "store", "product", "ordered_qty", "unit_price", "status", "notes", "issue"],
    )


def load_pending_deliveries():
    ensure_auth_files()
    if not os.path.exists(PENDING_DELIVERY_FILE):
        return pd.DataFrame(columns=["id", "timestamp", "date", "username", "company", "store", "product", "ordered_qty", "unit_price", "status", "notes", "issue"])
    pdf = pd.read_csv(PENDING_DELIVERY_FILE, dtype=str).fillna("")
    if pdf.empty:
        return pdf
    pdf["ordered_qty"] = pd.to_numeric(pdf["ordered_qty"], errors="coerce").fillna(0).astype(int)
    pdf["unit_price"] = pd.to_numeric(pdf["unit_price"], errors="coerce").fillna(0.0)
    if "notes" not in pdf.columns:
        pdf["notes"] = ""
    if "issue" not in pdf.columns:
        pdf["issue"] = ""
    return pdf


def save_pending_deliveries(pdf):
    safe_write_csv(pdf, PENDING_DELIVERY_FILE)


def append_discrepancy_log(distributor, company, store, product, ordered_qty, confirmed_qty, reason, corrected_by=""):
    diff = int(ordered_qty) - int(confirmed_qty)
    row = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "distributor": str(distributor),
        "company": str(company),
        "store": str(store),
        "product": str(product),
        "ordered_qty": int(ordered_qty),
        "confirmed_qty": int(confirmed_qty),
        "difference": int(diff),
        "reason": str(reason),
        "corrected_by": str(corrected_by),
    }
    safe_append_row(
        DISCREPANCY_LOG_FILE,
        row,
        ["timestamp", "distributor", "company", "store", "product", "ordered_qty", "confirmed_qty", "difference", "reason", "corrected_by"],
    )


def load_discrepancy_log():
    ensure_auth_files()
    if not os.path.exists(DISCREPANCY_LOG_FILE):
        return pd.DataFrame(columns=["timestamp", "distributor", "company", "store", "product", "ordered_qty", "confirmed_qty", "difference", "reason", "corrected_by"])
    dlog = pd.read_csv(DISCREPANCY_LOG_FILE)
    if dlog.empty:
        return dlog
    if "corrected_by" not in dlog.columns:
        dlog["corrected_by"] = ""
    dlog["difference"] = pd.to_numeric(dlog["difference"], errors="coerce").fillna(0)
    return dlog


def format_gel_currency(amount):
    """ლარის ფორმატი: ათასების გამოყოფა გამოტოვებით, ათწილადი მძიმით, ბოლოში ₾."""
    try:
        x = float(amount)
    except (TypeError, ValueError):
        x = 0.0
    neg = x < 0
    x = abs(x)
    whole, frac = f"{x:.2f}".split(".")
    whole_fmt = f"{int(whole):,}".replace(",", " ")
    res = f"{whole_fmt},{frac} ₾"
    return ("−" if neg else "") + res


def append_return_log(username, company, store, product, qty, unit_price, reason, note=""):
    ts = datetime.now()
    qty_i = int(qty)
    up = float(unit_price)
    total_ret = float(qty_i) * up
    row = {
        "id": f"ret_{int(ts.timestamp() * 1000)}_{random.randint(1000, 9999)}",
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "date": ts.strftime("%Y-%m-%d"),
        "username": str(username),
        "company": str(company),
        "store": str(store),
        "product": str(product),
        "qty": qty_i,
        "unit_price": up,
        "total_return": total_ret,
        "reason": str(reason),
        "note": str(note)[:500] if note is not None else "",
    }
    safe_append_row(
        RETURNS_LOG_FILE,
        row,
        [
            "id",
            "timestamp",
            "date",
            "username",
            "company",
            "store",
            "product",
            "qty",
            "unit_price",
            "total_return",
            "reason",
            "note",
        ],
    )


def load_return_log():
    ensure_auth_files()
    if not os.path.exists(RETURNS_LOG_FILE):
        return pd.DataFrame(
            columns=[
                "id",
                "timestamp",
                "date",
                "username",
                "company",
                "store",
                "product",
                "qty",
                "unit_price",
                "total_return",
                "reason",
                "note",
            ]
        )
    rdf = pd.read_csv(RETURNS_LOG_FILE)
    if rdf.empty:
        return rdf
    for col in ["qty", "unit_price", "total_return"]:
        if col not in rdf.columns:
            rdf[col] = 0
        rdf[col] = pd.to_numeric(rdf[col], errors="coerce").fillna(0)
    rdf["timestamp"] = pd.to_datetime(rdf["timestamp"], errors="coerce")
    return rdf.dropna(subset=["timestamp"])


def _parse_float_safe(v, default=0.0):
    try:
        return float(str(v).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return float(default)


def get_user_monthly_sales_target(user_dict):
    return max(0.0, _parse_float_safe(user_dict.get("monthly_sales_target", "0"), 0.0))


def get_distributor_month_sales_for_plan(username, mapping_data, sales_df, delivery_df, returns_df, month_start, month_end):
    """
    თვის შესრულება: თუ დისტრიბუტორს აქვს მიბმული ფილიალები — გაყიდვების ჟურნალის Revenue (sales_log);
    წინააღმდეგ შემთხვევაში — ნეტო მიწოდება (deliveries_log total_sales − returns_log).
    """
    dist_user = str(username)
    raw_stores = mapping_data.get("distributor_stores", {}).get(dist_user) or set()
    stores = {str(s).strip() for s in raw_stores if str(s).strip()}

    sales_log_total = 0.0
    if not sales_df.empty and stores:
        m_sl = (
            (sales_df["Timestamp"].dt.date >= month_start)
            & (sales_df["Timestamp"].dt.date <= month_end)
            & (sales_df["Store"].astype(str).isin(stores))
        )
        sales_log_total = float(sales_df.loc[m_sl, "Revenue"].sum())

    gross = 0.0
    ret = 0.0
    if not delivery_df.empty:
        dd = delivery_df[delivery_df["username"].astype(str) == dist_user]
        if not dd.empty and "timestamp" in dd.columns:
            dm = (dd["timestamp"].dt.date >= month_start) & (dd["timestamp"].dt.date <= month_end)
            gross = float(dd.loc[dm, "total_sales"].sum())
    if not returns_df.empty:
        rf = returns_df[returns_df["username"].astype(str) == dist_user]
        if not rf.empty:
            rm = (rf["timestamp"].dt.date >= month_start) & (rf["timestamp"].dt.date <= month_end)
            ret = float(pd.to_numeric(rf.loc[rm, "total_return"], errors="coerce").fillna(0).sum())
    deliveries_net = max(0.0, gross - ret)

    if stores:
        return sales_log_total, "sales_log", sales_log_total, deliveries_net
    return deliveries_net, "deliveries_net", sales_log_total, deliveries_net


def compute_distributor_period_financials(username, delivery_df, returns_df, start_d, end_d, commission_rate):
    """
    ერთიანი ფინანსური ბაზა დისტრიბუტორის დაფისთვის: მხოლოდ deliveries_log + returns_log.
    start_d/end_d — date ობიექტები (ჩათვლით).
    """
    uname = str(username)
    cr = float(commission_rate or 0)
    gross = 0.0
    if delivery_df is not None and not delivery_df.empty and "timestamp" in delivery_df.columns:
        dd = delivery_df[delivery_df["username"].astype(str) == uname]
        if not dd.empty:
            dm = (dd["timestamp"].dt.date >= start_d) & (dd["timestamp"].dt.date <= end_d)
            gross = float(dd.loc[dm, "total_sales"].sum())
    ret = 0.0
    if returns_df is not None and not returns_df.empty and "timestamp" in returns_df.columns:
        rf = returns_df[returns_df["username"].astype(str) == uname]
        if not rf.empty:
            rm = (rf["timestamp"].dt.date >= start_d) & (rf["timestamp"].dt.date <= end_d)
            ret = float(pd.to_numeric(rf.loc[rm, "total_return"], errors="coerce").fillna(0).sum())
    net = max(0.0, gross - ret)
    return {
        "gross": gross,
        "returns": ret,
        "net": net,
        "commission": net * cr,
    }


def calculate_salary(username, delivery_df, returns_df, start_d, end_d, commission_rate):
    """სახელფასო / თვიური ნეტო — მხოლოდ deliveries_log + returns_log (იგივე რაც compute_distributor_period_financials)."""
    return compute_distributor_period_financials(username, delivery_df, returns_df, start_d, end_d, commission_rate)


def build_distributor_route_context(distributor_live_df, route_alert_threshold=5, stores_directory_df=None):
    """მარშრუტის სია პრიორიტეტით + ქულა; `stores_directory.csv`-ის ყველა სახელი ერთიანდება სიაში (რომ ახალი ფილიალი დაუყოვნებლივ ჩანდეს)."""
    assigned_stores = (
        sorted(distributor_live_df["Store_Name"].astype(str).unique().tolist()) if not distributor_live_df.empty else []
    )
    priority_scores = {}
    if not distributor_live_df.empty:
        tmp_df = distributor_live_df.copy()
        tmp_df["Current_Stock"] = pd.to_numeric(tmp_df["Current_Stock"], errors="coerce").fillna(0)
        for store_name in assigned_stores:
            store_mask = tmp_df["Store_Name"].astype(str) == str(store_name)
            low_count = int((tmp_df.loc[store_mask, "Current_Stock"] < route_alert_threshold).sum())
            priority_scores[store_name] = low_count
    extras = []
    if stores_directory_df is not None and not stores_directory_df.empty and "Store_Name" in stores_directory_df.columns:
        extras = [str(x).strip() for x in stores_directory_df["Store_Name"].tolist() if str(x).strip()]
    for s in extras:
        if s not in priority_scores:
            priority_scores[s] = 0
    merged = sorted(set(assigned_stores) | set(extras), key=lambda s: (-priority_scores.get(s, 0), str(s)))
    return merged, priority_scores


def filter_distributor_visible_stores(assigned_stores, priority_scores, store_search_term, filter_priority_only, route_alert_threshold=5):
    """ძიება და «მხოლოდ პრიორიტეტული» ფილტრი მარშრუტის სიისთვის."""
    _stores = list(assigned_stores)
    if filter_priority_only:
        _stores = [s for s in assigned_stores if priority_scores.get(s, 0) > 0]
    term = (store_search_term or "").strip().lower()
    if not term:
        return _stores
    return [store_name for store_name in _stores if store_matches_search(store_name, term)]


DEBUG_SIM_DISTRIBUTOR_USERNAME = "distributor_a"


def debug_seed_distributor_a_test_logs(mapping_data, product_master_df):
    """
    სიმულაცია: 3–4 შემთხვევითი გაყიდვა (sales_log + deliveries_log) და 1 დაბრუნება (returns_log)
    მომხმარებლისთვის distributor_a — ტესტირებისთვის.
    """
    uname = DEBUG_SIM_DISTRIBUTOR_USERNAME
    company = str(mapping_data.get("user_company", {}).get(uname, "Ifkli"))
    stores = list(mapping_data.get("distributor_stores", {}).get(uname, set()) or [])
    if not stores:
        stores = ["გლდანის ფილიალი", "ვაკის ფილიალი"]
    if product_master_df is None or getattr(product_master_df, "empty", True):
        plist = ["Apple", "Milk", "Bread", "Rice"]
    else:
        plist = product_master_df["Product_Name"].dropna().astype(str).unique().tolist()
    if not plist:
        plist = ["Apple", "Milk"]
    n_sales = random.randint(3, 4)
    now = datetime.now()
    for _ in range(n_sales):
        store = random.choice(stores)
        product = random.choice(plist)
        qty = random.randint(1, 10)
        sp = round(random.uniform(2.5, 18.0), 2)
        cp = round(sp * 0.55, 2)
        append_sales_log(now, store, product, qty, sp, cp)
        append_delivery_log(
            uname,
            company,
            store,
            product,
            qty,
            sp,
            rs_status="Debug-Sim",
        )
    rs = random.choice(stores)
    rp = random.choice(plist)
    rq = random.randint(1, 4)
    rpr = round(random.uniform(3.0, 14.0), 2)
    append_return_log(
        uname,
        company,
        rs,
        rp,
        rq,
        rpr,
        reason="Debug დაბრუნება",
        note="simulation_suite",
    )


def load_delivery_log():
    ensure_auth_files()
    if not os.path.exists(DELIVERY_LOG_FILE):
        return pd.DataFrame(columns=["id", "timestamp", "date", "username", "company", "store", "product", "qty", "unit_price", "total_sales", "waybill_no", "rs_status"])
    ddf = pd.read_csv(DELIVERY_LOG_FILE)
    if ddf.empty:
        return ddf
    if "id" not in ddf.columns:
        ddf["id"] = [f"legacy_{i}" for i in range(len(ddf))]
    if "waybill_no" not in ddf.columns:
        ddf["waybill_no"] = ""
    if "rs_status" not in ddf.columns:
        ddf["rs_status"] = "Pending on RS.GE"
    ddf["timestamp"] = pd.to_datetime(ddf["timestamp"], errors="coerce")
    ddf["qty"] = pd.to_numeric(ddf["qty"], errors="coerce").fillna(0).astype(int)
    ddf["unit_price"] = pd.to_numeric(ddf["unit_price"], errors="coerce").fillna(0.0)
    ddf["total_sales"] = pd.to_numeric(ddf["total_sales"], errors="coerce").fillna(0.0)
    return ddf.dropna(subset=["timestamp"])


def save_delivery_log(ddf):
    safe_write_csv(ddf, DELIVERY_LOG_FILE)


def append_correction_log(delivery_id, distributor, company, store, product, original_qty, updated_qty, reason, updated_by):
    row = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "delivery_id": str(delivery_id),
        "distributor": str(distributor),
        "company": str(company),
        "store": str(store),
        "product": str(product),
        "original_qty": int(original_qty),
        "updated_qty": int(updated_qty),
        "difference": int(updated_qty) - int(original_qty),
        "reason": str(reason),
        "updated_by": str(updated_by),
    }
    safe_append_row(
        CORRECTION_LOG_FILE,
        row,
        ["timestamp", "delivery_id", "distributor", "company", "store", "product", "original_qty", "updated_qty", "difference", "reason", "updated_by"],
    )


def load_correction_log():
    ensure_auth_files()
    if not os.path.exists(CORRECTION_LOG_FILE):
        return pd.DataFrame(columns=["timestamp", "delivery_id", "distributor", "company", "store", "product", "original_qty", "updated_qty", "difference", "reason", "updated_by"])
    cdf = pd.read_csv(CORRECTION_LOG_FILE)
    if cdf.empty:
        return cdf
    cdf["timestamp"] = pd.to_datetime(cdf["timestamp"], errors="coerce")
    return cdf


def append_adjustment_request(delivery_id, store_manager, distributor, company, store, product, current_qty, requested_qty, reason):
    ts = datetime.now()
    row = {
        "request_id": f"req_{int(ts.timestamp() * 1000)}_{random.randint(1000, 9999)}",
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "delivery_id": str(delivery_id),
        "store_manager": str(store_manager),
        "distributor": str(distributor),
        "company": str(company),
        "store": str(store),
        "product": str(product),
        "current_qty": int(current_qty),
        "requested_qty": int(requested_qty),
        "reason": str(reason),
        "status": "pending",
        "reviewed_by": "",
    }
    safe_append_row(
        ADJUSTMENT_REQUEST_FILE,
        row,
        ["request_id", "timestamp", "delivery_id", "store_manager", "distributor", "company", "store", "product", "current_qty", "requested_qty", "reason", "status", "reviewed_by"],
    )


def load_adjustment_requests():
    ensure_auth_files()
    if not os.path.exists(ADJUSTMENT_REQUEST_FILE):
        return pd.DataFrame(columns=["request_id", "timestamp", "delivery_id", "store_manager", "distributor", "company", "store", "product", "current_qty", "requested_qty", "reason", "status", "reviewed_by"])
    rdf = pd.read_csv(ADJUSTMENT_REQUEST_FILE, dtype=str).fillna("")
    if rdf.empty:
        return rdf
    for col in ["current_qty", "requested_qty"]:
        rdf[col] = pd.to_numeric(rdf[col], errors="coerce").fillna(0).astype(int)
    rdf["timestamp"] = pd.to_datetime(rdf["timestamp"], errors="coerce")
    return rdf


def save_adjustment_requests(rdf):
    safe_write_csv(rdf, ADJUSTMENT_REQUEST_FILE)


def update_adjustment_request_status(request_id, status, reviewed_by):
    latest = load_adjustment_requests()
    if latest.empty:
        return False
    mask = latest["request_id"].astype(str) == str(request_id)
    if not mask.any():
        return False
    latest.loc[mask, "status"] = str(status)
    latest.loc[mask, "reviewed_by"] = str(reviewed_by)
    save_adjustment_requests(latest)
    return True


def load_truck_stock():
    ensure_auth_files()
    if not os.path.exists(TRUCK_STOCK_FILE):
        return pd.DataFrame(columns=["username", "product", "qty", "updated_at"])
    tdf = pd.read_csv(TRUCK_STOCK_FILE, dtype=str).fillna("")
    if tdf.empty:
        return tdf
    tdf["qty"] = pd.to_numeric(tdf["qty"], errors="coerce").fillna(0).astype(int)
    return tdf


def save_truck_stock(tdf):
    safe_write_csv(tdf, TRUCK_STOCK_FILE)


def get_truck_qty(username, product):
    tdf = load_truck_stock()
    if tdf.empty:
        return 100
    mask = (
        (tdf["username"].astype(str) == str(username))
        & (tdf["product"].astype(str) == str(product))
    )
    if not mask.any():
        return 100
    return int(pd.to_numeric(tdf.loc[mask, "qty"], errors="coerce").fillna(0).iloc[0])


def set_truck_qty(username, product, qty):
    tdf = load_truck_stock()
    qty = max(0, int(float(qty)))
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if tdf.empty:
        tdf = pd.DataFrame([{"username": str(username), "product": str(product), "qty": qty, "updated_at": now_s}])
        save_truck_stock(tdf)
        return
    mask = (
        (tdf["username"].astype(str) == str(username))
        & (tdf["product"].astype(str) == str(product))
    )
    if mask.any():
        idx = tdf[mask].index[0]
        tdf.at[idx, "qty"] = qty
        tdf.at[idx, "updated_at"] = now_s
    else:
        tdf = pd.concat(
            [tdf, pd.DataFrame([{"username": str(username), "product": str(product), "qty": qty, "updated_at": now_s}])],
            ignore_index=True,
        )
    save_truck_stock(tdf)


def apply_delivery_correction(delivery_id, new_qty, reason, updated_by, full_df, delivery_df):
    target = delivery_df[delivery_df["id"].astype(str) == str(delivery_id)]
    if target.empty:
        return False, "ჩანაწერი ვერ მოიძებნა.", full_df, delivery_df
    row = target.iloc[0]
    old_qty = int(row["qty"])
    diff = int(new_qty) - old_qty
    store = str(row["store"])
    product = str(row["product"])
    distributor = str(row["username"])
    company = str(row["company"])

    mask = (
        (full_df["Store_Name"].astype(str) == store)
        & (full_df["Product_Name"].astype(str) == product)
    )
    if mask.any():
        idx = full_df[mask].index[0]
        full_df.at[idx, "Current_Stock"] = max(0, int(full_df.at[idx, "Current_Stock"]) + diff)

    delivery_df.loc[delivery_df["id"].astype(str) == str(delivery_id), "qty"] = int(new_qty)
    delivery_df.loc[delivery_df["id"].astype(str) == str(delivery_id), "total_sales"] = float(new_qty) * float(row["unit_price"])

    append_correction_log(
        delivery_id=delivery_id,
        distributor=distributor,
        company=company,
        store=store,
        product=product,
        original_qty=old_qty,
        updated_qty=int(new_qty),
        reason=reason,
        updated_by=updated_by,
    )
    if int(new_qty) < old_qty:
        append_discrepancy_log(
            distributor=distributor,
            company=company,
            store=store,
            product=product,
            ordered_qty=old_qty,
            confirmed_qty=int(new_qty),
            reason=f"Correction: {reason}",
            corrected_by=updated_by,
        )
    return True, "კორექცია შესრულდა.", full_df, delivery_df


def load_users():
    ensure_auth_files()
    users_df = pd.read_csv(USERS_FILE, dtype=str).fillna("")

    # Safety fallback: always guarantee default admin access.
    if "username" not in users_df.columns:
        users_df["username"] = ""
    if "password_hash" not in users_df.columns:
        users_df["password_hash"] = ""
    if "password" not in users_df.columns:
        users_df["password"] = ""
    if "role" not in users_df.columns:
        users_df["role"] = ""
    if "company" not in users_df.columns:
        users_df["company"] = ""
    if "retail_chain" not in users_df.columns:
        users_df["retail_chain"] = ""
    if "store" not in users_df.columns:
        users_df["store"] = ""
    if "branch" not in users_df.columns:
        users_df["branch"] = ""
    if "assigned_branch" not in users_df.columns:
        users_df["assigned_branch"] = users_df["store"] if "store" in users_df.columns else ""
    if "allowed_stores" not in users_df.columns:
        users_df["allowed_stores"] = ""
    if "allowed_products" not in users_df.columns:
        users_df["allowed_products"] = ""
    if "commission_rate" not in users_df.columns:
        users_df["commission_rate"] = "0.0"
    if "monthly_sales_target" not in users_df.columns:
        users_df["monthly_sales_target"] = "0"

    # Normalize simple schema (username,password,role,branch,company).
    branch_map = {
        "Vake": "ვაკის ფილიალი",
        "Gldani": "გლდანის ფილიალი",
        "Saburtalo": "საბურთალოს ფილიალი",
        "Didube": "დიდუბის ფილიალი",
        "none": "",
        "all": "",
    }
    users_df["branch"] = users_df["branch"].astype(str)
    users_df["branch"] = users_df["branch"].replace(branch_map)
    users_df.loc[users_df["store"].astype(str) == "", "store"] = users_df["branch"]
    users_df.loc[users_df["assigned_branch"].astype(str) == "", "assigned_branch"] = users_df["branch"]
    users_df["company"] = users_df["company"].replace({"all": "Global", "none": ""})

    admin_exists = (users_df["username"].astype(str) == "admin").any()
    if not admin_exists:
        admin_row = pd.DataFrame(
            [
                {
                    "username": "admin",
                    "password_hash": hash_password("admin123"),
                    "role": "Admin",
                    "company": "Global",
                    "retail_chain": "Global",
                    "store": "",
                    "assigned_branch": "",
                    "commission_rate": 0.0,
                    "allowed_stores": "",
                    "allowed_products": "",
                }
            ]
        )
        users_df = pd.concat([users_df, admin_row], ignore_index=True)
        safe_write_csv(users_df, USERS_FILE)

    market_exists = (users_df["username"].astype(str) == "market_mgr").any()
    if not market_exists:
        market_row = pd.DataFrame(
            [
                {
                    "username": "market_mgr",
                    "password_hash": hash_password("market123"),
                    "password": "",
                    "role": "Market",
                    "company": "Ifkli",
                    "retail_chain": "Ifkli_Retail",
                    "store": "",
                    "branch": "",
                    "assigned_branch": "",
                    "commission_rate": "0.0",
                    "allowed_stores": "",
                    "allowed_products": "",
                    "monthly_sales_target": "0",
                }
            ]
        )
        users_df = pd.concat([users_df, market_row], ignore_index=True)
        safe_write_csv(users_df, USERS_FILE)
        if os.path.exists(MAPPING_FILE):
            mdf = pd.read_csv(MAPPING_FILE, dtype=str).fillna("")
            m = (mdf["mapping_type"].astype(str) == "user_company") & (mdf["key"].astype(str) == "market_mgr")
            if not m.any():
                mdf = pd.concat(
                    [mdf, pd.DataFrame([{"mapping_type": "user_company", "key": "market_mgr", "value": "Ifkli"}])],
                    ignore_index=True,
                )
                safe_write_csv(mdf, MAPPING_FILE)

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
        if user.get("username") == username:
            plain = str(user.get("password", ""))
            hashed = str(user.get("password_hash", ""))
            if plain and plain == password:
                return user
            if hashed and hashed == hash_password(password):
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

    if role == "Market":
        store_name = str(st.session_state.get("market_store_pick_main", "")).strip()
        if not store_name:
            return filtered_df.iloc[0:0], filtered_sales.iloc[0:0]
        filtered_df = filtered_df[filtered_df["Store_Name"].astype(str) == store_name]
        if not filtered_sales.empty:
            filtered_sales = filtered_sales[filtered_sales["Store"].astype(str) == store_name]
        return filtered_df, filtered_sales

    if role == "Distributor":
        allowed_stores = set(distributor_stores.get(username, set()))
        if not allowed_stores:
            allowed_stores = set([x for x in str(auth_user.get("allowed_stores", "")).split("|") if x])
        if not allowed_stores:
            allowed_stores = set(filtered_df["Store_Name"].astype(str).unique().tolist())
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

    if role == "Company_Operator":
        allowed_products = set(company_products.get(company, set()))
        if allowed_products:
            filtered_df = filtered_df[filtered_df["Product_Name"].astype(str).isin(allowed_products)]
            if not filtered_sales.empty:
                filtered_sales = filtered_sales[filtered_sales["Product"].astype(str).isin(allowed_products)]
        return filtered_df, filtered_sales

    if role in ["Retail_Operator", "Supplier_Operator"]:
        allowed_stores = set([x for x in str(auth_user.get("allowed_stores", "")).split("|") if x])
        allowed_products = set(company_products.get(company, set()))
        if allowed_stores:
            filtered_df = filtered_df[filtered_df["Store_Name"].astype(str).isin(allowed_stores)]
            if not filtered_sales.empty:
                filtered_sales = filtered_sales[filtered_sales["Store"].astype(str).isin(allowed_stores)]
        if allowed_products:
            filtered_df = filtered_df[filtered_df["Product_Name"].astype(str).isin(allowed_products)]
            if not filtered_sales.empty:
                filtered_sales = filtered_sales[filtered_sales["Product"].astype(str).isin(allowed_products)]
        return filtered_df, filtered_sales

    return filtered_df.iloc[0:0], filtered_sales.iloc[0:0]


def save_users(users):
    safe_write_csv(pd.DataFrame(users), USERS_FILE)


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
    if role == "Market":
        ms = str(st.session_state.get("market_store_pick_main", "")).strip()
        return {"role": role, "stores": set([ms]) if ms else set(), "products": None}
    if role == "Company_Admin":
        return {"role": role, "stores": None, "products": set(company_products.get(company, set()))}
    if role == "Company_Operator":
        return {"role": role, "stores": None, "products": set(company_products.get(company, set()))}
    if role in ["Retail_Operator", "Supplier_Operator"]:
        stores = set([x for x in str(auth_user.get("allowed_stores", "")).split("|") if x])
        return {"role": role, "stores": stores if stores else None, "products": set(company_products.get(company, set()))}
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
        return "🌍 გლობალური ანალიტიკა"
    if role == "Company_Admin":
        return "🤝 დისტრიბუტორების მართვა"
    if role == "Company_Operator":
        return "🛠 ოპერატორის პანელი"
    if role in ["Retail_Operator", "Supplier_Operator"]:
        return "🛠 ოპერატორის პანელი"
    if role == "Distributor":
        return "🚚 აუცილებელი მიწოდებები"
    if role == "Store_Manager":
        return "🏠 მაღაზიის პანელი"
    if role == "Market":
        return "🏪 ბაზრის პანელი"
    return "🏢 კომპანიის მართვა"


def save_data(df):
    df = normalize_product_df(df.copy())
    df = ensure_data_structure(df)
    return safe_write_csv(df, FILE_NAME)


def ensure_product_row(df, store_name, product_name, qty, cost_price, selling_price):
    def _to_int(v, default=0):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return int(default)

    def _to_float(v, default=0.0):
        try:
            return float(v)
        except (TypeError, ValueError):
            return float(default)

    store_name = str(store_name).strip()
    product_name = str(product_name).strip()
    qty = _to_int(qty, 0)
    cost_price = _to_float(cost_price, 0.0)
    selling_price = _to_float(selling_price, 0.0)

    # Ensure core columns exist before any row-level access.
    for col in ["Cost_Price", "Selling_Price", "Price", "Current_Stock"]:
        if col not in df.columns:
            df[col] = 0
    df["Cost_Price"] = pd.to_numeric(df["Cost_Price"], errors="coerce").fillna(0.0)
    df["Selling_Price"] = pd.to_numeric(df["Selling_Price"], errors="coerce").fillna(0.0)
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce").fillna(0.0)
    df["Current_Stock"] = pd.to_numeric(df["Current_Stock"], errors="coerce").fillna(0)

    # Robust sync: match by normalized store/product to avoid type/text mismatches.
    mask = (
        (df["Store_Name"].astype(str).str.strip() == store_name)
        & (df["Product_Name"].astype(str).str.strip() == product_name)
    )
    if mask.any():
        idx = df[mask].index[0]
        row = df.loc[idx]
        current_cost = _to_float(row["Cost_Price"] if "Cost_Price" in df.columns else 0.0, 0.0)
        current_sell = _to_float(row["Selling_Price"] if "Selling_Price" in df.columns else 0.0, 0.0)
        df.at[idx, "Current_Stock"] = _to_int(df.at[idx, "Current_Stock"], 0) + qty
        if pd.isna(current_cost) or current_cost <= 0:
            df.at[idx, "Cost_Price"] = cost_price
        if pd.isna(current_sell) or current_sell <= 0:
            df.at[idx, "Selling_Price"] = selling_price
        if "Price" in df.columns and (_to_float(row["Price"], 0.0) <= 0):
            df.at[idx, "Price"] = selling_price
        return df

    new_row = {
        "Store_Name": store_name,
        "Product_Name": product_name,
        "Current_Stock": qty,
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
        "Expiry_Date": pd.to_datetime(datetime.now().date() + timedelta(days=30)),
    }
    return pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)


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
    safe_append_row(
        AUDIT_LOG_FILE,
        log_row,
        ["Date", "Store", "Product", "Old Stock", "New Stock", "Difference", "Reason", "Cost Price"],
    )


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
    safe_append_row(
        SALES_LOG_FILE,
        log_row,
        ["Timestamp", "Date", "Store", "Product", "Qty", "Selling_Price", "Cost_Price", "Revenue", "Profit"],
    )


def save_sale(sale_date, store, product, qty, selling_price, cost_price):
    append_sales_log(
        sale_date=sale_date,
        store=store,
        product=product,
        qty=qty,
        selling_price=selling_price,
        cost_price=cost_price,
    )


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
            safe_write_csv(sales_df, SALES_LOG_FILE)

        for numeric_col in ["Qty", "Selling_Price", "Cost_Price", "Revenue", "Profit"]:
            sales_df[numeric_col] = pd.to_numeric(sales_df[numeric_col], errors="coerce").fillna(0)
        return sales_df.dropna(subset=["Timestamp"])
    return pd.DataFrame(columns=["Timestamp", "Date", "Store", "Product", "Qty", "Selling_Price", "Cost_Price", "Revenue", "Profit"])


@st.cache_data(ttl=120)
def _cached_monthly_analytics_sales(sales_mtime: float):
    return load_sales_log()


def distributor_plan_bonus_estimate(plan_target: float, net_deliveries: float) -> float:
    """გეგმის შესრულების პრემია: თუ ნეტო მიწოდება ≥ თვიური გეგმა — ფიქსირებული + ზედმეტი ნაწილი."""
    pt = float(plan_target or 0)
    nd = float(net_deliveries or 0)
    if pt <= 0 or nd < pt:
        return 0.0
    excess = nd - pt
    return round(0.02 * pt + 0.008 * excess, 2)


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
    safe_write_csv(simulated_sales_df, SALES_LOG_FILE)

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


def run_thirty_day_business_simulation(
    start_date=None,
    days=30,
    seed=None,
    distributor_username=None,
    progress_callback=None,
):
    """
    30 დღის სიმულაცია: 33 ფილიალი, 200–1000 ვიზიტი/დღე, ალბათობით გაყიდვები → sales_log.csv.
    ნაშთი < 20% ნომინალური მიზნისა → ავტომატური რესტოკი (მარაგი + deliveries_log, დრო სიმულაციის დღეზე).
    """
    ensure_auth_files()
    rng = random.Random(seed if seed is not None else random.randrange(1, 10**9))
    dist_user = str(distributor_username or DEBUG_SIM_DISTRIBUTOR_USERNAME)
    mapping_data = load_mapping()
    company = str(mapping_data.get("user_company", {}).get(dist_user, "Ifkli"))

    ensure_directory_stores_have_inventory_rows()
    get_cached_store_directory_names.clear()
    df = load_data()
    if df.empty:
        return {"ok": False, "error": "Product.csv.txt ცარიელია."}

    stores = list(get_cached_store_directory_names())
    if not stores:
        return {"ok": False, "error": "stores_directory.csv ცარიელია."}

    df = ensure_data_structure(df.copy())
    nominal = {}
    for _, r in df.iterrows():
        sn = str(r["Store_Name"]).strip()
        pn = str(r["Product_Name"]).strip()
        c0 = int(pd.to_numeric(r["Current_Stock"], errors="coerce") or 0)
        nominal[(sn, pn)] = max(28, int(c0 * 1.35) + rng.randint(0, 15))

    if start_date is None:
        end_anchor = datetime.now().date()
        start_d = end_anchor - timedelta(days=days - 1)
    else:
        start_d = start_date if isinstance(start_date, date) else pd.to_datetime(start_date).date()

    sales_rows = []
    restock_count = 0
    customer_visits = 0

    for di in range(days):
        sim_date = start_d + timedelta(days=di)
        day_rng = random.Random(rng.randint(0, 2**30))

        for store in stores:
            smask = df["Store_Name"].astype(str).str.strip() == store
            idx_list = df.index[smask].tolist()
            if not idx_list:
                continue
            n_cust = day_rng.randint(200, 1000)
            customer_visits += n_cust
            for _visit in range(n_cust):
                idx = day_rng.choice(idx_list)
                cur = int(pd.to_numeric(df.at[idx, "Current_Stock"], errors="coerce") or 0)
                if cur <= 0:
                    continue
                prod = str(df.at[idx, "Product_Name"]).strip()
                p_buy = 0.035 + min(0.14, (cur / 160.0) * 0.09)
                if day_rng.random() >= p_buy:
                    continue
                qty = day_rng.randint(1, min(3, cur))
                sp = float(pd.to_numeric(df.at[idx, "Selling_Price"], errors="coerce") or 0.0)
                cp = float(pd.to_numeric(df.at[idx, "Cost_Price"], errors="coerce") or 0.0)
                if sp <= 0:
                    sp = 2.0
                if cp <= 0:
                    cp = round(sp * 0.58, 2)
                sec = day_rng.randint(0, 86399)
                sale_ts = datetime.combine(sim_date, datetime.min.time()) + timedelta(seconds=int(sec))
                rev = float(qty) * sp
                profit = float(qty) * (sp - cp)
                sales_rows.append(
                    {
                        "Timestamp": sale_ts.strftime("%Y-%m-%d %H:%M:%S"),
                        "Date": sim_date.strftime("%Y-%m-%d"),
                        "Store": store,
                        "Product": prod,
                        "Qty": int(qty),
                        "Selling_Price": sp,
                        "Cost_Price": cp,
                        "Revenue": rev,
                        "Profit": profit,
                    }
                )
                df.at[idx, "Current_Stock"] = cur - qty

        restock_ops = []
        for idx in list(df.index):
            sn = str(df.at[idx, "Store_Name"]).strip()
            pn = str(df.at[idx, "Product_Name"]).strip()
            key = (sn, pn)
            nom = int(nominal.get(key, 40))
            cur2 = int(pd.to_numeric(df.at[idx, "Current_Stock"], errors="coerce") or 0)
            thresh = max(1, int(math.ceil(0.2 * nom)))
            if cur2 < thresh:
                add = max(0, nom - cur2)
                if add > 0:
                    restock_ops.append((sn, pn, add))

        for sn, pn, add in restock_ops:
            m = (df["Store_Name"].astype(str).str.strip() == sn) & (df["Product_Name"].astype(str).str.strip() == pn)
            if not m.any():
                continue
            ridx = df[m].index[0]
            sp = float(pd.to_numeric(df.at[ridx, "Selling_Price"], errors="coerce") or 2.0)
            cp = float(pd.to_numeric(df.at[ridx, "Cost_Price"], errors="coerce") or round(sp * 0.58, 2))
            df = ensure_product_row(df, sn, pn, int(add), cp, sp)
            restock_ts = datetime.combine(sim_date, datetime.min.time()) + timedelta(
                hours=17, minutes=int(day_rng.randint(3, 55))
            )
            append_delivery_log(
                dist_user,
                company,
                sn,
                pn,
                int(add),
                sp,
                at_time=restock_ts,
                rs_status="Sim-30d-Restock",
            )
            restock_count += 1

        if progress_callback:
            progress_callback((di + 1) / float(days))

    new_sales = pd.DataFrame(sales_rows)
    if os.path.exists(SALES_LOG_FILE):
        try:
            existing = pd.read_csv(SALES_LOG_FILE)
        except Exception:
            existing = pd.DataFrame()
    else:
        existing = pd.DataFrame()
    if existing.empty:
        combined = new_sales
    else:
        combined = pd.concat([existing, new_sales], ignore_index=True)
    for col in ["Timestamp", "Date", "Store", "Product", "Qty", "Selling_Price", "Cost_Price", "Revenue", "Profit"]:
        if col not in combined.columns:
            combined[col] = "" if col in ["Timestamp", "Date", "Store", "Product"] else 0
    safe_write_csv(combined, SALES_LOG_FILE)

    df = recalc_metrics(ensure_data_structure(df))
    save_data(df)

    month_end = start_d + timedelta(days=days - 1)
    users = load_users()
    dist_row = next((u for u in users if str(u.get("username", "")) == dist_user), {})
    cr = float(dist_row.get("commission_rate", 0) or 0)
    del_df = load_delivery_log()
    ret_df = load_return_log()
    fin = calculate_salary(dist_user, del_df, ret_df, start_d, month_end, cr)
    plan = get_user_monthly_sales_target(dist_row)
    bonus = distributor_plan_bonus_estimate(plan, fin["net"])
    revenue_sim = float(new_sales["Revenue"].sum()) if not new_sales.empty else 0.0

    try:
        mtime = os.path.getmtime(SALES_LOG_FILE)
    except OSError:
        mtime = 0.0
    _cached_monthly_analytics_sales.clear()

    return {
        "ok": True,
        "days": days,
        "start_date": start_d.isoformat(),
        "end_date": month_end.isoformat(),
        "transactions": len(sales_rows),
        "revenue": revenue_sim,
        "restock_events": restock_count,
        "customer_visits": customer_visits,
        "distributor_net_deliveries": fin["net"],
        "distributor_commission": fin["commission"],
        "distributor_bonus": bonus,
        "distributor_total_earnings": round(float(fin["commission"]) + float(bonus), 2),
    }


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


def get_live_store_stock(store_name, product_name):
    live_df = get_products()
    if live_df.empty:
        return 0
    mask = (
        (live_df["Store_Name"].astype(str).str.strip() == str(store_name).strip())
        & (live_df["Product_Name"].astype(str).str.strip() == str(product_name).strip())
    )
    if not mask.any():
        return 0
    return int(pd.to_numeric(live_df.loc[mask, "Current_Stock"], errors="coerce").fillna(0).iloc[0])


def get_distributor_recommended_order(sales_df, store_name, product_name, live_stock):
    avg_daily, needed = get_restock_recommendation_qty(sales_df, product_name, store_name, live_stock)
    if live_stock < 10:
        return max(40, needed), avg_daily
    return max(needed, 0), avg_daily


def get_yesterday_sales_qty(sales_df, store_name, product_name):
    if sales_df is None or sales_df.empty:
        return 0
    target_day = datetime.now().date() - timedelta(days=1)
    sdf = sales_df.copy()
    if "Timestamp" in sdf.columns:
        day_col = sdf["Timestamp"].dt.date
    elif "timestamp" in sdf.columns:
        day_col = pd.to_datetime(sdf["timestamp"], errors="coerce").dt.date
    else:
        return 0
    mask = (
        (day_col == target_day)
        & (sdf["Store"].astype(str) == str(store_name))
        & (sdf["Product"].astype(str) == str(product_name))
    )
    if not mask.any():
        return 0
    return int(pd.to_numeric(sdf.loc[mask, "Qty"], errors="coerce").fillna(0).sum())


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

def render_distributor_dashboard(auth_user, mapping_data, sales_df_all):
    """დისტრიბუტორის დაფა (ლოკალური კომპონენტი): გეგმა, სახელფასო, მაღაზიები, ძიება, მარშრუტი, ჟურნალები."""
    st.title("🚚 დისტრიბუტორის სუპერ დაფა")
    st.caption(
        "ერთიანი სისტემა: მარაგი — "
        f"`{FILE_NAME}` · მიწოდებები — `deliveries_log.csv` · დაბრუნებები — `returns_log.csv`. "
        "ყველა ფინანსური მეტრიკა იკითხება იგივე ჟურნალებიდან; საკომისიო — პროფილიდან."
    )

    ensure_demo_sales_log_entries_today()
    sales_df_all = load_sales_log()

    fresh_df = load_data()
    if not fresh_df.empty:
        st.session_state.df = recalc_metrics(ensure_data_structure(fresh_df.copy()))
    distributor_live_df = fresh_df.copy() if not fresh_df.empty else fresh_df
    mapping_data = load_mapping()
    if not distributor_live_df.empty:
        distributor_live_df["Current_Stock"] = pd.to_numeric(distributor_live_df["Current_Stock"], errors="coerce").fillna(0)
    user_company = mapping_data.get("user_company", {}).get(auth_user.get("username", ""), auth_user.get("company", ""))
    commission_rate = float(auth_user.get("commission_rate", 0) or 0)
    today = datetime.now().date()
    dist_user = str(auth_user.get("username", ""))
    ensure_demo_delivery_samples_today(dist_user, str(user_company or auth_user.get("company", "") or "Ifkli"))
    delivery_df = load_delivery_log()
    returns_df = load_return_log()
    # ერთი rerun = ერთი წაკითხვა დისკიდან; მიწოდების შემდეგ st.rerun() ახლდება გეგმას, სახელფასოს და ჟურნალს ერთდროულად.
    st.session_state["distributor_logs_synced_at"] = datetime.now().isoformat(timespec="seconds")

    _now_d = datetime.now()
    _today_d = _now_d.date()
    _week_start = _today_d - timedelta(days=_now_d.weekday())
    _month_start = _today_d.replace(day=1)

    my_deliveries_today = delivery_df[
        (delivery_df["username"].astype(str) == dist_user)
        & (delivery_df["timestamp"].dt.date == today)
    ] if not delivery_df.empty else delivery_df
    deliveries_count_today = len(my_deliveries_today)

    _today_fin = calculate_salary(
        dist_user, delivery_df, returns_df, today, today, commission_rate
    )
    total_sales_today_gross = _today_fin["gross"]
    returns_today_amt = _today_fin["returns"]
    total_sales_today_net = _today_fin["net"]
    commission_today = _today_fin["commission"]

    today_sales_log_revenue = 0.0
    if sales_df_all is not None and not sales_df_all.empty and "Timestamp" in sales_df_all.columns:
        today_sales_log_revenue = float(
            pd.to_numeric(
                sales_df_all.loc[sales_df_all["Timestamp"].dt.date == today, "Revenue"],
                errors="coerce",
            ).fillna(0).sum()
        )

    _dir_stores_df = load_stores_directory_df()
    assigned_stores, priority_scores = build_distributor_route_context(
        distributor_live_df, route_alert_threshold=5, stores_directory_df=_dir_stores_df
    )

    visited_stores_today = (
        set(my_deliveries_today["store"].astype(str).unique())
        if not my_deliveries_today.empty
        else set()
    )
    remaining_stores_visit = len([s for s in assigned_stores if str(s) not in visited_stores_today])

    low_stock_alerts = get_low_stock_alerts(distributor_live_df, threshold=5)

    if not _dir_stores_df.empty:
        st.caption(
            f"📍 `stores_directory.csv`: **{len(_dir_stores_df)}** ფილიალი (ქართული ქსელი) · პროდუქტის ფაილში **{len(assigned_stores)}** უნიკალური მაღაზია მარშრუტისთვის."
        )

    # --- ზედა: თვიური გეგმა + სახელფასო (ერთი წყარო: deliveries + returns) ---
    st.markdown("### 💼 გამომუშავება და თვიური გეგმა")
    with st.container(border=True):
        _users_fresh = load_users()
        _me_fresh = next(
            (u for u in _users_fresh if str(u.get("username", "")) == dist_user),
            dict(auth_user) if auth_user else {},
        )
        _plan_target = get_user_monthly_sales_target(_me_fresh)
        _plan_month_start = _month_start
        _plan_today_d = _today_d
        _month_fin = calculate_salary(
            dist_user, delivery_df, returns_df, _plan_month_start, _plan_today_d, commission_rate
        )
        _plan_progress_net = _month_fin["net"]

        st.caption(
            "თვიური შესრულება ითვლება **ნეტო მიწოდებად** "
            "(deliveries_log − returns_log), იგივე ლოგიკით, რაც სახელფასო კალკულატორში."
        )
        pm1, pm2, pm3 = st.columns(3)
        with pm1:
            st.metric(
                "გეგმის თანხა (₾)",
                format_gel_currency(_plan_target) if _plan_target > 0 else "—",
                help="თვიური გეგმა — users.csv (monthly_sales_target).",
            )
        with pm2:
            st.metric(
                "თვის ნეტო შესრულება (₾)",
                format_gel_currency(_plan_progress_net),
                help="მიმდინარე თვეში: მიწოდებების ჯამი − დაბრუნებების ჯამი.",
            )
        with pm3:
            st.metric(
                "დღეს: ნეტო მიწოდება / საკომისიო",
                f"{format_gel_currency(total_sales_today_net)} / {format_gel_currency(commission_today)}",
                help=f"მიწოდებები − დაბრუნება. საკომისიო: {commission_rate * 100:.2f}%",
            )

        if _plan_target <= 0:
            st.info(
                "თვიური გეგმა არ არის დაყენებული. ადმინისტრატორმა შეგიძლიათ დააყენოთ "
                "გვერდზე «🤝 დისტრიბუტორების მართვა»."
            )
            st.progress(0)
            st.caption("გეგმა: არ არის განსაზღვრული")
        else:
            _plan_ratio = min(1.0, float(_plan_progress_net) / float(_plan_target))
            st.progress(_plan_ratio)
            st.caption(f"შესრულება (ნეტო მიწოდება): {_plan_ratio * 100:.1f}%")
            _remaining_plan = max(0.0, float(_plan_target) - float(_plan_progress_net))
            st.markdown(
                f"**გეგმამდე დარჩენილი თანხა:** {format_gel_currency(_remaining_plan)}"
            )
            if _plan_progress_net >= _plan_target:
                st.success("🎉 გეგმა შესრულებულია! შესანიშნავი მუშაობა!")

        st.divider()
        dk1, dk2, dk3, dk4, dk5 = st.columns(5)
        with dk1:
            st.metric("💰 დღიური ჯამი (sales_log)", format_gel_currency(today_sales_log_revenue))
        with dk2:
            st.metric("🚚 დღევანდელი მიწოდებები (რაოდ.)", f"{deliveries_count_today}")
        with dk3:
            st.metric("📤 მიწოდება ბრუტო (₾)", format_gel_currency(total_sales_today_gross))
        with dk4:
            st.metric("↩️ დაბრუნება დღეს (₾)", format_gel_currency(returns_today_amt))
        with dk5:
            st.metric("🏪 დარჩენილი ვიზიტი", f"{remaining_stores_visit}")
        st.caption(f"კომპანია: **{user_company or '—'}** · მომხმარებელი: **{dist_user}**")

        st.divider()
        st.markdown("##### 💼 სახელფასო კალკულატორი")
        st.caption(
            f"გამომუშავება = (მიწოდება − დაბრუნება) × **{commission_rate * 100:.2f}%** — იგივე ფაილები, რაც ზედა გეგმაში."
        )

        def _render_salary_period(start_d, end_d, period_desc):
            fin = calculate_salary(
                dist_user, delivery_df, returns_df, start_d, end_d, commission_rate
            )
            g, r, n, e = fin["gross"], fin["returns"], fin["net"], fin["commission"]
            b1, b2, b3 = st.columns(3)
            with b1:
                st.caption("🚚 მიწოდებების ჯამი")
                st.markdown(f"**{format_gel_currency(g)}**")
            with b2:
                st.caption("↩️ დაბრუნებები")
                st.markdown(f"**{format_gel_currency(r)}**")
            with b3:
                st.caption("➖ ნეტო (საკომისიოს ბაზა)")
                st.markdown(f"**{format_gel_currency(n)}**")
            st.caption(period_desc)
            with st.container(border=True):
                st.markdown(
                        """
                    <div style="padding:0.5rem 0.75rem;background:linear-gradient(90deg,#e8f5e9,#f1f8e9);border-radius:8px;border:1px solid #a5d6a7;margin-bottom:0.5rem;">
                        <span style="color:#2e7d32;font-weight:600;">💚 საკომისიო-ს გამოთვლილი ჯამი (ლარი)</span>
                    </div>
                        """,
                    unsafe_allow_html=True,
                )
                st.metric(
                    "შენი გამომუშავება (₾)",
                    format_gel_currency(e),
                    help=f"({format_gel_currency(n)}) × {commission_rate * 100:.2f}%",
                )
            with st.expander("დეტალური განმარტება"):
                st.markdown(
                    f"- მიწოდებების ჯამი: **{format_gel_currency(g)}**\n"
                    f"- დაბრუნებების ჯამი: **{format_gel_currency(r)}**\n"
                    f"- ნეტო: **{format_gel_currency(n)}**\n"
                    f"- საკომისიო: **{commission_rate * 100:.2f}%**"
                )

        _tab_today, _tab_week, _tab_month = st.tabs(["📅 დღეს", "📆 ამ კვირაში", "🗓️ ამ თვეში"])
        with _tab_today:
            _render_salary_period(_today_d, _today_d, f"პერიოდი: {_today_d.isoformat()} (დღეს).")
        with _tab_week:
            _render_salary_period(
                _week_start,
                _today_d,
                f"პერიოდი: კვირის დასაწყისი ({_week_start}) — დღეს ({_today_d}).",
            )
        with _tab_month:
            _render_salary_period(
                _month_start,
                _today_d,
                f"პერიოდი: თვის პირველი რიცხვი ({_month_start}) — დღეს ({_today_d}).",
            )

    st.divider()

    # --- შუა: ფილტრები (ძიება — მარშრუტის ჩანართში, ფილიალების სიის ზემოთ) ---
    st.markdown("### 🔎 ფილტრები")
    with st.container(border=True):
        store_search_term = ""
        filter_priority_only = st.checkbox(
            "მხოლოდ პრიორიტეტული",
            help="ფილიალები, სადაც მინიმუმ ერთი SKU-ს ნაშთი < 5",
            key="distributor_route_priority_only",
        )
        low_stock_threshold_ui = st.slider(
            "დაბალი მარაგის ზღვარი (გაფრთხილების ცხრილი)",
            min_value=1,
            max_value=20,
            value=5,
            key="distributor_low_stock_threshold_ui",
        )

    filtered_stores = filter_distributor_visible_stores(
        assigned_stores,
        priority_scores,
        store_search_term,
        filter_priority_only,
        route_alert_threshold=5,
    )

    st.divider()
    st.subheader("📦 მარაგის გაფრთხილება — ნაშთი ზღვრის ქვემოთ")
    low_stock_alerts_ui = get_low_stock_alerts(distributor_live_df, threshold=int(low_stock_threshold_ui))
    if distributor_live_df.empty:
        st.info("პროდუქციის მონაცემები ვერ მოიძებნა.")
    elif low_stock_alerts_ui.empty:
        st.success("✅ ამ ზღვარზე დაბალი ნაშთი არ ფიქსირდება.")
    else:
        st.caption(f"⚠️ სულ {len(low_stock_alerts_ui)} ჩანაწერი · ზღვარი: **{low_stock_threshold_ui}**.")
        _warn_df = low_stock_alerts_ui[["Store_Name", "Product_Name", "Current_Stock"]].copy()
        _warn_df["Current_Stock"] = pd.to_numeric(_warn_df["Current_Stock"], errors="coerce").fillna(0).astype(int)
        _warn_disp = _warn_df.rename(
            columns={
                "Store_Name": "ფილიალი",
                "Product_Name": "პროდუქტი",
                "Current_Stock": "ნაშთი",
            }
        )

        def _distributor_warn_red(_row):
            return ["background-color: #ffe5e5; color: #7a1515"] * len(_row)

        _warn_styled = _warn_disp.style.apply(_distributor_warn_red, axis=1)
        st.dataframe(
            _warn_styled,
            use_container_width=True,
            hide_index=True,
            height=min(360, 64 + 34 * len(_warn_disp)),
        )

    st.divider()
    st.markdown("### 🗺️ მარშრუტი, ბორტი და ჟურნალი")
    st.text_input("🔍 მოძებნე მაღაზია...", key="distributor_quick_branch_search")
    branch_find = (st.session_state.get("distributor_quick_branch_search") or "").strip().lower()
    route_list_stores = filtered_stores
    if branch_find:
        route_list_stores = [s for s in filtered_stores if store_matches_search(s, branch_find)]

    route_tab, stock_tab, log_tab = st.tabs(["🗺️ მარშრუტი და მიწოდება", "📦 ბორტის ნაშთი", "📜 ბოლო მიწოდებები"])

    with route_tab:
        st.markdown("##### 🛣️ დღევანდელი მარშრუტი (გაფართოებული სია)")
        st.caption(
            "ფილიალები დალაგებულია პრიორიტეტით — პირველ რიგში ის მაღაზიები, სადაც მეტია დაბალი ნაშთის პროდუქტი. ძიება — ზუსტად ამ სიის ზემოთ."
        )

        if not route_list_stores:
            if branch_find:
                st.info("ამ ძიებით მაღაზია ვერ მოიძებნა.")
            else:
                st.info("მარშრუტზე მაღაზიები არ არის მინიჭებული.")
        else:
            for store_name in route_list_stores:
                _da, _dp = get_store_contact_info(store_name)
                with st.container(border=True):
                    st.markdown(f"### 🏪 {store_name}")
                    st.caption(
                        f"მისამართი: {_da}"
                        + (f" · ტელ.: {_dp}" if _dp else "")
                    )
                    if priority_scores.get(store_name, 0) > 0:
                        st.error("📌 პრიორიტეტული ვიზიტი — კრიტიკული ნაშთი ფიქსირდება.")
                    if st.button("ვიზიტის დაწყება", key=f"route_start_{store_name}", use_container_width=True):
                        st.session_state["active_route_store"] = store_name
                        st.rerun()

            active_store = st.session_state.get("active_route_store", "")
            if active_store:
                st.divider()
                st.subheader(f"🚚 მიწოდების ფორმა — {active_store}")
                store_items = low_stock_alerts[low_stock_alerts["Store_Name"].astype(str) == str(active_store)]
                if store_items.empty:
                    store_items = distributor_live_df[distributor_live_df["Store_Name"].astype(str) == str(active_store)].head(8)
                if store_items.empty:
                    st.info("პროდუქტები ვერ მოიძებნა ამ ვიზიტისთვის.")
                else:
                    _stock_cols = ["Product_Name", "Current_Stock"]
                    if "Selling_Price" in distributor_live_df.columns:
                        _stock_cols.append("Selling_Price")
                    live_store_stock_df = distributor_live_df[
                        distributor_live_df["Store_Name"].astype(str) == str(active_store)
                    ][_stock_cols].copy()
                    live_store_stock_df["Current_Stock"] = pd.to_numeric(
                        live_store_stock_df["Current_Stock"], errors="coerce"
                    ).fillna(0)
                    if "Selling_Price" not in live_store_stock_df.columns:
                        live_store_stock_df["Selling_Price"] = 0.0
                    live_store_stock_df["Selling_Price"] = pd.to_numeric(
                        live_store_stock_df["Selling_Price"], errors="coerce"
                    ).fillna(0.0)
                    if not live_store_stock_df.empty:
                        st.caption("მიმდინარე ნაშთი ფაილიდან: Product.csv.txt")
                        product_stock_search = st.text_input(
                            "🔎 პროდუქტის ძიება (ამ ცხრილისთვის)",
                            placeholder="ძიება პროდუქტის სახელით...",
                            key=f"distributor_product_stock_search_{active_store}",
                        ).strip()
                        stock_display = live_store_stock_df.rename(
                            columns={
                                "Product_Name": "პროდუქტის დასახელება",
                                "Current_Stock": "ნაშთი",
                                "Selling_Price": "ფასი",
                            }
                        )
                        stock_display["ნაშთი"] = pd.to_numeric(
                            stock_display["ნაშთი"], errors="coerce"
                        ).fillna(0).astype(int)
                        stock_display["ფასი"] = pd.to_numeric(
                            stock_display["ფასი"], errors="coerce"
                        ).fillna(0.0)
                        stock_display["სტატუსი"] = stock_display["ნაშთი"].apply(
                            lambda x: "დაბალი მარაგი" if int(x) < 5 else "ნორმალური"
                        )
                        _show_cols = [
                            "პროდუქტის დასახელება",
                            "ნაშთი",
                            "ფასი",
                            "სტატუსი",
                        ]
                        stock_display = stock_display[_show_cols]
                        if product_stock_search:
                            _mask = stock_display["პროდუქტის დასახელება"].astype(str).str.contains(
                                product_stock_search, case=False, na=False
                            )
                            stock_display = stock_display[_mask]
                        stock_view_mode = st.radio(
                            "ნაშთის ცხრილის ხედი",
                            ["ცხრილი", "ბარათები"],
                            horizontal=True,
                            label_visibility="collapsed",
                            key=f"distributor_stock_view_mode_{active_store}",
                        )
                        if stock_display.empty:
                            st.info("ამ ძიებით პროდუქტი ვერ მოიძებნა.")
                        elif stock_view_mode == "ცხრილი":

                            def _distributor_stock_row_style(row):
                                if int(row["ნაშთი"]) < 5:
                                    return ["background-color: #ffcccc"] * len(row)
                                return [""] * len(row)

                            _styled = (
                                stock_display.style.apply(_distributor_stock_row_style, axis=1).format(
                                    {"ფასი": "{:.2f}"}
                                )
                            )
                            st.dataframe(
                                _styled,
                                use_container_width=True,
                                hide_index=True,
                                height=min(420, 80 + 36 * len(stock_display)),
                            )
                        else:
                            for _i, _row in stock_display.iterrows():
                                _low = int(_row["ნაშთი"]) < 5
                                with st.container(border=True):
                                    if _low:
                                        st.markdown(
                                            '<p style="margin:0;padding:6px 8px;background:#ffcccc;border-radius:6px;font-size:0.9rem;">'
                                            "⚠️ დაბალი მარაგი"
                                            "</p>",
                                            unsafe_allow_html=True,
                                        )
                                    _r1, _r2 = st.columns([3, 2])
                                    with _r1:
                                        st.markdown(
                                            f"**{_row['პროდუქტის დასახელება']}**"
                                        )
                                        st.caption(
                                            f"სტატუსი: {_row['სტატუსი']} · ფასი: ₾{float(_row['ფასი']):.2f}"
                                        )
                                    with _r2:
                                        st.metric("ნაშთი", int(_row["ნაშთი"]))
                    with st.form("visit_delivery_confirm_form"):
                        row_items = []
                        for i, (_, item) in enumerate(store_items.iterrows()):
                            product_name = str(item["Product_Name"])
                            live_match = live_store_stock_df[
                                live_store_stock_df["Product_Name"].astype(str) == product_name
                            ] if not live_store_stock_df.empty else pd.DataFrame()
                            live_stock = int(live_match["Current_Stock"].iloc[0]) if not live_match.empty else 0
                            rec_qty, avg_daily = get_distributor_recommended_order(
                                sales_df_all, active_store, product_name, live_stock
                            )
                            yesterday_sales = get_yesterday_sales_qty(sales_df_all, active_store, product_name)
                            truck_qty = get_truck_qty(auth_user.get("username", ""), product_name)
                            c1, c2 = st.columns([2, 1])
                            if live_stock < 10:
                                c1.error(f"🔴 {product_name} — კრიტიკული ნაშთი")
                            else:
                                c1.markdown(f"**{product_name}**")
                            c1.caption(f"🛒 მაღაზიის ნაშთი: {live_stock}")
                            c1.caption(f"📈 გუშინდელი გაყიდვა: {yesterday_sales}")
                            c1.caption(f"📦 შემოთავაზებული რაოდენობა: {rec_qty} | საშუალო დღიური გაყიდვა: {avg_daily:.1f}")
                            c1.caption(f"🚚 ბორტზე ხელმისაწვდომი: {truck_qty}")
                            delivered_qty = c1.number_input(
                                f"{product_name} — მიწოდებული რაოდენობა (ხელით შესაცვლელი)",
                                min_value=0,
                                value=max(0, int(rec_qty)),
                                step=1,
                                key=f"deliver_qty_{i}_{product_name}",
                            )
                            issue = c2.selectbox(
                                f"შენიშვნა ({product_name})",
                                ["არ არის", "დაზიანება", "დანაკლისი", "დაბრუნება"],
                                key=f"deliver_issue_{i}_{product_name}",
                            )
                            row_items.append(
                                {
                                    "product": str(product_name).strip(),
                                    "qty": int(delivered_qty),
                                    "issue": str(issue) if issue is not None else "",
                                    "unit_price": float(item.get("Selling_Price", 0.0)),
                                }
                            )
                        notes = st.text_area("შენიშვნები", placeholder="დაზიანება/დანაკლისი/დაბრუნება")
                        confirm_delivery = st.form_submit_button("მიწოდების დადასტურება", use_container_width=True)

                    if confirm_delivery:
                        valid_rows = [r for r in row_items if r["qty"] > 0 and str(r.get("product", "")).strip()]
                        if not valid_rows:
                            st.warning("მიუთითეთ მინიმუმ ერთი პროდუქტის მიწოდებული რაოდენობა.")
                        else:
                            full_df = st.session_state.df.copy()
                            notes_clean = notes.strip() if isinstance(notes, str) else ""
                            for r in valid_rows:
                                qty_val = int(float(r.get("qty", 0) or 0))
                                unit_price_val = float(r.get("unit_price", 0.0) or 0.0)
                                issue_val = str(r.get("issue", "") or "")
                                current_truck = get_truck_qty(auth_user.get("username", ""), r["product"])
                                if qty_val > current_truck:
                                    st.warning(f"{r['product']}: ბორტზე მხოლოდ {current_truck} ერთეულია. მიწოდება შეზღუდდა.")
                                    qty_val = current_truck
                                if qty_val <= 0:
                                    continue
                                full_df = ensure_product_row(
                                    full_df,
                                    active_store,
                                    r["product"],
                                    qty_val,
                                    0.0,
                                    unit_price_val,
                                )
                                set_truck_qty(auth_user.get("username", ""), r["product"], current_truck - qty_val)
                                append_delivery_log(
                                    username=auth_user.get("username", ""),
                                    company=user_company,
                                    store=active_store,
                                    product=r["product"],
                                    qty=qty_val,
                                    unit_price=unit_price_val,
                                    rs_status="Pending on RS.GE",
                                )
                                if issue_val == "დაბრუნება":
                                    append_return_log(
                                        username=auth_user.get("username", ""),
                                        company=user_company,
                                        store=active_store,
                                        product=r["product"],
                                        qty=qty_val,
                                        unit_price=unit_price_val,
                                        reason="დაბრუნება",
                                        note=notes_clean,
                                    )
                                if issue_val and issue_val != "არ არის" or notes_clean:
                                    append_discrepancy_log(
                                        distributor=str(auth_user.get("username", "")),
                                        company=user_company,
                                        store=active_store,
                                        product=r["product"],
                                        ordered_qty=qty_val,
                                        confirmed_qty=qty_val,
                                        reason=f"შენიშვნა={issue_val if issue_val else 'არ არის'} | {notes_clean}",
                                        corrected_by=str(auth_user.get("username", "")),
                                    )
                            full_df = recalc_metrics(full_df)
                            save_products(full_df)
                            st.session_state.df = full_df
                            st.success("მიწოდება დადასტურდა და ბაზა განახლდა.")
                            st.rerun()

    with stock_tab:
        st.markdown("##### 📦 ბორტის ნაშთი და დატვირთვა")
        st.caption("თქვენი ბორტის ნაშთი და დაგროვილი მიწოდებები პროდუქტის მიხედვით.")
        truck_df = load_truck_stock()
        truck_df = truck_df[truck_df["username"].astype(str) == str(auth_user.get("username", ""))] if not truck_df.empty else truck_df
        if truck_df.empty:
            st.info("ბორტის ნაშთისთვის ჩანაწერები არ არის.")
        else:
            delivered_by_product = (
                delivery_df[delivery_df["username"].astype(str) == str(auth_user.get("username", ""))]
                .groupby("product", as_index=False)["qty"].sum()
                .rename(columns={"product": "პროდუქტი", "qty": "ჯამურად მიწოდებული"})
                if not delivery_df.empty
                else pd.DataFrame(columns=["პროდუქტი", "ჯამურად მიწოდებული"])
            )
            truck_view = truck_df.rename(columns={"product": "პროდუქტი", "qty": "ბორტის ნაშთი"})[["პროდუქტი", "ბორტის ნაშთი"]]
            delivered_by_product = delivered_by_product.merge(truck_view, on="პროდუქტი", how="outer").fillna(0)
            delivered_by_product["დილის დატვირთვა (სიმულაცია)"] = delivered_by_product["ჯამურად მიწოდებული"] + delivered_by_product["ბორტის ნაშთი"]
            st.dataframe(
                delivered_by_product[["პროდუქტი", "დილის დატვირთვა (სიმულაცია)", "ჯამურად მიწოდებული", "ბორტის ნაშთი"]],
                use_container_width=True,
            )

    with log_tab:
        st.markdown("##### 📜 ბოლო მიწოდებები (`deliveries_log.csv`)")
        my_all_deliveries = (
            delivery_df[delivery_df["username"].astype(str) == dist_user].copy()
            if not delivery_df.empty
            else delivery_df
        )
        if my_all_deliveries.empty:
            st.info("თქვენი მიწოდების ჩანაწერი ჯერ არ არის.")
        else:
            _hist = my_all_deliveries.sort_values("timestamp", ascending=False).head(5)
            _cols = ["timestamp", "store", "product", "qty", "total_sales"]
            if "rs_status" in _hist.columns:
                _cols = _cols + ["rs_status"]
            _hist_display = _hist[_cols].copy()
            _rename_map = {
                "timestamp": "დრო",
                "store": "ფილიალი",
                "product": "პროდუქტი",
                "qty": "რაოდენობა",
                "total_sales": "ჯამი (₾)",
                "rs_status": "RS სტატუსი",
            }
            _hist_display = _hist_display.rename(columns=_rename_map)
            st.dataframe(_hist_display, use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("##### ↩️ ბოლო დაბრუნებები (`returns_log.csv`)")
        my_returns = (
            returns_df[returns_df["username"].astype(str) == dist_user].copy()
            if not returns_df.empty
            else returns_df
        )
        if my_returns.empty:
            st.info("დაბრუნების ჩანაწერი ჯერ არ არის (მიწოდების ფორმაში აირჩიეთ «დაბრუნება» ჩანაწერისთვის).")
        else:
            _rh = my_returns.sort_values("timestamp", ascending=False).head(5)
            _rcols = ["timestamp", "store", "product", "qty", "total_return", "reason"]
            _rh = _rh[[c for c in _rcols if c in _rh.columns]]
            _rh_disp = _rh.rename(
                columns={
                    "timestamp": "დრო",
                    "store": "ფილიალი",
                    "product": "პროდუქტი",
                    "qty": "რაოდენობა",
                    "total_return": "ჯამი (₾)",
                    "reason": "მიზეზი",
                }
            )
            st.dataframe(_rh_disp, use_container_width=True, hide_index=True)


if 'df' not in st.session_state:
    st.session_state.df = get_products()

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


def _run_main_app_after_login():
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
    # დისტრიბუტორი: ყოველ Streamlit rerun-ზე სრული განახლება დისკიდან (Product.csv.txt + load_data-ის სინქი)
    if auth_user and str(auth_user.get("role")) == "Distributor":
        _disk_df = load_data()
        if not _disk_df.empty:
            st.session_state.df = _disk_df.copy()
    sales_df_all = load_sales_log()
    audit_df_all = load_audit_log()
    mapping_data = load_mapping()
    master_df = st.session_state.df
    
    # --- მთავარი გვერდითა მენიუ ---
    st.sidebar.title("🎛️ მართვის პანელი")
    st.sidebar.caption(
        f"მომხმარებელი: {auth_user.get('username')} | როლი: {auth_user.get('role')}"
    )
    if st.sidebar.button("გასვლა", use_container_width=True):
        st.session_state.auth_user = None
        st.rerun()
    
    if auth_user and str(auth_user.get("role")) == "Market":
        _ms_shops = list(get_cached_store_directory_names())
        st.sidebar.markdown("---")
        st.sidebar.subheader("🏪 Market — მაღაზიის არჩევა")
        if not _ms_shops:
            st.sidebar.warning("stores_directory.csv ცარიელია.")
        else:
            st.sidebar.selectbox(
                "რომელი ფილიალი გინდათ (33 ქსელი)",
                _ms_shops,
                key="market_store_pick_main",
            )
            st.sidebar.caption(f"საერთო ინვენტარი: `{SHARED_INVENTORY_FILE}` (= inventory / Product)")
    
    role = auth_user.get("role")
    if role == "Super_Admin":
        pages = [
            "🏢 კომპანიის მართვა",
            "🌍 გლობალური ანალიტიკა",
            "📊 თვის ანალიტიკა",
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
    elif role in ["Company_Operator", "Retail_Operator", "Supplier_Operator"]:
        pages = ["🛠 ოპერატორის პანელი", "📈 პროდუქტის ეფექტიანობა"]
    elif role == "Store_Manager":
        pages = ["🏠 მაღაზიის პანელი", "🛒 გაყიდვები"]
    elif role == "Market":
        pages = ["🏪 ბაზრის პანელი", "📊 თვის ანალიტიკა"]
    elif role == "Distributor":
        pages = ["🚚 აუცილებელი მიწოდებები", "📊 თვის ანალიტიკა"]
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
    
    df, sales_df_all = apply_role_filters(master_df, sales_df_all, auth_user, mapping_data)
    role_scope = get_role_scope(auth_user, mapping_data)
    df, sales_df_all, audit_df_all = apply_global_data_lock(df, sales_df_all, audit_df_all, role_scope)
    
    # 2. ანალიტიკური გათვლები (ფილტრის შემდეგ)
    sales_cols = [f"Sales_Day{i}" for i in range(1, 8)]
    for col in sales_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    df["საშ. დღიური გაყიდვა"] = df[sales_cols].mean(axis=1).round(1)
    df["დარჩენილი დღე"] = (df["Current_Stock"] / df["საშ. დღიური გაყიდვა"].replace(0, 0.1)).round(1)
    df["დღე ვადის გასვლამდე"] = (pd.to_datetime(df["Expiry_Date"]) - datetime.now()).dt.days
    
    if role == "Distributor" and page == "🚚 აუცილებელი მიწოდებები":
        st.sidebar.markdown("---")
        _dist_debug_on = st.sidebar.checkbox("🔧 Debug Mode", key="distributor_debug_mode_ui")
        if _dist_debug_on:
            st.sidebar.caption("ტესტირება / სიმულაცია")
            _dbg_rate = float(auth_user.get("commission_rate", 0) or 0)
            _forecast = st.sidebar.slider(
                "გაყიდვების პროგნოზი (₾)",
                min_value=0,
                max_value=500000,
                value=0,
                step=500,
                key="distributor_debug_forecast_slider",
            )
            _extra_commission = float(_forecast) * _dbg_rate
            st.sidebar.markdown(
                "ამ თანხის გაყიდვის შემთხვევაში, თქვენი დამატებითი გამომუშავება იქნება "
                f"**{format_gel_currency(_extra_commission)}**."
            )
            st.sidebar.caption(
                f"გაანგარიშება: პროგნოზი × საკომისიო ({_dbg_rate * 100:.2f}%)."
            )
            if str(auth_user.get("username", "")) != DEBUG_SIM_DISTRIBUTOR_USERNAME:
                st.sidebar.warning(
                    f"«Test Mode» ჩანაწერებს ამატებს მხოლოდ **{DEBUG_SIM_DISTRIBUTOR_USERNAME}**-ის ჟურნალში. "
                    "პროგრესის ზოლის სიცოცხლის ტესტისთვის შედით ამ მომხმარებლით."
                )
            if st.sidebar.button(
                "🧪 Test Mode — სიმულაციის ჩანაწერები",
                use_container_width=True,
                key="distributor_debug_test_mode_btn",
                help=f"ამატებს 3–4 გაყიდვას და 1 დაბრუნებას: {DEBUG_SIM_DISTRIBUTOR_USERNAME}",
            ):
                if str(auth_user.get("username", "")) != DEBUG_SIM_DISTRIBUTOR_USERNAME:
                    st.sidebar.error("შედით სისტემაში როგორც distributor_a.")
                else:
                    _ms = datetime.now().date().replace(day=1)
                    _me = datetime.now().date()
                    _del_b = load_delivery_log()
                    _ret_b = load_return_log()
                    _users_b = load_users()
                    _row_b = next(
                        (u for u in _users_b if str(u.get("username", "")) == DEBUG_SIM_DISTRIBUTOR_USERNAME),
                        {},
                    )
                    _plan_b = get_user_monthly_sales_target(_row_b)
                    _dbg_rate_b = float(_row_b.get("commission_rate", 0) or 0)
                    _net_b = calculate_salary(
                        DEBUG_SIM_DISTRIBUTOR_USERNAME, _del_b, _ret_b, _ms, _me, _dbg_rate_b
                    )["net"]
                    debug_seed_distributor_a_test_logs(mapping_data, master_df)
                    _del_a = load_delivery_log()
                    _ret_a = load_return_log()
                    _net_a = calculate_salary(
                        DEBUG_SIM_DISTRIBUTOR_USERNAME, _del_a, _ret_a, _ms, _me, _dbg_rate_b
                    )["net"]
                    if _plan_b > 0:
                        _r0 = float(_net_b) / float(_plan_b)
                        _r1 = float(_net_a) / float(_plan_b)
                        try:
                            if _r0 < 0.5 <= _r1:
                                st.toast("მიღწეულია გეგმის 50% (სიმულაცია).", icon="🎯")
                            if _r0 < 1.0 <= _r1:
                                st.toast("გეგმა სრულად შესრულებულია (სიმულაცია)!", icon="🎉")
                        except Exception:
                            if _r0 < 0.5 <= _r1:
                                st.sidebar.success("🎯 მიღწეულია გეგმის 50% (სიმულაცია).")
                            if _r0 < 1.0 <= _r1:
                                st.sidebar.success("🎉 გეგმა სრულად შესრულებულია (სიმულაცია)!")
                    st.rerun()
    
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
    
        if st.sidebar.button("🚀 Run 30-Day Simulation", use_container_width=True, key="btn_run_30d_sim"):
            prog = st.sidebar.progress(0.0)
    
            def _tick(p):
                prog.progress(min(1.0, float(p)))
    
            try:
                summary = run_thirty_day_business_simulation(progress_callback=_tick)
            finally:
                prog.empty()
            if summary.get("ok"):
                st.session_state.df = load_data()
                get_cached_store_directory_names.clear()
                st.sidebar.success(
                    f"დასრულდა: {summary.get('transactions', 0)} გაყიდვა, "
                    f"₾ {summary.get('revenue', 0):,.2f}, რესტოკი: {summary.get('restock_events', 0)}."
                )
                st.rerun()
            else:
                st.sidebar.error(str(summary.get("error", "შეცდომა")))
    
    low_stock_threshold = 5
    low_stock_items = df[df["Current_Stock"] < low_stock_threshold]
    if not low_stock_items.empty:
        st.error(f"⚠️ კრიტიკული მარაგი: {len(low_stock_items)} პროდუქტი იწურება!")
        with st.expander("იხილეთ სია"):
            st.write(low_stock_items[["Product_Name", "Current_Stock"]])
    
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
                safe_write_csv(map_df, MAPPING_FILE)
            st.success(f"{selected_user} მიბმულია ფილიალზე: {selected_branch}")
            st.rerun()
    
        st.divider()
        st.subheader("დაზიანება/დანაკარგის მონიტორინგი დისტრიბუტორების მიხედვით")
        discrepancy_df = load_discrepancy_log()
        if discrepancy_df.empty:
            st.info("დანაკარგის ჩანაწერები ჯერ არ არსებობს.")
        else:
            top_distributors = (
                discrepancy_df.groupby("distributor", as_index=False)["difference"].sum()
                .sort_values("difference", ascending=False)
                .rename(columns={"distributor": "დისტრიბუტორი", "difference": "ჯამური დანაკარგი"})
            )
            st.dataframe(top_distributors, use_container_width=True)
    
        st.divider()
        st.subheader("Correction Log")
        correction_df = load_correction_log()
        if correction_df.empty:
            st.info("კორექციის ჩანაწერები არ არის.")
        else:
            corr_display = correction_df[["timestamp", "distributor", "store", "product", "original_qty", "updated_qty", "difference", "reason", "updated_by"]].rename(
                columns={
                    "timestamp": "დრო",
                    "distributor": "დისტრიბუტორი",
                    "store": "ფილიალი",
                    "product": "პროდუქტი",
                    "original_qty": "საწყისი",
                    "updated_qty": "განახლებული",
                    "difference": "სხვაობა",
                    "reason": "მიზეზი",
                    "updated_by": "განაახლა",
                }
            )
            st.dataframe(corr_display.sort_values("დრო", ascending=False), use_container_width=True)
    
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
        st.caption("Apex სტილის მარშრუტი: მაღაზიების სია, აქტიური მიწოდება და ბორტის ნაშთი.")
        user_company = mapping_data.get("user_company", {}).get(auth_user.get("username", ""), auth_user.get("company", ""))
        commission_rate = float(auth_user.get("commission_rate", 0) or 0)
        sales_df = sales_df_all.copy()
        low_stock_alerts = get_low_stock_alerts(df, threshold=5)
        pending_df = load_pending_deliveries()
        delivery_df = load_delivery_log()
    
        st.subheader("🗺️ მაღაზიების სია")
        assigned_stores = sorted(df["Store_Name"].astype(str).unique().tolist()) if not df.empty else []
        if not assigned_stores:
            st.info("მინიჭებული ვიზიტები არ არის.")
        else:
            for store_name in assigned_stores:
                has_pending = False
                if not pending_df.empty:
                    has_pending = not pending_df[
                        (pending_df["username"].astype(str) == str(auth_user.get("username", "")))
                        & (pending_df["store"].astype(str) == str(store_name))
                        & (pending_df["status"].astype(str) == "pending")
                    ].empty
                status_icon = "⏳" if has_pending else "✅"
                _va, _vp = get_store_contact_info(store_name)
                with st.container(border=True):
                    st.markdown(f"### {status_icon} {store_name}")
                    st.markdown(f"**მისამართი:** {_va}")
                    if _vp:
                        st.markdown(f"**ტელეფონი:** {_vp}")
                    if st.button("ვიზიტის გახსნა", key=f"open_visit_{store_name}", use_container_width=True):
                        st.session_state["distributor_selected_store"] = store_name
                        st.rerun()
    
        selected_store = st.session_state.get("distributor_selected_store", "")
        if selected_store:
            st.divider()
            st.subheader(f"🛒 აქტიური მიწოდება: {selected_store}")
            store_items = low_stock_alerts[low_stock_alerts["Store_Name"].astype(str) == str(selected_store)]
            if store_items.empty:
                store_items = df[df["Store_Name"].astype(str) == str(selected_store)].head(6)
            if store_items.empty:
                st.info("ამ ფილიალზე პროდუქტი არ მოიძებნა.")
            else:
                with st.form("distributor_active_delivery_form"):
                    delivery_rows = []
                    for i, (_, item) in enumerate(store_items.iterrows()):
                        product_name = str(item["Product_Name"])
                        current_stock = int(item.get("Current_Stock", 0))
                        _, recommended_qty = get_restock_recommendation_qty(
                            sales_df, product_name, selected_store, current_stock
                        )
                        qty = st.number_input(
                            f"{product_name} (ნაშთი: {current_stock})",
                            min_value=0,
                            value=max(0, int(recommended_qty)),
                            step=1,
                            key=f"dist_deliver_qty_{i}_{product_name}",
                        )
                        delivery_rows.append({"product": product_name, "qty": int(qty), "unit_price": float(item.get("Selling_Price", 0.0))})
                    delivery_notes = st.text_area("შენიშვნები (დაზიანება/კორექცია)", placeholder="მაგ: 2 ყუთი დაზიანებული იყო")
                    submit_delivery = st.form_submit_button("მიწოდების დადასტურება")
                if submit_delivery:
                    confirmed = [r for r in delivery_rows if r["qty"] > 0]
                    if not confirmed:
                        st.warning("გთხოვთ მიუთითოთ მინიმუმ ერთი პროდუქტის რაოდენობა.")
                    else:
                        for item in confirmed:
                            append_pending_delivery(
                                username=auth_user.get("username", ""),
                                company=user_company,
                                store=selected_store,
                                product=item["product"],
                                ordered_qty=item["qty"],
                                unit_price=item["unit_price"],
                                notes=delivery_notes.strip(),
                            )
                        st.success(f"{selected_store}-ისთვის მიწოდება გაიგზავნა დასადასტურებლად.")
                        st.rerun()
    
        st.divider()
        st.subheader("📦 ჩემი ნაშთი (ბორტზე)")
        my_pending = pending_df[
            (pending_df["username"].astype(str) == str(auth_user.get("username", "")))
            & (pending_df["status"].astype(str) == "pending")
        ] if not pending_df.empty else pending_df
        if my_pending.empty:
            st.info("ბორტზე აქტიური მიწოდება არ არის.")
        else:
            truck_df = (
                my_pending.groupby("product", as_index=False)["ordered_qty"].sum()
                .rename(columns={"product": "პროდუქტი", "ordered_qty": "რაოდენობა"})
                .sort_values("რაოდენობა", ascending=False)
            )
            st.dataframe(truck_df, use_container_width=True)
    
        st.divider()
        st.subheader("📊 ჩემი გამომუშავება")
        my_deliveries = delivery_df[delivery_df["username"].astype(str) == str(auth_user.get("username", ""))] if not delivery_df.empty else delivery_df
        today = datetime.now().date()
        today_sales = float(
            my_deliveries[my_deliveries["timestamp"].dt.date == today]["total_sales"].sum()
        ) if not my_deliveries.empty else 0.0
        estimated_commission = today_sales * commission_rate
        f1, f2 = st.columns(2)
        f1.metric("Total Sales Today", f"{today_sales:.2f}")
        f2.metric("Estimated Commission", f"{estimated_commission:.2f}")
    
        correction_df = load_correction_log()
        my_corrections = correction_df[correction_df["distributor"].astype(str) == str(auth_user.get("username", ""))] if not correction_df.empty else correction_df
        st.divider()
        st.subheader("კორექციის ჟურნალი")
        if my_corrections.empty:
            st.info("კორექციის ჩანაწერები არ არის.")
        else:
            st.dataframe(
                my_corrections[["timestamp", "store", "product", "original_qty", "updated_qty", "difference", "reason"]]
                .rename(
                    columns={
                        "timestamp": "დრო",
                        "store": "ფილიალი",
                        "product": "პროდუქტი",
                        "original_qty": "საწყისი რაოდენობა",
                        "updated_qty": "განახლებული რაოდენობა",
                        "difference": "სხვაობა",
                        "reason": "მიზეზი",
                    }
                ),
                use_container_width=True,
            )
    
    elif page == "🏪 ბაზრის პანელი":
        st.title("🏪 ბაზრის პანელი")
        ms = str(st.session_state.get("market_store_pick_main", "")).strip()
        if not ms:
            st.warning("გთხოვთ, გვერდითა პანელიდან აირჩიოთ თქვენი ფილიალი (Market).")
        else:
            st.caption(f"ფილიალი: **{ms}** · საერთო ინვენტარი: `{SHARED_INVENTORY_FILE}`")
            today_d = datetime.now().date()
            delivery_df_m = load_delivery_log()
            inc_m = (
                delivery_df_m[delivery_df_m["store"].astype(str) == ms].copy()
                if not delivery_df_m.empty
                else delivery_df_m
            )
            if not inc_m.empty and "timestamp" in inc_m.columns:
                inc_m = inc_m.sort_values("timestamp", ascending=False)
    
            k1, k2, k3 = st.columns(3)
            with k1:
                st.metric("SKU / ხაზები", len(df))
            with k2:
                day_rev = 0.0
                if not sales_df_all.empty and "Timestamp" in sales_df_all.columns:
                    day_rev = float(
                        pd.to_numeric(
                            sales_df_all.loc[sales_df_all["Timestamp"].dt.date == today_d, "Revenue"],
                            errors="coerce",
                        ).fillna(0).sum()
                    )
                st.metric("დღევანდელი გაყიდვა (₾)", f"{day_rev:.2f}")
            with k3:
                st.metric("სულ ნაშთი (ერთ.)", int(df["Current_Stock"].sum()) if not df.empty else 0)
    
            st.subheader("📦 მიმდინარე ინვენტარი")
            if df.empty:
                st.info("ამ ფილიალზე პროდუქტის ჩანაწერი არ არის — გაუშვით სინქი (load_data) ან დაელოდეთ მიწოდებას.")
            else:
                disp_m = df[
                    ["Product_Name", "Current_Stock", "Cost_Price", "Selling_Price", "დარჩენილი დღე"]
                ].copy()
                st.dataframe(disp_m, use_container_width=True, hide_index=True)
    
            st.subheader("🛒 დღევანდელი გაყიდვები (sales_log)")
            sday = (
                sales_df_all[sales_df_all["Timestamp"].dt.date == today_d].copy()
                if not sales_df_all.empty and "Timestamp" in sales_df_all.columns
                else sales_df_all
            )
            if sday.empty:
                st.caption("დღეს გაყიდვის ჩანაწერი არ არის.")
            else:
                st.dataframe(
                    sday[["Timestamp", "Product", "Qty", "Revenue", "Profit"]].rename(
                        columns={
                            "Timestamp": "დრო",
                            "Product": "პროდუქტი",
                            "Qty": "რაოდ.",
                            "Revenue": "შემოსავალი",
                            "Profit": "მოგება",
                        }
                    ),
                    use_container_width=True,
                    hide_index=True,
                )
    
            st.subheader("📥 შემომავალი მიწოდებები (deliveries_log)")
            if inc_m.empty:
                st.caption("ჩანაწერი არ არის — დისტრიბუტორის დადასტურებული მიწოდება გამოჩნდება აქ.")
            else:
                show_m = inc_m.head(100)[
                    ["timestamp", "username", "product", "qty", "total_sales", "rs_status"]
                ].rename(
                    columns={
                        "timestamp": "დრო",
                        "username": "დისტრიბუტორი",
                        "product": "პროდუქტი",
                        "qty": "რაოდ.",
                        "total_sales": "ჯამი (₾)",
                        "rs_status": "სტატუსი",
                    }
                )
                st.dataframe(show_m, use_container_width=True, hide_index=True)
    
            st.divider()
            st.subheader("➕ გაყიდვის ჩაწერა (ნაშთის შემცირება)")
            if df.empty:
                st.info("პროდუქტის სია ცარიელია.")
            else:
                with st.form("market_register_sale"):
                    prod_pick = st.selectbox("პროდუქტი", df["Product_Name"].astype(str).tolist())
                    sale_qty_in = st.number_input("რაოდენობა", min_value=1, value=1, step=1)
                    do_sale = st.form_submit_button("გაყიდვის შენახვა")
                if do_sale:
                    m_idx = (master_df["Store_Name"].astype(str) == ms) & (
                        master_df["Product_Name"].astype(str) == str(prod_pick)
                    )
                    if not m_idx.any():
                        st.error("პროდუქტი ვერ მოიძებნა მთავარ მარაგში.")
                    else:
                        ix = master_df[m_idx].index[0]
                        cur_st = int(pd.to_numeric(master_df.at[ix, "Current_Stock"], errors="coerce") or 0)
                        sq = min(int(sale_qty_in), cur_st)
                        if sq < 1:
                            st.warning("ნაშთი არასაკმარისია.")
                        else:
                            sp = float(pd.to_numeric(master_df.at[ix, "Selling_Price"], errors="coerce") or 0.0)
                            cp = float(pd.to_numeric(master_df.at[ix, "Cost_Price"], errors="coerce") or 0.0)
                            st.session_state.daily_profit += (sp - cp) * sq
                            save_sale(
                                sale_date=datetime.now(),
                                store=ms,
                                product=str(prod_pick),
                                qty=sq,
                                selling_price=sp,
                                cost_price=cp,
                            )
                            full_df = master_df.copy()
                            full_df.at[ix, "Current_Stock"] = cur_st - sq
                            full_df = recalc_metrics(ensure_data_structure(full_df))
                            save_data(full_df)
                            st.session_state.df = full_df
                            st.success("გაყიდვა ჩაწერილია — ნაშთი განახლებულია.")
                            st.rerun()
    
    elif page == "📊 თვის ანალიტიკა":
        st.title("📊 თვის ანალიტიკა")
        st.caption("ბოლო 30 დღის აგრეგატები: `sales_log.csv`, დისტრიბუტორის საკომისიო — `deliveries_log` / `returns_log`.")
        try:
            _sm_mtime = os.path.getmtime(SALES_LOG_FILE) if os.path.exists(SALES_LOG_FILE) else 0.0
        except OSError:
            _sm_mtime = 0.0
        sales_an = _cached_monthly_analytics_sales(_sm_mtime)
        end_an = datetime.now().date()
        start_an = end_an - timedelta(days=29)
        role_an = str(auth_user.get("role", ""))
        ms_an = str(st.session_state.get("market_store_pick_main", "")).strip() if role_an == "Market" else ""
        if role_an == "Market" and not ms_an:
            st.warning("გვერდითა პანელიდან აირჩიეთ ფილიალი — ანალიტიკა ცარიელია.")
    
        if not sales_an.empty and "Timestamp" in sales_an.columns:
            sl_win = sales_an[
                (sales_an["Timestamp"].dt.date >= start_an) & (sales_an["Timestamp"].dt.date <= end_an)
            ].copy()
        else:
            sl_win = sales_an.iloc[0:0].copy()
    
        if ms_an:
            sl_win = sl_win[sl_win["Store"].astype(str) == ms_an]
    
        total_rev_an = float(pd.to_numeric(sl_win["Revenue"], errors="coerce").fillna(0).sum()) if not sl_win.empty else 0.0
        st.metric("ჯამური შემოსავალი (sales_log, 30 დღე)", format_gel_currency(total_rev_an))
    
        if role_an != "Market":
            if not sales_an.empty and "Timestamp" in sales_an.columns:
                sl_all_top = sales_an[
                    (sales_an["Timestamp"].dt.date >= start_an) & (sales_an["Timestamp"].dt.date <= end_an)
                ]
                top_st = (
                    sl_all_top.groupby("Store", as_index=False)["Revenue"]
                    .sum()
                    .sort_values("Revenue", ascending=False)
                    .head(15)
                )
                st.subheader("ტოპ ფილიალები (შემოსავლით)")
                if top_st.empty:
                    st.caption("მონაცემი არ არის.")
                else:
                    st.dataframe(
                        top_st.rename(columns={"Store": "ფილიალი", "Revenue": "შემოსავალი (₾)"}),
                        use_container_width=True,
                        hide_index=True,
                    )
        else:
            st.info(f"ნაჩვენებია მხოლოდ **{ms_an}** — სრული ქსელის რეიტინგი ხელმისაწვდომია სუპერ-ადმინს.")
    
        st.subheader("გაყიდვების ტრენდი (ხაზის დიაგრამა)")
        if sl_win.empty:
            st.caption("ამ ფილტრით მონაცემი არ არის.")
        else:
            sl_plot = sl_win.copy()
            sl_plot["day"] = pd.to_datetime(sl_plot["Timestamp"]).dt.date
            by_d = sl_plot.groupby("day", as_index=False)["Revenue"].sum()
            st.line_chart(by_d.set_index("day"))
    
        st.subheader("დისტრიბუტორი: ბონუსი + საკომისიო (30 დღე)")
        dist_pick = str(auth_user.get("username", "")) if role_an == "Distributor" else DEBUG_SIM_DISTRIBUTOR_USERNAME
        users_an = load_users()
        row_an = next((u for u in users_an if str(u.get("username", "")) == dist_pick), {})
        cr_an = float(row_an.get("commission_rate", 0) or 0)
        plan_an = get_user_monthly_sales_target(row_an)
        del_an = load_delivery_log()
        ret_an = load_return_log()
        fin_an = calculate_salary(dist_pick, del_an, ret_an, start_an, end_an, cr_an)
        bonus_an = distributor_plan_bonus_estimate(plan_an, fin_an["net"])
        tot_earn = round(float(fin_an["commission"]) + float(bonus_an), 2)
        e1, e2, e3, e4 = st.columns(4)
        with e1:
            st.metric("ნეტო მიწოდება (პერიოდი)", format_gel_currency(fin_an["net"]))
        with e2:
            st.metric("საკომისიო", format_gel_currency(fin_an["commission"]))
        with e3:
            st.metric("გეგმის ბონუსი (ესტ.)", format_gel_currency(bonus_an))
        with e4:
            st.metric("ჯამური შემოსავალი (ბონუს + საკომისიო)", format_gel_currency(tot_earn))
        st.caption(
            f"პერიოდი: **{start_an}** — **{end_an}** · ანგარიშგება: **{dist_pick}** · გეგმა (users.csv): "
            f"{format_gel_currency(plan_an) if plan_an > 0 else '—'}"
        )
    
    elif page == "🚚 აუცილებელი მიწოდებები":
        render_distributor_dashboard(auth_user, mapping_data, sales_df_all)
    
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
        with c1:
            st.markdown("### 📦 სულ პროდუქტი")
            st.metric("Total Products", len(df))
        with c2:
            st.markdown("### 🚨 დაბალი ნაშთი")
            st.error(f"Low Stock Alerts: {len(low_stock_alerts)}")
        with c3:
            st.markdown("### 💰 დღიური მოგება")
            st.metric("Daily Profit", f"{st.session_state.daily_profit:.2f}")
    
        st.divider()
        st.subheader("✅ დღიური To-Do")
        if low_stock_alerts.empty:
            st.info("დღეს დაბალი ნაშთის ამოცანები არ არის.")
        else:
            for idx, row in low_stock_alerts.iterrows():
                product_name = str(row["Product_Name"])
                current_stock = int(row["Current_Stock"])
                _, recommended_qty = get_restock_recommendation_qty(sales_df, product_name, assigned_branch, current_stock)
                t1, t2 = st.columns([5, 1])
                t1.warning(f"შეუკვეთე {product_name} (+{recommended_qty}) | მიმდინარე ნაშთი: {current_stock}")
                if t2.button("Order Now", key=f"sm_order_now_{idx}"):
                    st.session_state.pending_page = "📥 მარაგების მიღება"
                    st.session_state.prefill_product = product_name
                    st.session_state.prefill_qty = max(1, int(recommended_qty))
                    st.rerun()
    
        st.divider()
        st.subheader("📥 ბოლო მიღებები (5)")
        pending_df = load_pending_deliveries()
        delivery_df = load_delivery_log()
        incoming_rows = []
        if not delivery_df.empty:
            ddf = delivery_df[delivery_df["store"].astype(str) == str(assigned_branch)].copy()
            for _, r in ddf.iterrows():
                incoming_rows.append(
                    {
                        "დრო": r["timestamp"],
                        "პროდუქტი": r["product"],
                        "რაოდენობა": int(r["qty"]),
                        "სტატუსი": "Confirmed",
                    }
                )
        if not pending_df.empty:
            pdf = pending_df[pending_df["store"].astype(str) == str(assigned_branch)].copy()
            for _, r in pdf.iterrows():
                incoming_rows.append(
                    {
                        "დრო": r["timestamp"],
                        "პროდუქტი": r["product"],
                        "რაოდენობა": int(r["ordered_qty"]),
                        "სტატუსი": "Pending" if str(r["status"]) == "pending" else "Confirmed",
                    }
                )
        if incoming_rows:
            incoming_df = pd.DataFrame(incoming_rows).sort_values("დრო", ascending=False).head(5)
            st.dataframe(incoming_df, use_container_width=True)
        else:
            st.info("მიღების ჩანაწერები ვერ მოიძებნა.")
    
        st.divider()
        st.subheader("⚡ სწრაფი ქმედებები")
        qa1, qa2, qa3 = st.columns(3)
        if qa1.button("📋 Inventory Count", use_container_width=True):
            st.session_state.pending_page = "📥 მარაგების მიღება"
            st.rerun()
        if qa2.button("🧾 Report Loss", use_container_width=True):
            st.session_state.pending_page = "📥 მარაგების მიღება"
            st.rerun()
        if qa3.button("📞 Contact Supplier", use_container_width=True):
            st.info("დაუკავშირდით თქვენს დისტრიბუტორს შიდა საკონტაქტო არხით.")
    
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
                    save_sale(
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
    
        st.subheader("RS.GE ზედნადების მიღება")
        waybill_no = st.text_input("ზედნადების ნომერი", key="rs_waybill_no")
        cwb1, cwb2 = st.columns([1, 1])
        if cwb1.button("RS.GE-დან წამოღება", key="fetch_waybill_btn"):
            if not waybill_no.strip():
                st.warning("გთხოვთ მიუთითოთ ზედნადების ნომერი.")
            else:
                payload_text, payload_type = simulate_fetch_waybill(waybill_no.strip())
                parsed = parse_rs_payload(payload_text)
                st.session_state["rs_waybill_data"] = parsed
                st.session_state["rs_waybill_format"] = payload_type
                st.success(f"ზედნადები ჩაიტვირთა ({payload_type.upper()}).")
                st.rerun()
        if cwb2.button("გასუფთავება", key="clear_waybill_btn"):
            st.session_state.pop("rs_waybill_data", None)
            st.session_state.pop("rs_waybill_format", None)
            st.rerun()
    
        rs_data = st.session_state.get("rs_waybill_data")
        if rs_data and rs_data.get("items"):
            st.info(f"ზედნადები #{rs_data.get('waybill_no', '')} | ფორმატი: {st.session_state.get('rs_waybill_format', '').upper()}")
            with st.form("rs_waybill_confirm_form"):
                rs_store = assigned_branch
                order_rows = []
                for i, item in enumerate(rs_data.get("items", [])):
                    product = str(item.get("product", ""))
                    qty = st.number_input(
                        f"{product} რაოდენობა",
                        min_value=0,
                        value=int(item.get("qty", 0)),
                        step=1,
                        key=f"rs_qty_{i}_{product}",
                    )
                    order_rows.append(
                        {
                            "product": product,
                            "qty": int(qty),
                            "cost_price": float(item.get("cost_price", 0.0)),
                            "selling_price": float(item.get("selling_price", 0.0)),
                        }
                    )
                confirm_rs = st.form_submit_button("ზედნადების დადასტურება")
    
            if confirm_rs:
                full_df = st.session_state.df.copy()
                company_name = mapping_data.get("user_company", {}).get(auth_user.get("username", ""), auth_user.get("company", ""))
                for item in order_rows:
                    if item["qty"] <= 0:
                        continue
                    full_df = ensure_product_row(
                        full_df,
                        rs_store,
                        item["product"],
                        item["qty"],
                        item["cost_price"],
                        item["selling_price"],
                    )
                    append_delivery_log(
                        username=auth_user.get("username", ""),
                        company=company_name,
                        store=rs_store,
                        product=item["product"],
                        qty=item["qty"],
                        unit_price=item["selling_price"],
                        waybill_no=str(rs_data.get("waybill_no", "")),
                        rs_status="Confirmed on RS.GE",
                    )
                full_df = recalc_metrics(full_df)
                save_products(full_df)
                st.session_state.df = full_df
                st.success("ზედნადები დამუშავდა. პროდუქცია განახლდა და RS.GE სტატუსი ჩაიწერა.")
                st.session_state.pop("rs_waybill_data", None)
                st.session_state.pop("rs_waybill_format", None)
                st.rerun()
    
        pending_df = load_pending_deliveries()
        branch_pending = pending_df[
            (pending_df["store"].astype(str) == str(assigned_branch))
            & (pending_df["status"].astype(str) == "pending")
        ] if not pending_df.empty else pending_df
    
        if branch_pending.empty:
            st.info("დასადასტურებელი მიწოდება არ არის.")
        else:
            st.subheader("დასადასტურებელი მიწოდებები")
            for _, p in branch_pending.iterrows():
                pid = str(p["id"])
                product_name = str(p["product"])
                ordered_qty = int(p["ordered_qty"])
                distributor_name = str(p["username"])
                with st.container(border=True):
                    st.markdown(f"**პროდუქტი:** {product_name} | **შეკვეთილი რაოდენობა:** {ordered_qty}")
                    st.caption(f"დისტრიბუტორი: {distributor_name}")
                    issue_text = str(p.get("issue", "None"))
                    notes_text = str(p.get("notes", ""))
                    st.caption(f"Issue: {issue_text} | შენიშვნა: {notes_text if notes_text else '-'}")
                    final_qty = st.number_input(
                        f"მიღებული რაოდენობა ({product_name})",
                        min_value=0,
                        value=ordered_qty,
                        step=1,
                        key=f"confirm_qty_{pid}",
                    )
                    reason = st.text_input(
                        "კომენტარი / მიზეზი (თუ მიღებული რაოდენობა ნაკლებია)",
                        key=f"reason_{pid}",
                        placeholder="მაგ: Damaged / Missing",
                    )
                    if st.button("მიღების დადასტურება", key=f"confirm_btn_{pid}"):
                        full_df = st.session_state.df.copy()
                        mask = (
                            (full_df["Store_Name"].astype(str) == str(assigned_branch))
                            & (full_df["Product_Name"].astype(str) == str(product_name))
                        )
                        if mask.any():
                            first_idx = full_df[mask].index[0]
                            full_df.at[first_idx, "Current_Stock"] = int(full_df.at[first_idx, "Current_Stock"]) + int(final_qty)
                        else:
                            # if product row doesn't exist in branch, create minimal row
                            full_df = pd.concat(
                                [
                                    full_df,
                                    pd.DataFrame(
                                        [
                                            {
                                                "Store_Name": assigned_branch,
                                                "Product_Name": product_name,
                                                "Current_Stock": int(final_qty),
                                                "Cost_Price": 0.0,
                                                "Selling_Price": float(p["unit_price"]),
                                                "Price": float(p["unit_price"]),
                                                "Sales_Day1": 0,
                                                "Sales_Day2": 0,
                                                "Sales_Day3": 0,
                                                "Sales_Day4": 0,
                                                "Sales_Day5": 0,
                                                "Sales_Day6": 0,
                                                "Sales_Day7": 0,
                                                "Expiry_Date": pd.to_datetime(datetime.now().date() + timedelta(days=30)),
                                            }
                                        ]
                                    ),
                                ],
                                ignore_index=True,
                            )
    
                        full_df = recalc_metrics(full_df)
                        save_data(full_df)
                        st.session_state.df = full_df
    
                        # Commission/sales are based on FINAL confirmed quantity only.
                        append_delivery_log(
                            username=distributor_name,
                            company=str(p["company"]),
                            store=assigned_branch,
                            product=product_name,
                            qty=int(final_qty),
                            unit_price=float(p["unit_price"]),
                            delivery_id=pid,
                        )
    
                        if int(final_qty) < ordered_qty:
                            append_discrepancy_log(
                                distributor=distributor_name,
                                company=str(p["company"]),
                                store=assigned_branch,
                                product=product_name,
                                ordered_qty=ordered_qty,
                                confirmed_qty=int(final_qty),
                                reason=reason if reason.strip() else "Unspecified",
                                corrected_by=str(auth_user.get("username", "")),
                            )
    
                        pending_df.loc[pending_df["id"].astype(str) == pid, "status"] = "confirmed"
                        save_pending_deliveries(pending_df)
                        st.success("მიღება დადასტურდა და მარაგი განახლდა.")
                        st.rerun()
    
        st.divider()
        st.subheader("Post-Confirmation Correction (24 საათი)")
        delivery_df = load_delivery_log()
        if delivery_df.empty:
            st.info("დადასტურებული მიწოდებები არ არის.")
        else:
            now_ts = datetime.now()
            editable = delivery_df[
                (delivery_df["store"].astype(str) == str(assigned_branch))
                & (delivery_df["timestamp"] >= (now_ts - timedelta(hours=24)))
            ]
            locked = delivery_df[
                (delivery_df["store"].astype(str) == str(assigned_branch))
                & (delivery_df["timestamp"] < (now_ts - timedelta(hours=24)))
            ]
    
            if editable.empty:
                st.info("24 საათიან ფანჯარაში ჩასწორებადი ჩანაწერი არ არის.")
            else:
                for _, drow in editable.iterrows():
                    did = str(drow["id"])
                    old_qty = int(drow["qty"])
                    product_name = str(drow["product"])
                    distributor_name = str(drow["username"])
                    unit_price = float(drow["unit_price"])
                    with st.container(border=True):
                        st.markdown(f"**{product_name}** | დისტრიბუტორი: {distributor_name} | მიმდინარე: {old_qty}")
                        new_qty = st.number_input(
                            f"განახლებული რაოდენობა ({product_name})",
                            min_value=0,
                            value=old_qty,
                            step=1,
                            key=f"corr_qty_{did}",
                        )
                        corr_reason = st.text_input(
                            "კორექციის მიზეზი",
                            key=f"corr_reason_{did}",
                            placeholder="მაგ: დაზიანება, დანაკლისი",
                        )
                        if st.button("კორექციის შენახვა", key=f"corr_save_{did}"):
                            if int(new_qty) == old_qty:
                                st.info("რაოდენობა უცვლელია.")
                            elif not corr_reason.strip():
                                st.warning("გთხოვთ მიუთითოთ კორექციის მიზეზი.")
                            else:
                                diff = int(new_qty) - old_qty
                                full_df = st.session_state.df.copy()
                                mask = (
                                    (full_df["Store_Name"].astype(str) == str(assigned_branch))
                                    & (full_df["Product_Name"].astype(str) == str(product_name))
                                )
                                if mask.any():
                                    idx_first = full_df[mask].index[0]
                                    full_df.at[idx_first, "Current_Stock"] = max(
                                        0, int(full_df.at[idx_first, "Current_Stock"]) + diff
                                    )
                                    full_df = recalc_metrics(full_df)
                                    save_data(full_df)
                                    st.session_state.df = full_df
    
                                delivery_df.loc[delivery_df["id"].astype(str) == did, "qty"] = int(new_qty)
                                delivery_df.loc[delivery_df["id"].astype(str) == did, "total_sales"] = float(new_qty) * unit_price
                                save_delivery_log(delivery_df)
    
                                append_correction_log(
                                    delivery_id=did,
                                    distributor=distributor_name,
                                    company=str(drow["company"]),
                                    store=assigned_branch,
                                    product=product_name,
                                    original_qty=old_qty,
                                    updated_qty=int(new_qty),
                                    reason=corr_reason.strip(),
                                    updated_by=str(auth_user.get("username", "")),
                                )
                                st.success("კორექცია შენახულია. მარაგი და საკომისიო სინქრონიზებულია.")
                                st.rerun()
    
            if not locked.empty:
                st.warning("ზოგი ჩანაწერი 24 საათზე ძველია და პირდაპირი რედაქტირება დაბლოკილია.")
                st.subheader("Pending Adjustment Request")
                for _, drow in locked.iterrows():
                    did = str(drow["id"])
                    old_qty = int(drow["qty"])
                    product_name = str(drow["product"])
                    distributor_name = str(drow["username"])
                    with st.container(border=True):
                        st.markdown(f"**{product_name}** | დისტრიბუტორი: {distributor_name} | მიმდინარე: {old_qty}")
                        req_qty = st.number_input(
                            f"მოთხოვნილი ახალი რაოდენობა ({product_name})",
                            min_value=0,
                            value=old_qty,
                            step=1,
                            key=f"req_qty_{did}",
                        )
                        req_reason = st.text_input(
                            "მოთხოვნის მიზეზი",
                            key=f"req_reason_{did}",
                            placeholder="რატომ გჭირდებათ ცვლილება?",
                        )
                        if st.button("მოთხოვნის გაგზავნა", key=f"send_req_{did}"):
                            if int(req_qty) == old_qty:
                                st.info("რაოდენობა უცვლელია.")
                            elif not req_reason.strip():
                                st.warning("მიუთითეთ მიზეზი.")
                            else:
                                append_adjustment_request(
                                    delivery_id=did,
                                    store_manager=str(auth_user.get("username", "")),
                                    distributor=distributor_name,
                                    company=str(drow["company"]),
                                    store=assigned_branch,
                                    product=product_name,
                                    current_qty=old_qty,
                                    requested_qty=int(req_qty),
                                    reason=req_reason.strip(),
                                )
                                st.success("მოთხოვნა გაიგზავნა ოპერატორთან.")
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
                save_sale(
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
        st.subheader("🎯 თვიური გაყიდვების გეგმა (დისტრიბუტორი)")
        st.caption(
            "დააყენეთ თვიური გეგმა ლარებით (₾) თითოეული დისტრიბუტორისთვის. მნიშვნელობა ინახება `users.csv`-ში (`monthly_sales_target`). "
            "შესრულება აითვლება მიმდინარე თვის გაყიდვების ჟურნალიდან (Revenue) მიბმულ ფილიალებზე; თუ ფილიალი არ არის მიბმული — მიწოდების ჟურნალის ნეტო მოცულობით."
        )
        distributor_accounts = [u for u in visible_users if str(u.get("role", "")) == "Distributor"]
        if not distributor_accounts:
            st.info("დისტრიბუტორის ანგარიში ვერ მოიძებნა.")
        else:
            dist_names = [str(u.get("username", "")) for u in distributor_accounts if u.get("username")]
            pick_dist = st.selectbox(
                "დისტრიბუტორი",
                dist_names,
                key="admin_monthly_plan_pick_distributor",
            )
            cur_row = next(u for u in distributor_accounts if str(u.get("username", "")) == str(pick_dist))
            cur_plan = get_user_monthly_sales_target(cur_row)
            new_plan_val = st.number_input(
                "თვიური გაყიდვების გეგმა — ჯამური თანხა (₾)",
                min_value=0.0,
                value=float(cur_plan),
                step=500.0,
                key="admin_monthly_plan_amount",
            )
            if st.button("გეგმის შენახვა", key="admin_save_monthly_plan_btn", type="primary"):
                updated = load_users()
                for u in updated:
                    if str(u.get("username", "")) == str(pick_dist):
                        u["monthly_sales_target"] = str(round(float(new_plan_val), 2))
                        break
                save_users(updated)
                st.success(f"თვიური გეგმა შენახულია: {pick_dist} → {format_gel_currency(new_plan_val)}")
                st.rerun()
    
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
                    "commission_rate": "0.05",
                    "monthly_sales_target": "0",
                }
                users.append(new_user)
                save_users(users)
                map_df = mapping_data.get("df", pd.DataFrame(columns=["mapping_type", "key", "value"]))
                map_rows = [{"mapping_type": "user_company", "key": new_username.strip(), "value": new_company}]
                map_rows += [{"mapping_type": "distributor_store", "key": new_username.strip(), "value": s} for s in assign_stores]
                map_df = pd.concat([map_df, pd.DataFrame(map_rows)], ignore_index=True)
                safe_write_csv(map_df, MAPPING_FILE)
                st.success("Distributor ანგარიში წარმატებით შეიქმნა.")
                st.rerun()
    
    elif page == "🛠 ოპერატორის პანელი":
        st.title("🛠 ოპერატორის პანელი")
        operator_company = mapping_data.get("user_company", {}).get(auth_user.get("username", ""), auth_user.get("company", ""))
        pending_tab, master_tab = st.tabs(["Pending Approvals", "Master Edit"])
        with pending_tab:
            st.subheader("Pending Adjustment Request")
            req_df = load_adjustment_requests()
            req_df = req_df[
                (req_df["company"].astype(str) == str(operator_company))
                & (req_df["status"].astype(str) == "pending")
            ] if not req_df.empty else req_df
            if req_df.empty:
                st.info("მოლოდინში მოთხოვნები არ არის.")
            else:
                for _, req in req_df.iterrows():
                    rid = str(req["request_id"])
                    did = str(req["delivery_id"])
                    with st.container(border=True):
                        st.markdown(
                            f"**ფილიალი:** {req['store']} | **პროდუქტი:** {req['product']} | "
                            f"**მიმდინარე:** {int(req['current_qty'])} -> **მოთხოვნილი:** {int(req['requested_qty'])}"
                        )
                        st.caption(f"Store Manager: {req['store_manager']} | მიზეზი: {req['reason']}")
                        c1, c2 = st.columns(2)
                        if c1.button("დამტკიცება", key=f"approve_req_{rid}"):
                            delivery_df = load_delivery_log()
                            full_df = st.session_state.df.copy()
                            ok, msg, full_df, delivery_df = apply_delivery_correction(
                                delivery_id=did,
                                new_qty=int(req["requested_qty"]),
                                reason=f"Operator Approved: {req['reason']}",
                                updated_by=str(auth_user.get("username", "")),
                                full_df=full_df,
                                delivery_df=delivery_df,
                            )
                            if ok:
                                save_data(recalc_metrics(full_df))
                                st.session_state.df = recalc_metrics(full_df)
                                save_delivery_log(delivery_df)
                                update_adjustment_request_status(
                                    request_id=rid,
                                    status="approved",
                                    reviewed_by=str(auth_user.get("username", "")),
                                )
                                st.success("მოთხოვნა დამტკიცდა და ცვლილება გატარდა.")
                                st.rerun()
                            else:
                                st.error(msg)
                        if c2.button("უარყოფა", key=f"reject_req_{rid}"):
                            update_adjustment_request_status(
                                request_id=rid,
                                status="rejected",
                                reviewed_by=str(auth_user.get("username", "")),
                            )
                            st.warning("მოთხოვნა უარყოფილია.")
                            st.rerun()
        with master_tab:
            st.subheader("Master Edit (Force Update)")
            delivery_df = load_delivery_log()
            company_delivery = delivery_df[delivery_df["company"].astype(str) == str(operator_company)] if not delivery_df.empty else delivery_df
            if company_delivery.empty:
                st.info("კომპანიის მიწოდების ჩანაწერები არ არის.")
            else:
                for _, drow in company_delivery.sort_values("timestamp", ascending=False).head(30).iterrows():
                    did = str(drow["id"])
                    old_qty = int(drow["qty"])
                    product_name = str(drow["product"])
                    store_name = str(drow["store"])
                    with st.container(border=True):
                        st.markdown(f"**{store_name} | {product_name}** — მიმდინარე რაოდენობა: {old_qty}")
                        new_qty = st.number_input("ახალი რაოდენობა", min_value=0, value=old_qty, step=1, key=f"op_qty_{did}")
                        force_reason = st.text_input("მიზეზი", key=f"op_reason_{did}", placeholder="მაგ: Operator correction / Lost")
                        is_lost = st.checkbox("ჩამოწერა (Lost)", key=f"lost_{did}")
                        if st.button("Force Update", key=f"force_{did}"):
                            if int(new_qty) == old_qty:
                                st.info("რაოდენობა უცვლელია.")
                            elif not force_reason.strip():
                                st.warning("მიუთითეთ მიზეზი.")
                            else:
                                if is_lost and int(new_qty) > old_qty:
                                    st.warning("Lost რეჟიმში რაოდენობის გაზრდა შეუძლებელია.")
                                else:
                                    full_df = st.session_state.df.copy()
                                    ok, msg, full_df, delivery_df = apply_delivery_correction(
                                        delivery_id=did,
                                        new_qty=int(new_qty),
                                        reason=("Lost: " if is_lost else "") + force_reason.strip(),
                                        updated_by=str(auth_user.get("username", "")),
                                        full_df=full_df,
                                        delivery_df=delivery_df,
                                    )
                                    if ok:
                                        save_data(recalc_metrics(full_df))
                                        st.session_state.df = recalc_metrics(full_df)
                                        save_delivery_log(delivery_df)
                                        st.success("Force Update შესრულდა.")
                                        st.rerun()
                                    else:
                                        st.error(msg)
    
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


try:
    _run_main_app_after_login()
except Exception as _app_exc:
    log_exception_to_app_errors(_app_exc)
    st.error(
        "სისტემაში შეცდომა მოხდა. ინტერფეისი არ უნდა გათიშოს — დეტალები ჩაიწერა ჟურნალში. "
        "გთხოვთ სცადოთ გვერდის განახლება (Rerun) ან დაუკავშირდეთ ადმინისტრატორს."
    )
    st.caption(f"ტექნიკური ლოგი: `{APP_ERROR_LOG_FILE}`")
