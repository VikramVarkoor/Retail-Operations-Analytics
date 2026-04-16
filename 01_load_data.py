"""
Step 1: Generate Sample Superstore Dataset, Clean It, and Load into SQLite
============================================================================
This script creates a realistic replica of the classic Kaggle "Sample Superstore"
dataset (~9,994 rows), cleans it, and loads it into a SQLite database.

The dataset mirrors the real one's schema:
  - Row ID, Order ID, Order Date, Ship Date, Ship Mode
  - Customer ID, Customer Name, Segment
  - Country, City, State, Postal Code, Region
  - Product ID, Category, Sub-Category, Product Name
  - Sales, Quantity, Discount, Profit
"""

import pandas as pd
import numpy as np
import sqlite3
import os
from datetime import datetime, timedelta

np.random.seed(42)

# ─── CONFIG ──────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# SQLite needs to write to a local filesystem first (mounted dirs may not support locking)
WORK_DIR = os.environ.get("WORK_DIR", SCRIPT_DIR)
DB_PATH = os.path.join(WORK_DIR, "superstore.db")
CSV_RAW_PATH = os.path.join(os.path.dirname(__file__), "data", "superstore_raw.csv")
CSV_CLEAN_PATH = os.path.join(os.path.dirname(__file__), "data", "superstore_clean.csv")
N_ROWS = 9994

# ─── REFERENCE DATA ─────────────────────────────────────────────────────────
SHIP_MODES = ["Standard Class", "Second Class", "First Class", "Same Day"]
SHIP_MODE_WEIGHTS = [0.60, 0.20, 0.15, 0.05]
SHIP_MODE_DAYS = {"Standard Class": (5, 8), "Second Class": (3, 5),
                  "First Class": (1, 3), "Same Day": (0, 0)}

SEGMENTS = ["Consumer", "Corporate", "Home Office"]
SEGMENT_WEIGHTS = [0.52, 0.30, 0.18]

REGIONS = ["West", "East", "Central", "South"]

STATES_BY_REGION = {
    "West": [("California", ["Los Angeles", "San Francisco", "San Diego", "Sacramento", "San Jose"],
              ["90001", "94102", "92101", "95814", "95112"]),
             ("Washington", ["Seattle", "Spokane", "Tacoma"], ["98101", "99201", "98401"]),
             ("Oregon", ["Portland", "Eugene", "Salem"], ["97201", "97401", "97301"]),
             ("Colorado", ["Denver", "Colorado Springs", "Aurora"], ["80201", "80901", "80010"]),
             ("Arizona", ["Phoenix", "Tucson", "Mesa"], ["85001", "85701", "85201"]),
             ("Nevada", ["Las Vegas", "Reno"], ["89101", "89501"]),
             ("Utah", ["Salt Lake City", "Provo"], ["84101", "84601"]),
             ("New Mexico", ["Albuquerque", "Santa Fe"], ["87101", "87501"])],
    "East": [("New York", ["New York City", "Buffalo", "Rochester", "Albany"], ["10001", "14201", "14601", "12201"]),
             ("Pennsylvania", ["Philadelphia", "Pittsburgh", "Harrisburg"], ["19101", "15201", "17101"]),
             ("Massachusetts", ["Boston", "Worcester", "Springfield"], ["02101", "01601", "01101"]),
             ("New Jersey", ["Newark", "Jersey City", "Trenton"], ["07101", "07301", "08601"]),
             ("Connecticut", ["Hartford", "New Haven", "Stamford"], ["06101", "06501", "06901"]),
             ("Virginia", ["Richmond", "Virginia Beach", "Norfolk"], ["23219", "23450", "23501"]),
             ("Maryland", ["Baltimore", "Annapolis"], ["21201", "21401"]),
             ("North Carolina", ["Charlotte", "Raleigh", "Durham"], ["28201", "27601", "27701"])],
    "Central": [("Illinois", ["Chicago", "Springfield", "Naperville"], ["60601", "62701", "60540"]),
                ("Texas", ["Houston", "Dallas", "Austin", "San Antonio"], ["77001", "75201", "73301", "78201"]),
                ("Ohio", ["Columbus", "Cleveland", "Cincinnati"], ["43201", "44101", "45201"]),
                ("Michigan", ["Detroit", "Grand Rapids", "Ann Arbor"], ["48201", "49501", "48104"]),
                ("Indiana", ["Indianapolis", "Fort Wayne"], ["46201", "46801"]),
                ("Minnesota", ["Minneapolis", "Saint Paul"], ["55401", "55101"]),
                ("Wisconsin", ["Milwaukee", "Madison"], ["53201", "53701"]),
                ("Missouri", ["St. Louis", "Kansas City"], ["63101", "64101"])],
    "South": [("Florida", ["Miami", "Tampa", "Orlando", "Jacksonville"], ["33101", "33601", "32801", "32201"]),
              ("Georgia", ["Atlanta", "Savannah", "Augusta"], ["30301", "31401", "30901"]),
              ("Tennessee", ["Nashville", "Memphis", "Knoxville"], ["37201", "38101", "37901"]),
              ("Louisiana", ["New Orleans", "Baton Rouge"], ["70112", "70801"]),
              ("Alabama", ["Birmingham", "Montgomery"], ["35201", "36101"]),
              ("South Carolina", ["Charleston", "Columbia"], ["29401", "29201"]),
              ("Mississippi", ["Jackson", "Gulfport"], ["39201", "39501"]),
              ("Kentucky", ["Louisville", "Lexington"], ["40201", "40501"])]
}

CATEGORIES = {
    "Furniture": {
        "sub_cats": ["Bookcases", "Chairs", "Tables", "Furnishings"],
        "products": {
            "Bookcases": ["Bush Somerset Bookcase", "Sauder Classic Bookcase", "O'Sullivan Bookcase",
                          "Hon Metal Bookcase", "Atlantic Metals Bookcase"],
            "Chairs": ["Hon Deluxe Task Chair", "Global Troy Executive Chair", "Office Star Chair",
                       "HON 5400 Series Chair", "Novimex Swivel Chair"],
            "Tables": ["Bretford CR4500 Table", "Chromcraft Table", "KI Adjustable Table",
                       "Bevis Round Table", "Balt Solid Wood Table"],
            "Furnishings": ["Eldon Base Organizer", "Howard Miller Clock", "Artistic Wall Frame",
                            "Tenex File Cart", "Dana Lighting Lamp"]
        },
        "price_range": (20, 2000), "profit_margin": (-0.15, 0.25)
    },
    "Office Supplies": {
        "sub_cats": ["Labels", "Storage", "Art", "Binders", "Paper", "Envelopes",
                     "Fasteners", "Supplies", "Appliances"],
        "products": {
            "Labels": ["Avery 510 Labels", "Avery Address Labels", "Avery Round Labels"],
            "Storage": ["Fellowes Bankers Box", "Storex File Tote", "Akro Stacking Bins",
                        "Rogers File Cart", "Carina Desktop Organizer"],
            "Art": ["Newell Pens", "Boston School Scissors", "Fiskars Scissors",
                    "Sanford Markers", "Dixon Prang Crayons"],
            "Binders": ["GBC DocuBind Binder", "Avery Binder", "Wilson Jones Binder",
                        "Cardinal Slant-D Binder", "Acco Pressboard Covers"],
            "Paper": ["Xerox Copy Paper", "HP Office Paper", "Easy-staple Paper",
                      "Southworth Resume Paper", "Hammermill Color Copy"],
            "Envelopes": ["#10 White Envelopes", "Tyvek Side-Opening Envelopes",
                          "Manila Clasp Envelopes"],
            "Fasteners": ["Advantus Paper Clips", "OIC Binder Clips", "Acco Brass Fasteners"],
            "Supplies": ["Scotch Bubble Mailer", "Avery Hole Punch", "BIC Round Stic Pens",
                         "Stanley Bostitch Staples", "Swingline Stapler"],
            "Appliances": ["Hamilton Beach Toaster", "Belkin Surge Protector", "Hoover Stove",
                           "Eureka Disposable Bags", "Acco 7-Outlet Surge"]
        },
        "price_range": (1, 500), "profit_margin": (-0.05, 0.45)
    },
    "Technology": {
        "sub_cats": ["Phones", "Accessories", "Machines", "Copiers"],
        "products": {
            "Phones": ["Cisco SPA 501G Phone", "AT&T CL2909 Phone", "Plantronics Headset",
                       "Samsung Galaxy", "Apple iPhone"],
            "Accessories": ["Logitech Wireless Mouse", "Belkin USB Cable", "SanDisk USB Drive",
                            "Targus Backpack", "Imation USB Flash Drive"],
            "Machines": ["Brother Fax Machine", "Hewlett Packard LaserJet", "Canon imageCLASS",
                         "Lexmark Printer", "Epson TM Printer"],
            "Copiers": ["Canon Color Copier", "HP Designjet Plotter", "Sharp Digital Copier",
                        "Xerox WorkCentre", "Brother Copier"]
        },
        "price_range": (10, 7000), "profit_margin": (-0.20, 0.50)
    }
}

FIRST_NAMES = ["Aaron", "Adam", "Alex", "Alice", "Amy", "Andrew", "Anna", "Ben",
               "Beth", "Bill", "Brian", "Carol", "Chris", "Claire", "Craig",
               "Dan", "Dave", "Dean", "Diana", "Ed", "Emily", "Eric", "Frank",
               "Grace", "Greg", "Hannah", "Helen", "Jack", "Jane", "Jason",
               "Jennifer", "Jim", "Joe", "John", "Karen", "Kate", "Keith",
               "Kelly", "Ken", "Laura", "Linda", "Lisa", "Mark", "Mary", "Matt",
               "Mike", "Nancy", "Nick", "Olivia", "Pat", "Paul", "Pete", "Phil",
               "Rachel", "Ralph", "Rick", "Rob", "Roger", "Ryan", "Sam",
               "Sandra", "Sarah", "Sean", "Steve", "Sue", "Ted", "Tim", "Tom",
               "Tony", "Tracy", "Victor", "Wendy", "William", "Zach"]
LAST_NAMES = ["Anderson", "Baker", "Brown", "Campbell", "Carter", "Clark",
              "Collins", "Davis", "Evans", "Fisher", "Garcia", "Green", "Hall",
              "Harris", "Hill", "Jackson", "Johnson", "Jones", "King", "Lee",
              "Lewis", "Lopez", "Martin", "Miller", "Mitchell", "Moore",
              "Nelson", "Parker", "Patel", "Perez", "Phillips", "Roberts",
              "Robinson", "Rodriguez", "Scott", "Smith", "Taylor", "Thomas",
              "Thompson", "Turner", "Walker", "White", "Williams", "Wilson",
              "Wright", "Young"]

# ─── GENERATE CUSTOMERS ─────────────────────────────────────────────────────
print("Generating customers...")
n_customers = 793
customers = []
for i in range(n_customers):
    fn = np.random.choice(FIRST_NAMES)
    ln = np.random.choice(LAST_NAMES)
    seg = np.random.choice(SEGMENTS, p=SEGMENT_WEIGHTS)
    cid = f"{''.join([c for c in seg[:2].upper()])}-{2*i + 10000 + np.random.randint(0,500)}"
    customers.append({"Customer ID": cid, "Customer Name": f"{fn} {ln}", "Segment": seg})

# ─── GENERATE ORDERS ─────────────────────────────────────────────────────────
print("Generating orders...")
start_date = datetime(2020, 1, 1)
end_date = datetime(2023, 12, 31)
date_range_days = (end_date - start_date).days

rows = []
order_counter = 0
row_id = 1

while len(rows) < N_ROWS:
    # Pick a customer
    cust = customers[np.random.randint(0, n_customers)]

    # Pick a region and location
    region = np.random.choice(REGIONS, p=[0.32, 0.30, 0.22, 0.16])
    state_info = STATES_BY_REGION[region][np.random.randint(0, len(STATES_BY_REGION[region]))]
    state_name = state_info[0]
    city_idx = np.random.randint(0, len(state_info[1]))
    city = state_info[1][city_idx]
    postal = state_info[2][city_idx]

    # Order date and shipping
    order_date = start_date + timedelta(days=np.random.randint(0, date_range_days))
    ship_mode = np.random.choice(SHIP_MODES, p=SHIP_MODE_WEIGHTS)
    ship_delay = np.random.randint(*SHIP_MODE_DAYS[ship_mode]) if SHIP_MODE_DAYS[ship_mode][1] > 0 else 0
    ship_date = order_date + timedelta(days=ship_delay)

    order_id = f"US-{order_date.year}-{order_counter + 100001}"
    order_counter += 1

    # How many items in this order (1-7)
    n_items = np.random.choice([1, 2, 3, 4, 5, 6, 7], p=[0.30, 0.25, 0.20, 0.12, 0.07, 0.04, 0.02])

    for _ in range(n_items):
        if len(rows) >= N_ROWS:
            break

        # Pick category, sub-category, product
        cat_name = np.random.choice(list(CATEGORIES.keys()), p=[0.22, 0.60, 0.18])
        cat = CATEGORIES[cat_name]
        sub_cat = np.random.choice(cat["sub_cats"])
        product = np.random.choice(cat["products"][sub_cat])
        prod_id = f"{cat_name[:3].upper()}-{sub_cat[:2].upper()}-{np.random.randint(10000000, 99999999)}"

        # Sales, quantity, discount, profit
        quantity = np.random.choice([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
                                     p=[0.25, 0.20, 0.18, 0.12, 0.08, 0.05, 0.03, 0.03, 0.02, 0.01, 0.01, 0.01, 0.005, 0.005])
        base_price = np.random.uniform(*cat["price_range"])
        discount = np.random.choice([0.0, 0.0, 0.0, 0.0, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5],
                                     p=[0.35, 0.10, 0.05, 0.05, 0.15, 0.10, 0.08, 0.06, 0.04, 0.02])
        sales = round(base_price * quantity * (1 - discount), 2)
        margin = np.random.uniform(*cat["profit_margin"])
        # High discount -> more likely negative profit
        if discount >= 0.3:
            margin = np.random.uniform(-0.40, 0.05)
        profit = round(sales * margin, 2)

        rows.append({
            "Row ID": row_id,
            "Order ID": order_id,
            "Order Date": order_date.strftime("%m/%d/%Y"),
            "Ship Date": ship_date.strftime("%m/%d/%Y"),
            "Ship Mode": ship_mode,
            "Customer ID": cust["Customer ID"],
            "Customer Name": cust["Customer Name"],
            "Segment": cust["Segment"],
            "Country": "United States",
            "City": city,
            "State": state_name,
            "Postal Code": postal,
            "Region": region,
            "Product ID": prod_id,
            "Category": cat_name,
            "Sub-Category": sub_cat,
            "Product Name": product,
            "Sales": sales,
            "Quantity": quantity,
            "Discount": discount,
            "Profit": profit
        })
        row_id += 1

df = pd.DataFrame(rows)
print(f"Generated {len(df)} rows")

# ─── INJECT SOME NULLS FOR CLEANING PRACTICE ────────────────────────────────
print("Injecting realistic nulls for data cleaning practice...")
null_indices_postal = np.random.choice(df.index, size=15, replace=False)
df.loc[null_indices_postal, "Postal Code"] = np.nan

null_indices_city = np.random.choice(df.index, size=5, replace=False)
df.loc[null_indices_city, "City"] = np.nan

# ─── SAVE RAW CSV ────────────────────────────────────────────────────────────
os.makedirs(os.path.dirname(CSV_RAW_PATH), exist_ok=True)
df.to_csv(CSV_RAW_PATH, index=False)
print(f"Raw CSV saved to {CSV_RAW_PATH}")

# ─── DATA CLEANING ───────────────────────────────────────────────────────────
print("\n--- DATA CLEANING ---")

# 1. Check for nulls
print(f"Null counts before cleaning:\n{df.isnull().sum()[df.isnull().sum() > 0]}")

# 2. Fill missing postal codes with '00000' (unknown)
df["Postal Code"] = df["Postal Code"].fillna("00000")

# 3. Fill missing cities with 'Unknown'
df["City"] = df["City"].fillna("Unknown")

# 4. Parse dates properly
df["Order Date"] = pd.to_datetime(df["Order Date"], format="%m/%d/%Y")
df["Ship Date"] = pd.to_datetime(df["Ship Date"], format="%m/%d/%Y")

# 5. Ensure numeric types
df["Sales"] = df["Sales"].astype(float)
df["Profit"] = df["Profit"].astype(float)
df["Quantity"] = df["Quantity"].astype(int)
df["Discount"] = df["Discount"].astype(float)
df["Row ID"] = df["Row ID"].astype(int)

# 6. Remove exact duplicates (if any)
dupes = df.duplicated().sum()
print(f"Exact duplicates found: {dupes}")
df = df.drop_duplicates()

# 7. Validate shipping dates >= order dates
bad_ships = (df["Ship Date"] < df["Order Date"]).sum()
print(f"Bad ship dates (before order): {bad_ships}")

print(f"\nNull counts after cleaning:\n{df.isnull().sum().sum()} total nulls remaining")
print(f"Final shape: {df.shape}")
print(f"Date range: {df['Order Date'].min()} to {df['Order Date'].max()}")
print(f"\nColumn types:\n{df.dtypes}")

# ─── SAVE CLEAN CSV ─────────────────────────────────────────────────────────
df.to_csv(CSV_CLEAN_PATH, index=False)
print(f"\nClean CSV saved to {CSV_CLEAN_PATH}")

# ─── LOAD INTO SQLITE ───────────────────────────────────────────────────────
print(f"\n--- LOADING INTO SQLITE ({DB_PATH}) ---")

conn = sqlite3.connect(DB_PATH)

# Convert dates back to strings for SQLite storage (ISO format)
df_sql = df.copy()
df_sql["Order Date"] = df_sql["Order Date"].dt.strftime("%Y-%m-%d")
df_sql["Ship Date"] = df_sql["Ship Date"].dt.strftime("%Y-%m-%d")

# Rename columns to be SQL-friendly (no spaces)
df_sql.columns = [c.replace(" ", "_").replace("-", "_") for c in df_sql.columns]

df_sql.to_sql("orders", conn, if_exists="replace", index=False)

# Verify
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM orders")
count = cursor.fetchone()[0]
print(f"Rows in 'orders' table: {count}")

cursor.execute("PRAGMA table_info(orders)")
cols = cursor.fetchall()
print(f"\nTable schema:")
for col in cols:
    print(f"  {col[1]:20s} {col[2]}")

# Quick sanity check
cursor.execute("SELECT Region, COUNT(*), ROUND(SUM(Sales),2), ROUND(SUM(Profit),2) FROM orders GROUP BY Region")
print(f"\nQuick check - by region:")
print(f"  {'Region':10s} {'Orders':>8s} {'Sales':>12s} {'Profit':>12s}")
for row in cursor.fetchall():
    print(f"  {row[0]:10s} {row[1]:>8d} {row[2]:>12,.2f} {row[3]:>12,.2f}")

conn.close()
print("\nDone! Database ready for SQL queries.")
