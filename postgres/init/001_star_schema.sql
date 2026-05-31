CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    source_customer_id INTEGER,
    first_name TEXT,
    last_name TEXT,
    age INTEGER,
    email TEXT,
    country TEXT,
    postal_code TEXT,
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT
);

CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_key TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    source_seller_id INTEGER,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    country TEXT,
    postal_code TEXT
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_key TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    source_product_id INTEGER,
    name TEXT,
    category TEXT,
    price NUMERIC(12, 2),
    stock_quantity INTEGER,
    pet_category TEXT,
    weight NUMERIC(12, 3),
    color TEXT,
    size TEXT,
    brand TEXT,
    material TEXT,
    description TEXT,
    rating NUMERIC(3, 2),
    reviews INTEGER,
    release_date DATE,
    expiry_date DATE
);

CREATE TABLE IF NOT EXISTS dim_stores (
    store_key TEXT PRIMARY KEY,
    name TEXT,
    location TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    phone TEXT,
    email TEXT
);

CREATE TABLE IF NOT EXISTS dim_suppliers (
    supplier_key TEXT PRIMARY KEY,
    name TEXT,
    contact TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    country TEXT
);

CREATE TABLE IF NOT EXISTS dim_dates (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_event_id TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    source_sale_id INTEGER,
    customer_key TEXT NOT NULL,
    seller_key TEXT NOT NULL,
    product_key TEXT NOT NULL,
    store_key TEXT NOT NULL,
    supplier_key TEXT NOT NULL,
    sale_date_key INTEGER NOT NULL,
    sale_quantity INTEGER,
    sale_total_price NUMERIC(12, 2),
    product_unit_price NUMERIC(12, 2)
);

CREATE INDEX IF NOT EXISTS fact_sales_sale_date_idx ON fact_sales (sale_date_key);
CREATE INDEX IF NOT EXISTS fact_sales_customer_idx ON fact_sales (customer_key);
CREATE INDEX IF NOT EXISTS fact_sales_product_idx ON fact_sales (product_key);
