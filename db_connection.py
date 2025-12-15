import mysql.connector
import pandas as pd
from dotenv import load_dotenv
import os
from tqdm import tqdm  # for progress bar

# Load .env file
load_dotenv()

# Read credentials
host = os.getenv('HOST')
user = os.getenv('USER')
password = os.getenv('PASS')
database = os.getenv('DATABASE')

# Connect to MySQL
conn_mysql = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database,
    autocommit=False  # manage commits manually
)
cursor_mysql = conn_mysql.cursor()
print("MySQL connection established!")

# Step 2: Create the dashboard table
cursor_mysql.execute("""
CREATE TABLE IF NOT EXISTS amazon_data_2015_to_2025 (
    TransactionID VARCHAR(50),
    OrderDate DATE,
    CustomerID VARCHAR(50),
    ProductID VARCHAR(50),
    ProductName VARCHAR(255),
    Category VARCHAR(100),
    SubCategory VARCHAR(100),
    Brand VARCHAR(100),
    OriginalPrice DECIMAL(15,2),
    DiscountedPrice DECIMAL(15,2),
    Quantity INT,
    FinalAmount DECIMAL(15,2),
    CustomerCity VARCHAR(100),
    CustomerState VARCHAR(100),
    CustomerTier VARCHAR(50),
    CustomerSpendingTier VARCHAR(50),
    CustomerAgeGroup VARCHAR(50),
    PaymentMethod VARCHAR(50),
    DeliveryDays VARCHAR(50),
    DeliveryType VARCHAR(50),
    IsPrimeMember VARCHAR(10),
    IsFestivalSale VARCHAR(10),
    FestivalName VARCHAR(100),
    CustomerRating VARCHAR(50),
    ReturnStatus VARCHAR(50),
    OrderMonth INT,
    OrderYear INT,
    OrderQuarter INT,
    ProductWeight DECIMAL(10,2),
    IsPrimeEligible VARCHAR(10),
    ProductRating DECIMAL(3,2)
);
""")
conn_mysql.commit()
print("Dashboard table created successfully!")

# Step 3: Load cleaned CSV data
df = pd.read_csv(r"C:\Users\anbuh\OneDrive\Desktop\vicky all\amazon_data\amazon_india_cleaned_data.csv")

# Step 4: Select only necessary columns and copy to avoid warnings
cols_to_insert = [
    'transaction_id', 'order_date', 'customer_id', 'product_id', 'product_name', 'category',
    'subcategory', 'brand', 'original_price_inr', 'discounted_price_inr', 'quantity',
    'final_amount_inr', 'customer_city', 'customer_state', 'customer_tier',
    'customer_spending_tier', 'customer_age_group', 'payment_method', 'delivery_days',
    'delivery_type', 'is_prime_member', 'is_festival_sale', 'festival_name', 'customer_rating',
    'return_status', 'order_month', 'order_year', 'order_quarter', 'product_weight_kg',
    'is_prime_eligible', 'product_rating'
]

df_to_insert = df[cols_to_insert].copy()

# Rename columns to match MySQL table
df_to_insert.rename(columns={
    'transaction_id':'TransactionID',
    'order_date':'OrderDate',
    'customer_id':'CustomerID',
    'product_id':'ProductID',
    'product_name':'ProductName',
    'category':'Category',
    'subcategory':'SubCategory',
    'brand':'Brand',
    'original_price_inr':'OriginalPrice',
    'discounted_price_inr':'DiscountedPrice',
    'quantity':'Quantity',
    'final_amount_inr':'FinalAmount',
    'customer_city':'CustomerCity',
    'customer_state':'CustomerState',
    'customer_tier':'CustomerTier',
    'customer_spending_tier':'CustomerSpendingTier',
    'customer_age_group':'CustomerAgeGroup',
    'payment_method':'PaymentMethod',
    'delivery_days':'DeliveryDays',
    'delivery_type':'DeliveryType',
    'is_prime_member':'IsPrimeMember',
    'is_festival_sale':'IsFestivalSale',
    'festival_name':'FestivalName',
    'customer_rating':'CustomerRating',
    'return_status':'ReturnStatus',
    'order_month':'OrderMonth',
    'order_year':'OrderYear',
    'order_quarter':'OrderQuarter',
    'product_weight_kg':'ProductWeight',
    'is_prime_eligible':'IsPrimeEligible',
    'product_rating':'ProductRating'
}, inplace=True)

# Replace NaN with None
df_to_insert = df_to_insert.where(pd.notnull(df_to_insert), None)

# Step 5: Insert in batches to avoid MySQL disconnect
insert_query = """
INSERT INTO amazon_data_2015_to_2025 (
    TransactionID, OrderDate, CustomerID, ProductID, ProductName, Category, SubCategory, Brand,
    OriginalPrice, DiscountedPrice, Quantity, FinalAmount, CustomerCity, CustomerState,
    CustomerTier, CustomerSpendingTier, CustomerAgeGroup, PaymentMethod, DeliveryDays, DeliveryType,
    IsPrimeMember, IsFestivalSale, FestivalName, CustomerRating, ReturnStatus, OrderMonth,
    OrderYear, OrderQuarter, ProductWeight, IsPrimeEligible, ProductRating
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

batch_size = 5000  # insert 5000 rows at a time
for i in tqdm(range(0, len(df_to_insert), batch_size), desc="Inserting batches"):
    batch = df_to_insert.iloc[i:i+batch_size].values.tolist()
    cursor_mysql.executemany(insert_query, batch)
    conn_mysql.commit()

print("All data inserted successfully into the dashboard table!")

# Close connection
cursor_mysql.close()
conn_mysql.close()
print("MySQL connection closed.")
