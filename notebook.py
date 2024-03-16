# %%
#Code line to install requirements.txt
#cat requirements.txt | xargs poetry add
# %%
import pandas as pd
import glob
import time
import duckdb
# %%
conn = duckdb.connect() # create an in-memory database
# %%
# with pandas
cur_time = time.time()
df = pd.concat([pd.read_csv(f) for f in glob.glob('dataset/*.csv')])
print(f"time: {(time.time() - cur_time)}")
print(df.head(10))
# %%
# with duckdb
cur_time = time.time()
df = conn.execute("""
	SELECT *
	FROM 'dataset/*.csv'
	LIMIT 10
""").df()
print(f"time: {(time.time() - cur_time)}")
print(df)
# %%
df = conn.execute("""
	SELECT *
	FROM 'dataset/*.csv'
""").df()
conn.register("df_view", df)
conn.execute("DESCRIBE df_view").df() # doesn't work if you don't register df as a virtual table
# %%
conn.execute("SELECT COUNT(*) FROM df_view").df()
# %%
df.isnull().sum()
df = df.dropna(how='all')

# Notice we use df and not df_view
# With DuckDB you can run SQL queries on top of Pandas dataframes
conn.execute("SELECT COUNT(*) FROM df").df()
# %%
conn.execute("""SELECT * FROM df WHERE "Order ID"='295665'""").df()
# %%
conn.execute("""
CREATE OR REPLACE TABLE sales AS
	SELECT
		"Order ID"::INTEGER AS order_id,
		Product AS product,
		"Quantity Ordered"::INTEGER AS quantity,
		"Price"::VARCHAR AS price,
		"Purchase Address" AS purchase_address
	FROM df
	WHERE
		TRY_CAST("Order ID" AS INTEGER) NOTNULL
""")
# %%
conn.execute("FROM sales").df()
# %%
conn.execute("""
	SELECT 
		* EXCLUDE (product, purchase_address)
	FROM sales
	""").df()
# %%
conn.execute("""
	SELECT 
		MIN(COLUMNS(* EXCLUDE (product, purchase_address))) 
	FROM sales
	""").df()
# %%
conn.execute("""
	CREATE OR REPLACE VIEW aggregated_sales AS
	SELECT
		order_id,
		COUNT(1) as nb_orders,
		str_split(purchase_address, ',')[2] AS city,
		SUM(quantity * price_each) AS revenue
	FROM sales
	GROUP BY ALL
""")