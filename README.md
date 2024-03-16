# DuckDB Tutorial - Getting started for beginners

Thanks to [Data with Marc](https://www.youtube.com/watch?v=AjsB6lM2-zw).

# Materials

---

Dataset → https://www.kaggle.com/datasets/kushagra1211/usa-sales-product-datasetcleaned

Notion → https://robust-dinosaur-2ef.notion.site/DuckDB-Tutorial-Getting-started-for-beginners-b80bf0de8d6142d6979e78e59ffbbefe

## Dependencies

```yaml
# requirement.txt

duckdb==0.6.1
pandas==1.5.2
seaborn==0.12.1
matplotlib==3.6.2
```

```python
import pandas as pd
import glob
import time
import duckdb

conn = duckdb.connect() # create an in-memory database
```

## Print the first 10 rows

```python
# with pandas
cur_time = time.time()
df = pd.concat([pd.read_csv(f) for f in glob.glob('dataset/*.csv')])
print(f"time: {(time.time() - cur_time)}")
print(df.head(10))
```

```python
# with duckdb
cur_time = time.time()
df = conn.execute("""
	SELECT *
	FROM 'dataset/*.csv'
	LIMIT 10
""").df()
print(f"time: {(time.time() - cur_time)}")
print(df)
```

## Checking the types of the columns

```python
df = conn.execute("""
	SELECT *
	FROM 'dataset/*.csv'
""").df()
conn.register("df_view", df)
conn.execute("DESCRIBE df_view").df() # doesn't work if you don't register df as a virtual table
```

## Counting rows

```python
conn.execute("SELECT COUNT(*) FROM df_view").df()
```

## Drop nulls

```python
df.isnull().sum()
df = df.dropna(how='all')

# Notice we use df and not df_view
# With DuckDB you can run SQL queries on top of Pandas dataframes
conn.execute("SELECT COUNT(*) FROM df").df()
```

## Where clause

```python
conn.execute("""SELECT * FROM df WHERE "Order ID"='295665'""").df()
```

<aside>
💡 Make sure to use 3 double quotes at the beginning and the end of your statement + use doubles quotes if your column is not a single word column

</aside>

## Create a table and load the data

<aside>
💡 A View/Virtual Table is a SELECT statement. That statement is run every time the view is referenced in a query. Views are great for abstracting the complexity of the underlying tables they reference.

</aside>

```python
conn.execute("""
CREATE OR REPLACE TABLE sales AS
	SELECT
		"Order ID"::INTEGER AS order_id,
		Product AS product,
		"Quantity Ordered"::INTEGER AS quantity,
		"Price Each"::DECIMAL AS price_each,
		strptime("Order Date", '%m/%d/%Y %H:%M')::DATE as order_date,
		"Purchase Address" AS purchase_address
	FROM df
	WHERE
		TRY_CAST("Order ID" AS INTEGER) NOTNULL
""")
```

<aside>
💡 TRY_CAST returns null if the cast fails. By using TRY_CAST NOTNULL we skip malformatted rows.

</aside>

## FROM-first clause

```python
conn.execute("FROM sales").df()
```

## Exclude

```python
conn.execute("""
	SELECT 
		* EXCLUDE (product, order_date, purchase_address)
	FROM sales
	""").df()
```

## The Columns Expression

```python
conn.execute("""
	SELECT 
		MIN(COLUMNS(* EXCLUDE (product, order_date, purchase_address))) 
	FROM sales
	""").df()
```

## Create a VIEW

```python
conn.execute("""
	CREATE OR REPLACE VIEW aggregated_sales AS
	SELECT
		order_id,
		COUNT(1) as nb_orders,
		MONTH(order_date) as month,
		str_split(purchase_address, ',')[2] AS city,
		SUM(quantity * price_each) AS revenue
	FROM sales
	GROUP BY ALL
""")
```

<aside>
💡 Since VIEWS are recreated each time a query reference them, if new data is added to the sales table, the VIEW gets updated as well

</aside>

## Export to Parquet files

```python
conn.execute("COPY (FROM aggregated_sales) TO 'aggregated_sales.parquet' (FORMAT 'parquet')")
```

## Query Parquet files

```python
conn.execute("FROM aggregated_sales.parquet").df()
```

<aside>
💡 Querying Parquet files give much better performances than with CSV files

</aside>
