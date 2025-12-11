import pandas as pd
import numpy as np

# # np.random_seed(42)
n_rows = 5_000

data = {
    "category": np.random.choice(["Electronics", "Clothing", "Food", "Books"], size = n_rows),
    "region": np.random.choice(["North", "South", "East", "West"], size=n_rows),
    "amount": np.random.rand(n_rows) * 1000,
    "quantity": np.random.randint(1, 100, size=n_rows),
}

df_pandas = pd.DataFrame(data)
df_pandas.to_csv("sales_data.csv", index=False)

df_pd = pd.read_csv("sales_data.csv")
print(df_pd.head())
result_pd = df_pd[np.logical_and((df_pd["amount"] > 500),(df_pd['category'] == "Electronics"))]
print(result_pd.head())

result_pd = df_pd[["category", "amount"]]
print(result_pd.head())

result_pd = df_pd.groupby("category").agg({
    "amount": ["sum", "mean"],
    "quantity": "sum"
})
print(result_pd.head())


result_pd = df_pd.assign(
    amount_with_tax = df_pd["amount"] * 1.1,
    high_value = df_pd["amount"] > 500
)
print(result_pd.head())

import polars as pl

df_pl = pl.read_csv("sales_data.csv")
result_pl = df_pl.filter(
    (pl.col("amount") > 500) & (pl.col("category") == "Electronics")
)
print(result_pl.head())

result_pl = df_pl.select(['category', 'amount'])
print(result_pl.head())

result_pl = df_pl.group_by("category").agg([
    pl.col("amount").sum().alias("amount_sum"),
    pl.col("amount").mean().alias("amount_mean"),
    pl.col("quantity").sum().alias("quantity_sum"),
])
print(result_pl.head())

result_pl = df_pl.with_columns([
    (pl.col("amount") * 1.1).alias("amount_with_tax"),
    (pl.col("amount") > 500).alias("high_value")
])
print(result_pl.head())

import duckdb

result_duckdb = duckdb.sql("""
            SELECT * FROM 'sales_data.csv'
            WHERE amount > 500 and category = 'Electronics'                           
""").df()
print(result_duckdb.head())

result_duckdb = duckdb.sql("""
        select category, amount from 'sales_data.csv'
""").df()
print(result_duckdb.head())

result_duckdb = duckdb.sql("""
    select
                           category,
                           sum(amount) as amount_sum,
                           avg(amount) as amount_mean,
                           sum(quantity) as quantity_sum
                        from 'sales_data.csv'
                        group by category
""").df()
print(result_duckdb.head())

result_duckdb = duckdb.sql("""
                select *,
                           amount * 1.1 as amount_with_tax,
                           amount > 500 as high_value
                from df_pd
""").df()
print(result_duckdb.head())


result_pd = df_pd.assign(
    value_tier = np.where(
        df_pd['amount'] > 700, 'high',
        np.where(df_pd['amount'] > 300, 'medium', 'low')
    )
)
print(result_pd[['category', 'amount', 'value_tier']].head())

result_pl = df_pl.with_columns(
    pl.when(pl.col("amount") > 700).then(pl.lit("high"))
    .when(pl.col("amount") > 300).then(pl.lit("medium"))
    .otherwise(pl.lit("low"))
    .alias("value_tier")
)
print(result_pl.select(["category", "amount", "value_tier"]).head())

result_duckdb = duckdb.sql("""
    SELECT category, amount,
            case
                        when amount > 700 then 'high'
                        when amount > 300 then 'medium'
                        else 'low'
            end as value_tier                           
    from df_pd
""").df()
print(result_duckdb.head())

result_pd = df_pd.assign(
    category_avg = df_pd.groupby("category")['amount'].transform('mean'),
    category_rank = df_pd.groupby('category')['amount'].rank(ascending=False)
)
print(result_pd[['category', 'amount', 'category_avg', 'category_rank']].head())

result_pl = df_pl.with_columns([
    pl.col("amount").mean().over("category").alias("category_avg"),
    pl.col("amount").rank(descending=True).over("category").alias("category_rank")
])
print(result_pl.select(["category", 'amount', 'category_avg', 'category_rank']).head())

result_duckdb = duckdb.sql("""
    SELECT category, amount,
            avg(amount) over (partition by category) as category_avg,
            rank() over (partition by category order by amount desc) as category_rank
    from df_pd
""").df()
print(result_duckdb.head())


def pandas_query():
    return (
        pd.read_csv("sales_data.csv")
        .query("amount > 100")
        .groupby('category')['amount']
        .mean()
    )

query_pl = (
    pl.scan_csv("sales_data.csv")
     .filter(pl.col("amount") > 100)
     .group_by("category")
     .agg(pl.col("amount").mean().alias("avg_amount"))
)
print(query_pl.explain())

def polars_query():
    return (
        pl.scan_csv("sales_data.csv")
         .filter(pl.col("amount") > 100)
         .group_by("category")
         .agg(pl.col("amount").mean().alias("avg_amount"))
         .collect()
    )

def duckdb_query():
    return duckdb.sql("""
            select category, avg(amount) as avg_amount
            from 'sales_data.csv'
            where amount > 100
            group by category
    """).df()

df_pd = pd.read_csv("sales_data.csv")
df_pl = pl.read_csv("sales_data.csv")

def pandas_groupby():
    return df_pd.groupby("category")["amount"].mean()

def polars_groupby():
    return df_pl.group_by("category").agg(pl.col("amount").mean())

def duckdb_groupby():
    return duckdb.sql("""
        select category, avg(amount)
        from df_pd
        group by category
    """).df()

df_pd_mem = pd.read_csv("sales_data.csv")
pandas_mem = df_pd_mem.memory_usage(deep=True).sum() / 1e3

result_pl_stream = (pl.scan_csv("sales_data.csv")
                    .group_by("category")
                    .agg(pl.col("amount").mean())
                    .collect(streaming=True))
polars_mem = result_pl_stream.estimated_size() / 1e3

(pl.scan_csv("sales_data.csv")
 .filter(pl.col("amount") > 500)
 .sink_parquet("filtered_sales.parquet")
 )

duckdb.sql("SET memory_limit = '500MB'")
# duckdb.sql("SET temp_directory = '/tmp/duckdb_temp")

result_duckdb_mem = duckdb.sql("""
    select category, avg(amount) as avg_amount
    from 'sales_data.csv'
    group by category                                      
""").df()

duckdb_mem = result_duckdb_mem.memory_usage(deep=True).sum() / 1e3


orders_pd = pd.DataFrame({
    'order_id': range(1_000),
    'customer_id': np.random.randint(1, 1000, size=1_000),
    "amount": np.random.rand(1_000) * 500
})
customers_pd = pd.DataFrame({
    'customer_id': range(100_000),
    'region': np.random.choice(["North", "South", "East", "West"], size=1_00_0)
})
orders_pl = pl.from_pandas(orders_pd)
customers_pl = pl.from_pandas(customers_pd)

def pandas_join():
    return orders_pd.merge(customers_pd, on="customer_id", how="left")

def polars_join():
    return orders_pl.join(customers_pl, on="customer_id", how="left")

def duckdb_join():
    return duckdb.sql("""
        select o.*, c.region
        from orders_pd o
        left join customers_pd c on o.customer_id = c.customer_id                       
""").df()

df = pd.DataFrame({
    "product": ["A", "B", "C"],
    "sales": [100, 200, 150]
})

result = duckdb.sql("SELECT * FROM df where sales > 120")
print(result)

df_polars = pl.DataFrame({
    "feature1": [1,2,3],
    'feature2': [4,5,6],
    'target':[0,1,0]
})

df_pandas = df_polars.to_pandas()
print(type(df_pandas))

result = duckdb.sql("""
    select category, sum(amount) as total
    from 'sales_data.csv'
    group by category
""").pl()
print(type(result))
print(result)

aggregated = duckdb.sql("""
    select category, region,
           sum(amount) as total_amount,
           count(*) as order_count
    from 'sales_data.csv'
    group by category, region
""").pl()

enriched = (
    aggregated
    .with_columns([
        (pl.col("total_amount") / pl.col("order_count")).alias("avg_order_value"),
        pl.col("category").str.to_uppercase().alias("category_upper")
    ])
    .filter(pl.col("order_count") > 1000)
)

final_df = enriched.to_pandas()
print(final_df.head())
