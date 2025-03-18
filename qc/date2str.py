import polars as pl

# Sample DataFrame with a datetime column
df = pl.DataFrame(
    {
        "datetime_column": [
            pl.datetime(2023, 3, 13, 12, 0, 0),
            pl.datetime(2025, 3, 13, 12, 0, 0),
        ]
    }
)

# Convert the datetime column to string
df = df.with_columns(
    df["datetime_column"].dt.strftime("%Y-%m-%d %H:%M:%S").alias("datetime_str")
)

print(df)
