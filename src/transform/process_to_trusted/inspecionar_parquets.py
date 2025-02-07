import pyarrow.parquet as pq

parquet_path = "C:/Users/seysa/Downloads/part-00000-08377c2a-1c14-4553-8c2d-16e2ad5ef2aa-c000.snappy.parquet"
table = pq.read_table(parquet_path)
dataframe = table.to_pandas()

print("dataframe.describe:")
print(dataframe.describe())
print("\n\n")
print("dataframe.info:")
print(dataframe.info())
print("\n\n")
print("dataframe.head:")
print(dataframe.head(50))
print("\n\n")