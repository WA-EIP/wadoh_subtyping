import polars as pl
import polars.selectors as cs

def polars_to_spark_temp_view(spark, polars_df, view_name):
    # Step 1: Convert Polars DF to Pandas DF
    pandas_df = polars_df.to_pandas()

    # Step 2: Convert Pandas DF to Spark DF
    spark_df = spark.createDataFrame(pandas_df)

    # Step 3: Register as temp view
    spark_df.createOrReplaceTempView(view_name)

    return view_name

def safe_insert_sql(spark, catalog: str, schema: str, table: str, source_view: str, join_key: str):
    full_table_name = f"{catalog}.{schema}.{table}"

    # Use DESCRIBE TABLE to get column names
    col_info = spark.sql(f"DESCRIBE TABLE {full_table_name}").collect()
    cols = [row['col_name'] for row in col_info if row['col_name'] and not row['col_name'].startswith('#')]

    insert_cols = ", ".join(cols)                              # for INSERT
    select_cols = ", ".join([f"r.{col}" for col in cols])      # for SELECT

    sql = f"""
    INSERT INTO {full_table_name} ({insert_cols})
    SELECT {select_cols}
    FROM {source_view} AS r
    LEFT OUTER JOIN {full_table_name} AS rt
    ON rt.{join_key} = r.{join_key}
    WHERE rt.{join_key} IS NULL
    """

    spark.sql(sql)

def safe_insert_sql_uc(
    spark, 
    catalog: str, 
    schema: str, 
    table: str, 
    source_view, 
    join_key: str
):
    full_table_name = f"{catalog}.{schema}.{table}"

    # register polars df

    try:
        col_info = spark.sql(f"DESCRIBE TABLE {full_table_name}").collect()
        cols = [row['col_name'] for row in col_info if row['col_name'] and not row['col_name'].startswith('#')]

        insert_cols = ", ".join(cols)
        select_cols = ", ".join([f"r.{col}" for col in cols])

        sql = f"""
        INSERT INTO {full_table_name} ({insert_cols})
        SELECT {select_cols}
        FROM {source_view} AS r
        LEFT OUTER JOIN {full_table_name} AS rt
        ON rt.{join_key} = r.{join_key}
        WHERE rt.{join_key} IS NULL
        """

        spark.sql(sql)

    except Exception as e:
        print(f"Error during safe insert into {full_table_name}: {e}")

def uc_catalog_to_polars(spark,query):
    """
    Usage
    -----
    Pull from dbx catalog and output to a polars dataframe
    """
    spark_df = spark.sql(query)
    output_df = pl.from_pandas(spark_df.toPandas())
    return output_df
