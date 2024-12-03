import pandas as pd
import pyarrow.parquet as pq
import numpy as np
from connector import SnowflakeConnector


class OrdersTable:
    def __init__(self, user, password, account, warehouse, database):
        # Establish connection
        self.connector = SnowflakeConnector(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
        )
        self.connector.connect()

        # Create and use database and schema
        self.connector.execute_query(
            "CREATE DATABASE IF NOT EXISTS test_db_karthik_orders;")
        self.connector.execute_query(
            "CREATE SCHEMA IF NOT EXISTS test_schema_karthik_orders;")
        self.connector.execute_query("USE DATABASE test_db_karthik_orders;")
        self.connector.execute_query("USE SCHEMA test_schema_karthik_orders;")

    # Create the orders table
    def create_orders_table(self):
        self.connector.execute_query("""
            CREATE TABLE IF NOT EXISTS orders (
                o_orderkey INT,
                o_custkey STRING,
                o_orderstatus STRING,
                o_totalprice INT,
                o_orderdate DATE,
                o_orderpriority STRING,
                o_clerk STRING,
                o_shippriority INT,
                o_comment STRING
            );
        """)
        print("Orders table created.")

    # def upload_order_parquet_to_stage(self, local_parquet_path):
    #     try:
    #         # Upload the Parquet file to the specified stage
    #         self.connector.execute_query(f"PUT file://{local_parquet_path} @~/staging_directory AUTO_COMPRESS=TRUE;")
    #         print("Parquet file uploaded to Snowflake stage successfully.")
    #         self.connector.execute_query("LIST @~/staging_directory;")
    #     except Exception as e:
    #         print(f"Failed to upload Parquet file to stage: {e}")

    def upload_csv_to_stage(self, local_csv_path):
        try:
            self.connector.execute_query(
                f"PUT file://{local_csv_path} @~/staging_directory;")
            print("CSV file uploaded to Snowflake stage successfully.")
        except Exception as e:
            print(f"Failed to upload CSV file to stage: {e}")

    # def escape_sql_string(self, value):
    #     if value is None:
    #         return 'NULL'
    #     return str(value).replace("'", "''").replace('\\', '\\\\')

    # Insert data into the orders table
    # def insert_orders_data(self):
    #     try:
    #         self.connector.execute_query("""
    #             COPY INTO orders
    #             FROM @~/staging_directory/ordersdata.csv
    #             FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
    #             ON_ERROR = 'CONTINUE';  -- Continue loading even when encountering errors
    #         """)
    #         print("Data inserted into orders table successfully.")
    #     except Exception as e:
    #         print(f"Failed to insert data: {e}")

    def orders_data(self):
        # Absolute path to your Parquet file
        parquet_file_path = '/Users/karthikrayalli/Documents/Arokee/snowflake/ordersdata.csv'
        self.upload_csv_to_stage(parquet_file_path)
        self.create_orders_table()
        self.insert_orders_data()

    def noncluster_orders_data(self):
        result = self.connector.execute_query(
            "SELECT * FROM orders WHERE YEAR(o_orderdate) = 1992 LIMIT 10;")
        return result
#  AND o_orderpriority='1-URGENT'
    # def insert_orders_data(self):
    #     try:
    #         df = pd.read_parquet('/Users/karthikrayalli/Documents/Arokee/snowflake/ordersdata.parquet')
    #         df.columns = [col.lower() for col in df.columns]
    #         df = df.replace({np.nan: None})  # Replace NaN with None for SQL compatibility
    #         data_tuples = [tuple(x) for x in df.to_numpy()]
    #         batch_size = 100000
            
    #         for i in range(0, len(data_tuples), batch_size):
    #             batch = data_tuples[i:i + batch_size]
    #             values_str = ",".join([
    #                 "({})".format(", ".join(
    #                     "NULL" if value is None else "'{}'".format(self.escape_sql_string(str(value)))
    #                     for value in row
    #                 )) for row in batch
    #             ])

    #             insert_query = f"""
    #             INSERT INTO orders (
    #             o_orderkey,
    #             o_custkey,
    #             o_orderstatus,
    #             o_totalprice,
    #             o_orderdate,
    #             o_orderpriority,
    #             o_clerk,
    #             o_shippriority,
    #             o_comment)
    #             VALUES {values_str};
    #             """

    #             self.connector.execute_query(insert_query)
    #             self.connector.connection.commit()
    #             print(f"Inserted batch from index {i} to {i + len(batch) - 1}.")
    #     except Exception as e:
    #         print(f"Failed to insert data: {e}")


    def escape_sql_string(self, value):
        # Escaping single quotes in strings to prevent SQL errors
        return value.replace("'", "''")

    def insert_orders_data(self):
        try:
            # Load parquet data into a DataFrame
            df = pd.read_parquet('/Users/karthikrayalli/Documents/Arokee/snowflake/ordersdata.parquet')
            df.columns = [col.lower() for col in df.columns]
            df = df.replace({np.nan: None})  # Replace NaN with None for SQL compatibility
            data_tuples = [tuple(x) for x in df.to_numpy()]
            batch_size = 100000
            
            for i in range(0, len(data_tuples), batch_size):
                batch = data_tuples[i:i + batch_size]
                values_str = ",".join([
                    "({})".format(", ".join(
                        "NULL" if value is None else "'{}'".format(self.escape_sql_string(str(value)))
                        for value in row
                    )) for row in batch
                ])

                insert_query = f"""
                INSERT INTO orders (
                o_orderkey,
                o_custkey,
                o_orderstatus,
                o_totalprice,
                o_orderdate,
                o_orderpriority,
                o_clerk,
                o_shippriority,
                o_comment)
                VALUES {values_str};
                """

                # Use a cursor to execute the query
                self.connector.execute_query(insert_query)

                print(f"Inserted batch from index {i} to {i + len(batch) - 1}.")
            
            # Commit the transaction after all batches
            self.connector.commit()
            
        except Exception as e:
            print(f"Failed to insert data: {e}")