import pandas as pd
import pyarrow.parquet as pq
import numpy as np
from connector import SnowflakeConnector
import os

class TimeTravel:
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
    # Initialize your own database and schema
    self.connector.execute_query("CREATE DATABASE IF NOT EXISTS test_db_karthik_orders;")
    self.connector.execute_query("USE DATABASE test_db_karthik_orders;")
    self.connector.execute_query("CREATE SCHEMA IF NOT EXISTS test_schema_karthik_orders;")
    self.connector.execute_query("USE SCHEMA test_schema_karthik_orders;")


  def create_time_travel_table(self):
    self.connector.execute_query("""
      CREATE TABLE IF NOT EXISTS time_travel (
        o_orderkey INT,
        o_custkey STRING,
        o_orderstatus STRING,
        o_totalprice INT,
        o_orderdate DATE,
        o_orderpriority STRING,
        o_clerk STRING,
        o_shippriority INT,
        o_comment STRING
      )
      DATA_RETENTION_TIME_IN_DAYS = 3;
    """)
  def escape_sql_string(self,value):
    if value is None:
        return 'NULL'
    return str(value).replace("'", "''").replace('\\', '\\\\')

  def show_tables(self):
    try:
        # Example of loading and processing the Parquet file
        # project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        # print("project_root",project_root)
        # parquet_file_path = os.path.join(project_root, 'parquet_files', 'ordersdata.parquet')
        
        # Load the Parquet file and process it as a DataFrame
        # df = pd.read_parquet(parquet_file_path)
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
            INSERT INTO time_travel (
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

            self.connector.execute_query(insert_query)
            self.connector.connection.commit()
            print(f"Inserted batch from index {i} to {i + len(batch) - 1}.")
    except Exception as e:
        print(f"Failed to insert data: {e}")


  def time_travel_update(self):
    # self.connector.execute_query("""
    #     select min(O_ORDERKEY), max(O_ORDERKEY)  from time_travel;
    # """) 01b802af-0000-c877-0008-1fc20002311a
    # self.connector.execute_query("""
    #   UPDATE time_travel SET o_orderpriority='1-URGENT' WHERE o_orderkey < 10000 AND o_orderpriority <> '1-URGENT';
    # """)
    # self.connector.execute_query("""
    #   DELETE FROM time_travel WHERE o_orderkey < 10000 AND o_orderpriority = '1-URGENT';
    # """)
    self.connector.execute_query("""
      SELECT count(*) FROM time_travel;
    """)

  def time_travel_at_and_before(self):
    # self.connector.execute_query("""
    #   SELECT count(*) FROM time_travel BEFORE(statement =>'01b802b1-0000-c877-0008-1fc20002313a')
    # """)
    # self.connector.execute_query("""
    #   SELECT count(*) FROM time_travel AT(statement =>'01b802b1-0000-c877-0008-1fc20002313a')
    # """)
    # self.connector.execute_query("""
    #   SELECT count(*) FROM time_travel AT(OFFSET => -60*5);
    # """)
    # self.connector.execute_query("""
    #   ALTER TABLE time_travel SET DATA_RETENTION_TIME_IN_DAYS=5;
    # """)
    self.connector.execute_query("""
      show tables like 'time_travel';
    """)
  def create_transient_table1(self):
    self.connector.execute_query("""
      CREATE TRANSIENT TABLE IF NOT EXISTS time_travel_transient0 (
        o_orderkey INT,
        o_custkey STRING,
        o_orderstatus STRING,
        o_totalprice INT,
        o_orderdate DATE,
        o_orderpriority STRING,
        o_clerk STRING,
        o_shippriority INT,
        o_comment STRING
      )
      DATA_RETENTION_TIME_IN_DAYS = 0;
    """)
  def create_transient_table2(self):
    self.connector.execute_query("""
      CREATE TRANSIENT TABLE IF NOT EXISTS time_travel_transient1 (
        o_orderkey INT,
        o_custkey STRING,
        o_orderstatus STRING,
        o_totalprice INT,
        o_orderdate DATE,
        o_orderpriority STRING,
        o_clerk STRING,
        o_shippriority INT,
        o_comment STRING
      )
      DATA_RETENTION_TIME_IN_DAYS = 2;
    """)

  def create_tmp_table(self):
    self.connector.execute_query("""
      CREATE TEMPORARY TABLE IF NOT EXISTS time_travel_temporary (
        o_orderkey INT,
        o_custkey STRING,
        o_orderstatus STRING,
        o_totalprice INT,
        o_orderdate DATE,
        o_orderpriority STRING,
        o_clerk STRING,
        o_shippriority INT,
        o_comment STRING
      )
      DATA_RETENTION_TIME_IN_DAYS = 2;
    """)

  def create_updrop_table(self):
    self.connector.execute_query("""
      CREATE TABLE IF NOT EXISTS time_travel_undrop (
        o_orderkey INT,
        o_custkey STRING,
        o_orderstatus STRING,
        o_totalprice INT,
        o_orderdate DATE,
        o_orderpriority STRING,
        o_clerk STRING,
        o_shippriority INT,
        o_comment STRING
      )
      DATA_RETENTION_TIME_IN_DAYS = 2;
    """)

  def insert_data_into_table1(self):
    self.connector.execute_query("""
      INSERT INTO time_travel_undrop
        SELECT * FROM time_travel limit 100000
    """)

  def undrop_fun(self):
    # self.connector.execute_query("""
    #   SELECT count(*) FROM time_travel_undrop
    # """)
    # self.connector.execute_query("""
    #   DROP TABLE time_travel_undrop
    # """)
    # self.connector.execute_query("""
    #    SELECT count(*) FROM time_travel_undrop
    # """)
    # self.connector.execute_query("""
    #   UNDROP TABLE time_travel_undrop
    # """)
    self.connector.execute_query("""
      SELECT * FROM time_travel_undrop
    """)

  def time_travel_clone_fun(self):
    self.connector.execute_query("""
      CREATE TABLE IF NOT EXISTS time_travel_before_clone (
        o_orderkey INT,
        o_custkey STRING,
        o_orderstatus STRING,
        o_totalprice INT,
        o_orderdate DATE,
        o_orderpriority STRING,
        o_clerk STRING,
        o_shippriority INT,
        o_comment STRING
      )
      DATA_RETENTION_TIME_IN_DAYS = 1;
    """)

  def insert_data_into_tt_before_table(self):
    self.connector.execute_query("""
      INSERT INTO time_travel_before_clone
        SELECT * FROM time_travel limit 100000
    """)
  def clone_fun(self):
    # self.connector.execute_query("""
    #   SELECT count(*) FROM time_travel_before_clone;
    # """)
    # self.connector.execute_query("""
    #   DELETE FROM time_travel_before_clone WHERE o_orderpriority='5-LOW'; 01b802e9-0000-c94d-0008-1fc20002724e
    # """)
    # self.connector.execute_query("""
    #   SELECT count(*) FROM time_travel_before_clone;
    # """)
    # self.connector.execute_query("""
    #   UPDATE time_travel_before_clone SET o_orderstatus='X' WHERE o_orderpriority='3-MEDIUM'; 01b802ea-0000-c94d-0008-1fc20002728a
    # """)
    # self.connector.execute_query("""
    #   SELECT count(*) FROM time_travel_before_clone;
    # """)
    # self.connector.execute_query("""
    #   INSERT INTO time_travel_before_clone SELECT * FROM orders WHERE o_orderpriority='5-LOW' LIMIT 50;
    # """)
    self.connector.execute_query("""
      SELECT count(*) FROM time_travel_before_clone;
    """)

  def create_table_with_clone(self):
    # self.connector.execute_query("""
    #   CREATE TABLE time_travel_after_clone clone time_travel_before_clone before (statement => '01b802ea-0000-c94d-0008-1fc20002728a')
    # """)
    self.connector.execute_query("""
      SHOW TABLES LIKE 'time_travel%clone%';
    """)

  def orders_data(self):
    local_csv_path = r"/Users/karthikrayalli/Documents/Arokee/snowflake/ordersdata.csv"
    
    # Convert the CSV to a Parquet file using pandas
    parquet_file_path = r'/Users/karthikrayalli/Documents/Arokee/snowflake/ordersdata.parquet'
    df = pd.read_csv(local_csv_path)
    df.to_parquet(parquet_file_path, engine='pyarrow', index=False)
    
    # Print column names to verify conversion
    table = pq.read_table(parquet_file_path)
    print(table.column_names)

    # Upload the Parquet file to the Snowflake stage
    # try:
    #     # Execute the PUT command to upload the file
    #     self.connector.execute_query(f"PUT file://{parquet_file_path} @~/staging_directory;")
    #     print("Parquet file uploaded to Snowflake stage successfully.")
    # except Exception as e:
    #     print(f"Failed to upload Parquet file to stage: {e}")
    