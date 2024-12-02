from connector import SnowflakeConnector

class DataBaseCreation:
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
  # Create a new database
  def create_database(self):
    self.connector.execute_query("CREATE DATABASE IF NOT EXISTS test_db;")
    self.connector.execute_query("USE DATABASE test_db;")
    print("Database created.")

  # Create a new schema
  def create_schema(self):
    self.connector.execute_query("CREATE SCHEMA IF NOT EXISTS test_schema;")
    self.connector.execute_query("USE SCHEMA test_schema;")
    print("Schema created successfully.")
