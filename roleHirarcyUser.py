from connector import SnowflakeConnector

class RoleHierarchyUser:
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

  def role_based_user_fun(self):
    # self.connector.execute_query("SELECT current_account(), current_role(), current_user();")
    self.connector.execute_query("""
      USE ROLE SALES_PM_ROLE;
    """)
    # self.connector.execute_query("""
    #   CREATE DATABASE pm_db;
    # """)
    self.connector.execute_query("""
      CREATE warehouse load_wh
      WITH
      warehouse_size = 'xlarge'
      warehouse_type = 'standard'
      auto_suspend = 300
      auto_resume = true
      min_cluster_count = 1
      max_cluster_count = 1
      scaling_policy = 'standard';
    """)
    self.connector.execute_query("""
      CREATE warehouse adhoc_wh
      WITH
      warehouse_size = 'xsmall'
      warehouse_type = 'standard'
      auto_suspend = 300
      auto_resume = true
      min_cluster_count = 1
      max_cluster_count = 1
      scaling_policy = 'standard';
    """)
    self.connector.execute_query("""
      SHOW warehouses;
    """)
    self.connector.execute_query("""
      CREATE DATABASE sales_db;
    """)
    self.connector.execute_query("""
      CREATE SCHEMA sales_schema;
    """)
    self.connector.execute_query("""
      CREATE TABLE order_tables (name STRING);
    """)
    self.connector.execute_query("""
      INSERT INTO  order_tables (name) VALUES ('by role SALES_PM_ROLE');
    """)
    self.connector.execute_query("""
      SELECT * FROM order_tables;
    """)