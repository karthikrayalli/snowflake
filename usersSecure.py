from connector import SnowflakeConnector

class SecureViewTable:
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
    # Use database
    self.connector.execute_query("USE DATABASE test_db;")
    # Use schema
    self.connector.execute_query("USE SCHEMA test_schema;")

  def secure_vw_users(self):
    self.connector.execute_query("""CREATE SECURE VIEW vw_secure_users AS
    SELECT id, user_name, display_name
    FROM users
    WHERE country='Australia';
    """)
    self.connector.execute_query("SELECT * FROM vw_secure_users;")

  def show_views(self):
    self.connector.execute_query("SHOW VIEWS;")
