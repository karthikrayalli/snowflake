from connector import SnowflakeConnector

class UsersViewTable:
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

  def create_users_vw_table(self):
    self.connector.execute_query("""
      CREATE VIEW IF NOT EXISTS vw_users AS
      SELECT id, user_name, display_name
      FROM users
      WHERE country='Canada'
    """)

  def view_all_users_data(self):
    self.connector.execute_query("SELECT * FROM vw_users")
