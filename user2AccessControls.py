from connector import SnowflakeConnector

class User2AccessControls:
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

  def new_user_access_controls(self):
    self.connector.execute_query("desc user user02;")
    self.connector.execute_query("show grants to user user02;")
    self.connector.execute_query("show grants on user user02;")

  def user2_db_controls(self):
    self.connector.execute_query("""
      SHOW databases;
    """)
    self.connector.execute_query("""
      USE database my_db;
    """)
    self.connector.execute_query("""
      SHOW schemas;
    """)