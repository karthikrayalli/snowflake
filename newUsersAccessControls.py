from connector import SnowflakeConnector

class NewUserAccessControls:
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
    # self.connector.execute_query("SELECT current_role();")
    # self.connector.execute_query("SHOW GRANTS TO role public;")
    # self.connector.execute_query("CREATE DATABASE my_db;") # for role public user don't have privileges to create db
    self.connector.execute_query("desc user user01;")
    self.connector.execute_query("show grants to user user01;")
    self.connector.execute_query("show grants on user user01;")
