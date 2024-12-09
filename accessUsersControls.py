from connector import SnowflakeConnector


class AccessControls:
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

  def access_privilege_fun(self):
    # Finding my current account, role and user
    self.connector.execute_query("""
      SELECT current_account(), current_role(), current_user();
    """)

  def create_new_user(self):
    self.connector.execute_query("""
      CREATE USER user01
        PASSWORD = 'Test@1234'
        COMMENT = 'this is a trial user with name user01'
        MUST_CHANGE_PASSWORD = FALSE;
    """)

  def create_new_user2(self):
    # self.connector.execute_query("""
    #   CREATE USER user02
    #     PASSWORD = 'Test@1234'
    #     DEFAULT_ROLE = "SYSADMIN"
    #     MUST_CHANGE_PASSWORD = FALSE;
    # """)
    self.connector.execute_query("""
      GRANT ROLE "SYSADMIN" TO USER user02;
    """)
    self.connector.execute_query("""
      GRANT ROLE "SECURITYADMIN" TO USER user02;
    """)
    self.connector.execute_query("""
      GRANT ROLE "USERADMIN" TO USER user02;
    """)

  def database_controls(self):
    self.connector.execute_query("""
      USE ROLE SYSADMIN;
    """)
    self.connector.execute_query("""
      SHOW users;
    """)
    self.connector.execute_query("""
      CREATE DATABASE my_db;
    """)
    self.connector.execute_query("""
      SHOW databases;
    """)
    self.connector.execute_query("""
      CREATE SCHEMA my_schema;
    """)
    self.connector.execute_query("""
      SHOW schemas;
    """)
    self.connector.execute_query("""
      CREATE TABLE my_tb1(name STRING);
    """)
    self.connector.execute_query("""
      SHOW tables;
    """)

  def show_roles(self):
    self.connector.execute_query("""
      SHOW roles;
    """)
    self.connector.execute_query("""
      SELECT current_role();
    """)
    self.connector.execute_query("""
      USE ROLE SECURITYADMIN;
    """)
    self.connector.execute_query("""
      SELECT current_role();
    """)

  def create_roles(self):
    self.connector.execute_query("""
      CREATE ROLE SALES_PM_ROLE comment='This is project manager for sales project';
    """)
    self.connector.execute_query("""
      GRANT role SALES_PM_ROLE to role SECURITYADMIN;
    """)
    self.connector.execute_query("""
      CREATE ROLE SALES_DEV_ROLE comment='This is development team';
    """)
    self.connector.execute_query("""
      GRANT role SALES_DEV_ROLE to role SALES_PM_ROLE;
    """)
    self.connector.execute_query("""
      CREATE ROLE SALES_ANALYST_ROLE comment='This is analyst team';
    """)
    self.connector.execute_query("""
      GRANT role SALES_ANALYST_ROLE to role SALES_PM_ROLE;
    """)
    self.connector.execute_query("""
      CREATE ROLE SALES_QA_ROLE comment='This is qa team';
    """)
    self.connector.execute_query("""
      GRANT role SALES_QA_ROLE to role SALES_ANALYST_ROLE;
    """)

  # create multiple users
  def create_users(self):
    self.connector.execute_query("""
      USE ROLE USERADMIN;
    """)
    self.connector.execute_query("""
      SELECT current_role();
    """)
    self.connector.execute_query("""
      CREATE USER pm_user password='Test@1234' comment='This is pm user' must_change_password=false;
    """)
    self.connector.execute_query("""
      CREATE USER al_user password='Test@1234' comment='This is analyst user' must_change_password=false;
    """)
    self.connector.execute_query("""
      CREATE USER qa_user password='Test@1234' comment='This is qa user' must_change_password=false;
    """)
    self.connector.execute_query("""
      CREATE USER dev_user01 password='Test@1234' comment='This is dev-01 user' must_change_password=false;
    """)
    self.connector.execute_query("""
      CREATE USER dev_user02 password='Test@1234' comment='This is dev-02 user' must_change_password=false;
    """)

  def grant_roles_to_users(self):
    self.connector.execute_query("""
      SHOW USERS;
    """)
    self.connector.execute_query("""
      USE ROLE SECURITYADMIN;
    """)
    self.connector.execute_query("""
      GRANT role SALES_PM_ROLE to user pm_user;
    """)
    self.connector.execute_query("""
      GRANT role SALES_ANALYST_ROLE to user al_user;
    """)
    self.connector.execute_query("""
      GRANT role SALES_QA_ROLE to user qa_user;
    """)
    self.connector.execute_query("""
      GRANT role SALES_DEV_ROLE to user dev_user01;
    """)
    self.connector.execute_query("""
      GRANT role SALES_DEV_ROLE to user dev_user02;
    """)

  def grant_privileges_to_role_users(self):
    self.connector.execute_query("""
      USE ROLE SECURITYADMIN;
    """)
    self.connector.execute_query("""
      SELECT current_account(), current_role(), current_user();
    """)
    # grant access for pm user
    # self.connector.execute_query("""
    #   GRANT CREATE warehouse on account to role SALES_PM_ROLE;
    # """)
    # self.connector.execute_query("""
    #   GRANT CREATE database on account to role SALES_PM_ROLE;
    # """)
    self.connector.execute_query("""
      USE DATABASE sales_db;
    """)
    # self.connector.execute_query("""USE DATABASE sales_schema""")
    # grant access for dev team
    self.connector.execute_query("""
      GRANT USAGE on database sales_db to role SALES_DEV_ROLE;
    """)
    self.connector.execute_query("""
      GRANT ALL PRIVILEGES ON SCHEMA sales_schema to role SALES_DEV_ROLE;
    """)
    self.connector.execute_query("""
      GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA sales_schema to role SALES_DEV_ROLE;
    """)
    self.connector.execute_query("""
      GRANT USAGE ON WAREHOUSE load_wh to role SALES_DEV_ROLE;
    """)
    self.connector.execute_query("""
      GRANT USAGE ON WAREHOUSE adhoc_wh to role SALES_DEV_ROLE;
    """)
    # grant access for QA
    self.connector.execute_query("""
      GRANT USAGE ON DATABASE sales_db to role SALES_QA_ROLE;
    """)
    self.connector.execute_query("""
      GRANT USAGE ON SCHEMA sales_schema to role SALES_QA_ROLE;
    """)
    self.connector.execute_query("""
      GRANT SELECT ON ALL TABLES IN SCHEMA sales_schema to role SALES_QA_ROLE;
    """)
    self.connector.execute_query("""
      GRANT USAGE ON WAREHOUSE adhoc_wh to role SALES_QA_ROLE;
    """)

  def create_db_role(self):
    self.connector.execute_query("""
      USE DATABASE my_db;
    """)
    self.connector.execute_query("""
      USE ROLE ACCOUNTADMIN;
    """)
    self.connector.execute_query("""
      USE DATABASE sales_db;
    """)
    self.connector.execute_query("""
      CREATE DATABASE ROLE my_db.my_db_role1;
    """)
    self.connector.execute_query("""
      SHOW DATABASE ROLES IN DATABASE my_db;
    """)
    self.connector.execute_query("""
      GRANT DATABASE ROLE my_db.my_db_role to role SYSADMIN;
    """)
    self.connector.execute_query("""
      REVOKE ROLE SALES_ANALYST_ROLE FROM ROLE SALES_PM_ROLE;
    """)
    self.connector.execute_query("""
      DROP DATABASE ROLE my_db.my_db_role;
    """)
    self.connector.execute_query("""
      SHOW DATABASE ROLES IN DATABASE my_db;
    """)
    self.connector.execute_query("""
      REVOKE SELECT ON ALL TABLES IN SCHEMA sales_schema from ROLE SALES_QA_ROLE;
    """)

  def grant_privilege_with_share(self):
    self.connector.execute_query("""
      CREATE SHARE test_share COMMENT ="Share SALES_S successfully created."
    """)
    self.connector.execute_query("""
      GRANT USAGE ON DATABASE my_db TO SHARE test_share;
    """)
    self.connector.execute_query("""
      REVOKE USAGE ON DATABASE my_db FROM SHARE test_share;
    """)
    self.connector.execute_query("""
      SHOW SHARES;
    """)
