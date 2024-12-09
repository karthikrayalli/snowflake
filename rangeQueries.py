from connector import SnowflakeConnector


class RangeQueries:
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
    # self.connector.execute_query("USE DATABASE test_db;")
    # Use schema
    # self.connector.execute_query("USE SCHEMA test_schema;")

    self.connector.execute_query("USE DATABASE test_db_karthik_orders;")
    self.connector.execute_query("USE SCHEMA test_schema_karthik_orders;")

  def range_between(self):
    self.connector.execute_query("""
      SELECT * FROM orders
      WHERE o_totalprice BETWEEN 5000 AND 50000;
    """)

  def date_range(self):
    self.connector.execute_query("""
      SELECT * FROM orders
      WHERE o_orderdate BETWEEN '1992-05-09' AND '1995-06-02';
    """)

  def range_operators(self):
    self.connector.execute_query("""
      SELECT * FROM orders
      WHERE o_totalprice >= 5000 AND o_totalprice <= 100000;
    """)

  def range_operators_date(self):
    self.connector.execute_query("""
      SELECT * FROM orders
      WHERE o_orderdate > '1992-05-09' and o_orderdate < '1992-09-20';
    """)

  def query_with_in(self):
    self.connector.execute_query("""
      SELECT * FROM orders
      WHERE o_orderpriority IN('5-LOW','3-MEDIUM');
    """)

  def query_with_case(self):
    self.connector.execute_query("""
      SELECT o_orderkey,
      CASE
        WHEN o_totalprice BETWEEN 5000 AND 50000 THEN 'Low Price'
        WHEN o_totalprice BETWEEN 60000 AND 100000 THEN 'Medium Price'
        ELSE 'High Price'
      END AS price_renge
      FROM orders;
    """)

#   def query_with_joins(self):
#     self.connector.execute_query("""
#       SELECT users_sample_list.id,users_sample_list.displayname, user_achievements.totalgold
#       FROM users_sample_list
#       JOIN user_achievements
#         ON users_sample_list.id = user_achievements.userid;
#     """)

#   def range_function_query(self):
#     self.connector.execute_query("""
#       SELECT id, firstname,
#         NTILE(4) OVER (ORDER BY salary) AS salary_quartile
#       FROM customers;
#     """)

#   def range_union(self):
#     self.connector.execute_query("""
#       SELECT * FROM customers WHERE salary BETWEEN 50000 AND 60000
#       UNION
#       SELECT * FROM customers WHERE salary BETWEEN 80000 AND 100000;
#     """)

#   def comparisons_query(self):
#     self.connector.execute_query("""
#       SELECT firstname, lastname,
#        LAG(salary, 1) OVER (ORDER BY salary) AS previous_salary,
#        LEAD(salary, 1) OVER (ORDER BY salary) AS next_salary
#       FROM customers;
#     """)

  def combine_all_functions(self):
    self.range_between()
    self.date_range()
    self.range_operators()
    self.range_operators_date()
    self.query_with_in()
    self.query_with_case()
    # self.query_with_joins()
    # self.range_function_query()
    # self.range_union()
    # self.comparisons_query()
