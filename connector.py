import pandas as pd
import snowflake.connector


class SnowflakeConnector:
    def __init__(self, user, password, account, warehouse, database):
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.connection = None

    def connect(self):
        try:
            self.connection = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
            )
            print("Connection established")
        except Exception as e:
            print(f"Error connecting to Snowflake: {e}")
            raise e

    def close(self):
        if self.connection:
            self.connection.close()
            print("Connection closed")

    def execute_query(self, query):
        if not self.connection:
            raise Exception("Connection not established. Call connect() first.")
        df=None
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            df = pd.DataFrame(results, columns=cursor.description)
        except Exception as e:
            print(f"Error executing query: {e}")
            # raise e
        finally:
            cursor.close()

        return df
