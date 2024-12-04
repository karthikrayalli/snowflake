from db_query import DataBaseCreation
# from orders import OrdersTable;
from usersView import UsersViewTable;

def main():

    snowflake_params = {
        'user': 'user',
        'password': 'password',
        'account': 'account',
        'warehouse': 'warehouse',
        'database': 'database'
    }

    # Database and schema creation
    db_creation = DataBaseCreation(**snowflake_params)
    db_creation.create_database()
    db_creation.create_schema()

    # Orders Table
    # orders_table= OrdersTable(**snowflake_params)
    # orders_table.orders_data()
    # orders_table.insert_orders_data()
    # orders_table.upload_order_parquet_to_stage()
    # orders_table.noncluster_orders_data()


    # Users view table
    users_view_table=UsersViewTable(**snowflake_params)
    users_view_table.create_users_vw_table()
    # users_view_table.view_all_users_data()

if __name__ == "__main__":
    main()
