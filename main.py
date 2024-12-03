from db_query import DataBaseCreation
from orders import OrdersTable;

def main():

    snowflake_params = {
        'user': 'KARTHIKUDUPA',
        'password': 'Dev@arokee2024',
        'account': 'kvelixu-hh77725',
        'warehouse': 'COMPUTE_WH',
        'database': 'test_db_karthik_orders'
    }

    # Database and schema creation
    db_creation = DataBaseCreation(**snowflake_params)
    db_creation.create_database()
    db_creation.create_schema()

    # Orders Table
    orders_table= OrdersTable(**snowflake_params)
    orders_table.orders_data()
    # orders_table.insert_orders_data()
    # orders_table.upload_order_parquet_to_stage()
    # orders_table.noncluster_orders_data()

if __name__ == "__main__":
    main()
