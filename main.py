from db_query import DataBaseCreation
# from orders import OrdersTable;
# from usersView import UsersViewTable;
from timeTravel import TimeTravel;
# from roleHirarcyUser import RoleHierarchyUser

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

    # Role Hierarchy user
    # role_based_user = RoleHierarchyUser(
    #     user="pm_user",
    #     password="Test@1234",
    #     account="hgggsxk-vo71560",
    #     warehouse="COMPUTE_WH",
    #     database=""
    # )
    # role_based_user.role_based_user_fun()

    # Orders Table
    # orders_table= OrdersTable(**snowflake_params)
    # orders_table.orders_data()
    # orders_table.insert_orders_data()
    # orders_table.upload_order_parquet_to_stage()
    # orders_table.noncluster_orders_data()


    # Users view table
    # users_view_table=UsersViewTable(**snowflake_params)
    # users_view_table.create_users_vw_table()
    # users_view_table.view_all_users_data()

    # Time travel
    time_travel_retention = TimeTravel(**snowflake_params)
    time_travel_retention.create_time_travel_table()
    # time_travel_retention.time_travel_update()
    # time_travel_retention.time_travel_at_and_before()
    # time_travel_retention.create_transient_table1()
    # time_travel_retention.create_transient_table2()
    # time_travel_retention.create_tmp_table()
    # time_travel_retention.create_updrop_table()
    # time_travel_retention.show_tables()
    # time_travel_retention.orders_data()
    # time_travel_retention.undrop_fun()
    # time_travel_retention.time_travel_clone_fun()
    # time_travel_retention.clone_fun()
    # time_travel_retention.create_table_with_clone()
    # time_travel_retention.insert_data_into_table1()
    # time_travel_retention.insert_data_into_tt_before_table()

if __name__ == "__main__":
    main()
