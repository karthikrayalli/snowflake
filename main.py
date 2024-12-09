from db_query import DataBaseCreation
# from usersView import UsersViewTable;
# from userMaterialised import MaterializedViewTable;
# from usersSecure import SecureViewTable;
# from rangeQueries import RangeQueries;
from orders import OrdersTable;
# from timeTravel import TimeTravel; 
from accessUsersControls import AccessControls
from newUsersAccessControls import NewUserAccessControls # user01
from user2AccessControls import User2AccessControls # user02
from roleHirarcyUser import RoleHierarchyUser
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

    # Users view table
    # users_view_table=UsersViewTable(**snowflake_params)
    # users_view_table.create_users_vw_table()
    # users_view_table.view_all_users_data()

    # Users Materialized view table
    # users_materialized_table=MaterializedViewTable(**snowflake_params)
    # users_materialized_table.materialized_vw_users()

    # Users secure view table
    # users_secure_table=SecureViewTable(**snowflake_params)
    # users_secure_table.secure_vw_users()
    # users_secure_table.show_views()

    # Orders Table
    orders_table= OrdersTable(**snowflake_params)
    orders_table.orders_data()
    # orders_table.insert_orders_data()
    # orders_table.upload_order_parquet_to_stage()
    # orders_table.noncluster_orders_data()

    # Range Queries
    # range_sample_queries=RangeQueries(**snowflake_params)
    # range_sample_queries.combine_all_functions()

    # Time travel
    # time_travel_retention = TimeTravel(**snowflake_params)
    # time_travel_retention.create_time_travel_table()
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

    # access controls
    # user_access_control = AccessControls(**snowflake_params)
    # user_access_control.access_privilege_fun()
    # user_access_control.create_new_user()
    # user_access_control.create_new_user2()
    # user_access_control.database_controls()
    # user_access_control.show_roles()
    # user_access_control.create_roles()
    # user_access_control.create_users()
    # user_access_control.grant_roles_to_users()
    # user_access_control.grant_privileges_to_role_users()
    # user_access_control.create_db_role()
    # user_access_control.grant_privilege_with_share()

    # new user access controls
    # new_user_access=NewUserAccessControls(
    #     user = "user01",
    #     password = "Test@1234",
    #     account = "account",
    #     warehouse = "COMPUTE_WH",
    #     database=""
    # )
    # new_user_access.new_user_access_controls()

    # user2 access controls
    # new_user2_access = User2AccessControls(
    #     user="user02",
    #     password="Test@1234",
    #     account="account",
    #     warehouse="COMPUTE_WH",
    #     database=""
    # )
    # new_user2_access.user2_db_controls()
    # new_user2_access.new_user_access_controls()

    # Role Hierarchy user
    # role_based_user = RoleHierarchyUser(
    #     user="pm_user",
    #     password="Test@1234",
    #     account="account",
    #     warehouse="COMPUTE_WH",
    #     database=""
    # )


    # role_based_user.role_based_user_fun()
if __name__ == "__main__":
    main()
