from db_query import DataBaseCreation
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

if __name__ == "__main__":
    main()
