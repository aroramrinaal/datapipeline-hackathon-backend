import json
import psycopg2
import os

# RDS settings
RDS_HOST = "mydb.ctsqgmcogxc7.us-east-1.rds.amazonaws.com"
DB_USER = "username"
DB_PASSWORD = "password"
DB_NAME = "mydb"

def connect_to_rds():
    """Connect to the PostgreSQL RDS instance."""
    try:
        connection = psycopg2.connect(
            host=RDS_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        return connection
    except Exception as e:
        print(f"Error connecting to RDS: {e}")
        raise

def drop_table_if_exists(cursor, table_name):
    """Drop table if it exists."""
    try:
        cursor.execute(f"DROP TABLE IF EXISTS \"{table_name}\"")
        print(f"Table {table_name} dropped if it existed.")
    except Exception as e:
        print(f"Error dropping table {table_name}: {e}")
        raise

def create_dynamic_table(cursor, table_name, columns):
    """Dynamically create a table based on the column names and data types"""
    columns_sql = ", ".join([f'"{col}" TEXT' for col in columns])
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        "id" SERIAL PRIMARY KEY,
        {columns_sql}
    )
    """
    cursor.execute(create_table_sql)
    print(f"Table {table_name} created.")

def insert_dynamic_data(cursor, table_name, data):
    """Dynamically insert data into the specified table"""
    if isinstance(data, dict):
        data = [data]  # Convert single dict to a list for consistency
    
    # Iterate over each record (if it's a list)
    for record in data:
        columns = ", ".join([f'"{key}"' for key in record.keys()])
        values = ", ".join([f"'{str(value)}'" for value in record.values()])
        insert_sql = f"INSERT INTO \"{table_name}\" ({columns}) VALUES ({values})"
        cursor.execute(insert_sql)

def create_dataset_overview_table(cursor, dataset_overview):
    """Dynamically create and insert dataset overview table"""
    table_name = "dataset_overview"
    drop_table_if_exists(cursor, table_name)
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS \"{table_name}\" (
        "id" SERIAL PRIMARY KEY,
        "total_rows" INT,
        "total_columns" INT,
        "total_missing_values" INT,
        "total_unique_values" INT
    )
    """
    cursor.execute(create_table_sql)

    # Insert dataset overview
    insert_sql = f"""
    INSERT INTO \"{table_name}\" (\"total_rows\", \"total_columns\", \"total_missing_values\", \"total_unique_values\")
    VALUES (%s, %s, %s, %s)
    """
    cursor.execute(insert_sql, (
        dataset_overview['total_rows'],
        dataset_overview['total_columns'],
        dataset_overview['total_missing_values'],
        dataset_overview['total_unique_values']
    ))

def create_columns_metadata_table(cursor):
    """Dynamically create the columns metadata table"""
    table_name = "columns_metadata"
    drop_table_if_exists(cursor, table_name)
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS \"{table_name}\" (
        "id" SERIAL PRIMARY KEY,
        "column_name" VARCHAR(255),
        "data_type" VARCHAR(255),
        "missing_values" INT,
        "missing_percentage" FLOAT,
        "unique_values" INT,
        "unique_percentage" FLOAT,
        "numerical_summary_min" FLOAT,
        "numerical_summary_max" FLOAT,
        "numerical_summary_mean" FLOAT,
        "numerical_summary_median" FLOAT,
        "numerical_summary_std_dev" FLOAT,
        "high_missing_percentage_flag" BOOLEAN,
        "potential_outliers" TEXT
    )
    """
    cursor.execute(create_table_sql)

def insert_column_metadata(cursor, column_name, column_metadata):
    """Insert the column-level metadata into the database"""
    insert_sql = """
    INSERT INTO "columns_metadata" (
        "column_name",
        "data_type",
        "missing_values",
        "missing_percentage",
        "unique_values",
        "unique_percentage",
        "numerical_summary_min",
        "numerical_summary_max",
        "numerical_summary_mean",
        "numerical_summary_median",
        "numerical_summary_std_dev",
        "high_missing_percentage_flag",
        "potential_outliers"
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_sql, (
        column_name,
        column_metadata['data_type'],
        column_metadata['missing_values'],
        column_metadata['missing_percentage'],
        column_metadata['unique_values'],
        column_metadata['unique_percentage'],
        column_metadata['numerical_summary']['min'] if column_metadata['numerical_summary'] else None,
        column_metadata['numerical_summary']['max'] if column_metadata['numerical_summary'] else None,
        column_metadata['numerical_summary']['mean'] if column_metadata['numerical_summary'] else None,
        column_metadata['numerical_summary']['median'] if column_metadata['numerical_summary'] else None,
        column_metadata['numerical_summary']['std_dev'] if column_metadata['numerical_summary'] else None,
        column_metadata['flags']['high_missing_percentage'],
        json.dumps(column_metadata['flags']['potential_outliers'])  # Storing list as JSON string
    ))

def lambda_handler(event, context):
    try:
        # Parse the input JSON data
        json_data = event['json_data'][0]  # Example: {'json_data': {...}}

        # Extract dataset overview and column metadata
        dataset_overview = json_data['dataset_overview']
        columns_metadata = json_data['columns']

        # Connect to the PostgreSQL RDS instance
        connection = connect_to_rds()
        cursor = connection.cursor()

        # Create dataset overview table dynamically
        create_dataset_overview_table(cursor, dataset_overview)

        # Insert dataset overview data
        insert_dynamic_data(cursor, 'dataset_overview', [dataset_overview])

        # Create columns metadata table dynamically
        create_columns_metadata_table(cursor)

        # Insert column metadata data
        for column_name, column_metadata in columns_metadata.items():
            insert_column_metadata(cursor, column_name, column_metadata)

        # Commit changes
        connection.commit()

        # Close the connection
        cursor.close()
        connection.close()

        return {
            'statusCode': 200,
            'body': json.dumps("Data successfully inserted into PostgreSQL database.")
        }

    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {e}")
        }
