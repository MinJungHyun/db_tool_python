import pymysql
import json

# Get the database configuration from the JSON file
def get_json_data(json_file):
    with open(json_file, 'r') as config_file:
        config = json.load(config_file)
    return config

# Copy the schema and data from the source table to the destination table
def copy_schema(config_source, config_destination, schema_source, schema_destination, query_source=None, delete=False):
    # Connect to the source database
    source_db = pymysql.connect(**config_source)
    source_cursor = source_db.cursor()

    # Connect to the destination database
    destination_db = pymysql.connect(**config_destination)
    destination_cursor = destination_db.cursor()

    # Get the column names and order of the source table
    source_cursor.execute(f"SELECT * FROM {schema_source} LIMIT 0")
    source_columns = [column[0] for column in source_cursor.description]

    # Get the column names and order of the destination table
    destination_cursor.execute(f"SELECT * FROM {schema_destination} LIMIT 0")
    destination_columns = [column[0] for column in destination_cursor.description]

    # Get the common columns between source and destination tables
    common_columns = set(source_columns) & set(destination_columns)


    # Create the destination table with the same structure as the source table
    # column_definitions = []
    # for column_name in source_columns:
    #     if column_name in common_columns:
    #         column_type = next(column[1] for column in source_cursor.description if column[0] == column_name)
    #         column_definitions.append(f"{column_name} {column_type}")
    # destination_cursor.execute(f"CREATE TABLE IF NOT EXISTS {schema_destination} ({','.join(column_definitions)})")

    # Delete existing data from the destination table if delete is True
    if delete:
        destination_cursor.execute(f"DELETE FROM `{schema_destination}`")

    # Copy the data to the destination table
    if query_source is None:
        source_cursor.execute(f"SELECT {','.join(destination_columns)} FROM {schema_source}")
    else:
        source_cursor.execute(query_source)
    rows = source_cursor.fetchall()
    data = []
    for row in rows:
        row_dict = {}
        for i in range(len(source_cursor.description)):
            row_dict[source_cursor.description[i][0]] = row[i]
        data.append(row_dict)

    destination_column_name = []
    for i in range(len(destination_cursor.description)):
        destination_column_name.append(destination_cursor.description[i][0])

    ## data change tuple     
    newdata = []
    for row in data:
            
        row_dict = []
        for i in range(len(destination_column_name)):
            row_dict.append(row[destination_column_name[i]])
        newdata.append(row_dict)

    destination_cursor.executemany(f"INSERT INTO {schema_destination} ({','.join(destination_columns)}) VALUES ({','.join(['%s']*len(destination_columns))})", newdata)
    

    # Commit the changes and close the cursors and connections
    destination_db.commit()

    source_cursor.close()
    source_db.close()

    destination_cursor.close()
    destination_db.close()
