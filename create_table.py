import json
import os
import psycopg2
from dotenv import load_dotenv

# Constant for the possible geometry headers 
GEOMETRY_HEADERS = ("geom", "geometry", "wkt")

# Load variables from .env file
load_dotenv()

# Setup connection with PostgreSQL
def connect_to_pg():
    return psycopg2.connect(
        dbname=os.getenv("DATABASE"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
        host=os.getenv("HOST"),
        port=os.getenv("PORT")
    )

# Dynamically determine the corresponding PostgreSQL type 
# (could be more complex, this is just an example of how I would do it).
def decide_sql_type(header, value):
    if header.lower() in GEOMETRY_HEADERS:
        return "GEOMETRY(GEOMETRY, 3857)" 
    if isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "DOUBLE PRECISION"
    elif isinstance(value, dict):
        return "JSONB"
    else:
        return "TEXT"

# Create table using a JSON file
def create_table_from_json(json_file, table_name):
    # Open JSON file
    with open(json_file, 'r') as f:
        json_data = json.load(f)

    # Use the first entry in the JSON file as reference
    first_json_entry = json_data[0]
    headers = list(first_json_entry.keys())

    # In this list, we will store the column titles along with their type (the type of data that should be in the column)
    # The first column, ID, will be the primary key
    columns_with_types = []
    columns_with_types.append("id BIGSERIAL PRIMARY KEY")
    for header in headers:
        value = first_json_entry[header]
        sql_type = decide_sql_type(header, value)
        columns_with_types.append(f"{header} {sql_type}")

    # Query to make the table
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(columns_with_types)}
    );
    """

    # Make connection with PostgreSQL
    connection = connect_to_pg()
    cursor = connection.cursor()

    # Always recreate the table on each run (to handle possible changes in the structure of the JSON file)
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    cursor.execute(create_sql)

    # Check for spatial data. If present, use a spatial index (e.g. for efficient point lookup)
    if any(header.lower() in GEOMETRY_HEADERS for header in first_json_entry.keys()):
        spatial_column = next(header for header in first_json_entry.keys() 
                         if header.lower() in GEOMETRY_HEADERS)
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{table_name}_{spatial_column} 
            ON {table_name} USING GIST({spatial_column});
        """)
        
        # Commit and close connection with PostgreSQL
        connection.commit()
        cursor.close()
        connection.close()

        # Message to user
        print(f"Table {table_name} created successfully with spatial support ✅")
    else:
        print(f"Table {table_name} created successfully without spatial support ✅")

# Fills a previously created PostgreSQL table with data
def insert_data_from_json(json_file, table_name):
    # Open JSON file
    with open(json_file, 'r') as f:
        json_data = json.load(f)
       
    # Use the first entry in the JSON file as reference
    first_json_entry = json_data[0]
        
    # Declaration for placeholders (to prevent SQL injection in psycopg2) + column headers
    placeholders = []
    headers = []

    # Correctly handle placeholders for the spatial column
    for header in  first_json_entry.keys():
        if header.lower() in GEOMETRY_HEADERS:
            placeholders.append(f"ST_Transform(ST_GeomFromText(%s, 3857), 3857)")
        else:
            placeholders.append("%s")
        headers.append(header)
        
    # Query for INSERT Statement
    insert_sql = f"""
    INSERT INTO {table_name} ({', '.join(headers)})
    VALUES ({', '.join(placeholders)})
    """
    
    # Make connection with PostgreSQL
    connection = connect_to_pg()
    cursor = connection.cursor()

    # Add entries one by one using INSERT queries
    for entry in json_data:
        data = tuple(entry[header] for header in headers)
        cursor.execute(insert_sql, data)

    # Commit and close connection with PostgreSQL
    connection.commit()
    cursor.close()
    connection.close()

    # Message to user
    print(f"Table {table_name} was filled with data successfully ✅")
    


if __name__ == "__main__":
    create_table_from_json("assets/countries.json", "GEO_TABLE")
    insert_data_from_json("assets/countries.json", "GEO_TABLE")