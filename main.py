import sqlite3
import json

def create_table(cursor):
    """
    Create a table in the SQLite database to store ward information.
    """
    cursor.execute('''
        CREATE VIRTUAL TABLE wards USING geopoly(No,Name)
    ''')

def insert_ward(cursor, _shape, KGISWardNo, KGISWardName):
    """
    Insert ward information into the SQLite database.
    """
    cursor.execute('''
        INSERT INTO wards (_shape, No, Name)
        VALUES (?, ?, ?)
    ''', (_shape, KGISWardNo, KGISWardName))

def process_geojson_file(file_path, database_path):
    """
    Process the GeoJSON file and insert data into SQLite database.
    """
    # Open SQLite database
    connection = sqlite3.connect(database_path)
    cursor = connection.cursor()

    # Create the wards table
    create_table(cursor)

    # Read GeoJSON file
    with open(file_path, 'r') as geojson_file:
        data = json.load(geojson_file)

        # Iterate through each feature
        for feature in data['features']:
            # Extract KGISWardNo and KGISWardName
            KGISWardNo = feature['properties']['KGISWardNo']
            KGISWardName = feature['properties']['KGISWardName']

            # Iterate through each geometry polygon
            for geometry in feature['geometry']['coordinates']:

                # Insert data into the SQLite database
                insert_ward(cursor, json.dumps(geometry), KGISWardNo, KGISWardName)

    # Commit and close the connection
    connection.commit()
    connection.close()

if __name__ == "__main__":
    # Specify the path to the GeoJSON file and SQLite database
    geojson_file_path = "BBMP.geojson"
    database_path = "bbmp.db"

    # Process the GeoJSON file and insert data into SQLite database
    process_geojson_file(geojson_file_path, database_path)