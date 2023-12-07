import sqlite3
import json

def create_table(cursor):
    """
    Create a table in the SQLite database to store ward information.
    """
    cursor.execute('''
        CREATE VIRTUAL TABLE wards USING geopoly(id INT,name,assembly_constituency_id INT,assembly_constituency_name,parliamentary_constituency_id INT,parliamentary_constituency_name)
    ''')

def insert_ward(cursor, _shape,id_, name,assembly_constituency_id,assembly_constituency_name,parliamentary_constituency_id,parliamentary_constituency_name):
    """
    Insert ward information into the SQLite database.
    """
    cursor.execute('''
        INSERT INTO wards (_shape,id, name,assembly_constituency_id,assembly_constituency_name,parliamentary_constituency_id,parliamentary_constituency_name)
        VALUES (?, ?, ?,?,?,?,?)
    ''', (_shape, id_, name,assembly_constituency_id,assembly_constituency_name,parliamentary_constituency_id,parliamentary_constituency_name))

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

            # Iterate through each geometry polygon
            for geometry in feature['geometry']['coordinates']:
                for g in geometry:
                    try:
                        insert_ward(cursor, 
                            json.dumps(g), 
                            feature['properties']['id'],
                            feature['properties']['name_en'],
                            feature['properties']['assembly_constituency_id'],
                            feature['properties']['assembly_constituency_name_en'].split('-', 1)[-1].strip(),
                            feature['properties']['parliamentary_constituency_id'],
                            feature['properties']['parliamentary_constituency_name_en']
                        )
                        print("Inserted "+feature['properties']['name_en'])
                    except Exception as e:
                        print("Failed to insert "+feature['properties']['name_en'])
                        raise e
                

    # Commit and close the connection
    connection.commit()
    connection.close()

if __name__ == "__main__":
    # Specify the path to the GeoJSON file and SQLite database
    geojson_file_path = "bbmp-2023.geojson"
    database_path = "bbmp.db"

    # Process the GeoJSON file and insert data into SQLite database
    process_geojson_file(geojson_file_path, database_path)