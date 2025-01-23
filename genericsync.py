from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
from arcgis.gis import GIS
from arcgis.features import Feature
import re  # for sanitizing HTML content
from shapely.geometry import Point
from shapely import wkb

# Generalized database interaction class
class Database:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)

    def execute_query(self, query):
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
            return pd.DataFrame(result.fetchall(), columns=result.keys())

# Generalized data synchronization class
class DataSync:
    def __init__(self, gis, db_url):
        self.gis = gis
        self.db = Database(db_url)

    def fetch_ago_data(self, fs_id, layer_name):
        feature_service = self.gis.content.get(fs_id)
        layer = next((l for l in feature_service.layers + feature_service.tables if l.properties.name == layer_name), None)
        if not layer:
            raise ValueError(f"Layer '{layer_name}' not found in feature service {fs_id}")

        features = layer.query(where="1=1", out_fields="*", return_geometry=True).features
        data = pd.DataFrame([f.attributes for f in features])
        if layer.properties.type != 'Table':
            data['geometry'] = [f.geometry for f in features]
        return data

    def fetch_db_data(self, table_name):
        query = f"SELECT * FROM {table_name}"
        return self.db.execute_query(query)

    def prepare_data(self, data, spatial=False):
        data = data.copy()
        date_cols = [col for col in data.columns if 'date' in col or 'time' in col]
        for col in date_cols:
            data[col] = data[col].astype(str)

        data.fillna('', inplace=True)

        for col in data.columns:
            if 'note' in col:
                data[col] = data[col].apply(lambda x: re.sub(r'<[^>]*>', '', str(x)))

        if spatial and 'geometry' in data.columns:
            data['geometry'] = data['geometry'].apply(self.convert_geometry)

        return data

    def convert_geometry(self, geometry):
        point = Point(geometry['x'], geometry['y'])
        return wkb.dumps(point, hex=True)

    def sync_data(self, fs_id, ago_layer, db_table, key_columns):
        ago_data = self.fetch_ago_data(fs_id, ago_layer)
        db_data = self.fetch_db_data(db_table)

        merged = pd.merge(db_data, ago_data, how='outer', on=key_columns, indicator=True)
        records_to_add = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)
        # drop all the columns with an _y suffix
        records_to_add = records_to_add[records_to_add.columns.drop(list(records_to_add.filter(regex='_y')))]
        # rename all the columns with an _x suffix to remove the suffix
        records_to_add.columns = records_to_add.columns.str.replace('_x', '')
        # drop all the coumns not in the db_data
        records_to_add = records_to_add[db_data.columns]

        if not records_to_add.empty:
            self.add_to_ago(fs_id, ago_layer, records_to_add)
        else:
            print("No new records to sync.")

    def add_to_ago(self, fs_id, layer_name, data):
        feature_service = self.gis.content.get(fs_id)
        layer = next((l for l in feature_service.layers + feature_service.tables if l.properties.name == layer_name), None)

        features_to_add = [Feature(attributes=row.drop(['geometry'], errors='ignore').to_dict()) for _, row in data.iterrows()]
         # add the features to the feature service using a chink to avoid timeout
        results = []
        chunk_size = 100
        added = 0
        for i in range(0, len(features_to_add), chunk_size):
            chunk = features_to_add[i:i + chunk_size]
            
            result = layer.edit_features(adds=chunk)
            # print(result)
            results.append(result)
            added += len(chunk)
            # print percentage of progress with no decimal places
            # print(
            #     f"{str(added)} of {len(features_to_add)} records added to AGO - {round((i + chunk_size) / len(features_to_add) * 100)}%")
            # print(chunk)
        # result = layer.edit_features(adds=features)
        print(f"Added {len(features_to_add)} records to AGO.")
        return result

# Example usage
if __name__ == "__main__":
    import secret_stuff as s

    gis = GIS(s.AGOportal, s.AGOuser, s.AGOpass)
    db_url = f"postgresql://{s.db_user}:{s.db_pass}@{s.db_host}:{s.db_port}/{s.db_name}"

    data_sync = DataSync(gis, db_url)

    # Replace with your own feature service ID, layer name, database table, and key columns
    feature_service_id = "your_feature_service_id"
    layer_name = "your_layer_name"
    db_table_name = "your_db_table_name" #if it's in a schema, include the schema name as schema.table_name

    # Specify key columns for identifying records (e.g., ['unique_id'] or ['globalid'])
    key_columns = ["unique_id"]

    data_sync.sync_data(feature_service_id, layer_name, db_table_name, key_columns)
