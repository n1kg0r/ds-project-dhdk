from pandas import read_csv, Series, DataFrame
from sqlite3 import connect

# maybe replace path with: https://github.com/n1kg0r/ds-project-dhdk/tree/main/data/annotations.csv

annotations = read_csv("C:/Users/NIKI/Desktop/Data - Peroni/Progetto/annotations.csv", 
                        keep_default_na=False,
                        dtype={
                            "id": "string",
                            "body": "string",
                            "target": "string",
                            "motivation": "string"
                        })

image = annotations[["body"]]
image = image.rename(columns={"body": "id"})

# maybe replace path with: https://github.com/n1kg0r/ds-project-dhdk/tree/main/data/metadata.csv

entityWithMetadata= read_csv("C:/Users/NIKI/Desktop/Data - Peroni/Progetto/metadata.csv", 
                        keep_default_na=False,
                        dtype={
                            "id": "string",
                            "title": "string",
                            "creator": "string"
                        })

# maybe replace path with: https://github.com/n1kg0r/ds-project-dhdk/tree/main/data/annotation.db

with connect("C:/Users/NIKI/Desktop/Data - Peroni/Progetto/annotation.db") as con:
    annotations.to_sql("Annotation", con, if_exists="replace", index=False)
    image.to_sql("Image", con, if_exists="replace", index=False)
    entityWithMetadata.to_sql("Entity", con, if_exists="replace", index=False)
