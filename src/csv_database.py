from pandas import read_csv, Series, DataFrame
from sqlite3 import connect


annotations = read_csv("../data/annotations.csv", 
                        keep_default_na=False,
                        dtype={
                            "id": "string",
                            "body": "string",
                            "target": "string",
                            "motivation": "string"
                        })

image = annotations[["body"]]
image = image.rename(columns={"body": "id"})


entityWithMetadata= read_csv("../data/metadata.csv", 
                        keep_default_na=False,
                        dtype={
                            "id": "string",
                            "title": "string",
                            "creator": "string"
                        })


with connect("../data/annotation.db") as con:
    annotations.to_sql("Annotation", con, if_exists="replace", index=False)
    image.to_sql("Image", con, if_exists="replace", index=False)
    entityWithMetadata.to_sql("Entity", con, if_exists="replace", index=False)
