from pandas import read_csv, Series

indentifiableEntityID = read_csv("annotations.csv", 
                    keep_default_na=False,
                    dtype={
                        "id": "string",
                        "body": "string",
                        "target": "string",    
                        "motivation": "string"    
                    } )

# This will create a new data frame starting from 'metadata' one,
# and it will include only the column "id"
ie_ids = indentifiableEntityID[["target"]]

# Generate a list of internal identifiers for the metadata
ie_internal_id = []
for idx, row in ie_ids.iterrows():
    ie_internal_id.append("IdentifiableEntity-" + str(idx))

# Add the list of metadata internal identifiers as a new column
# of the data frame via the class 'Series'
ie_ids.insert(0, "metadataId", Series(ie_internal_id, dtype="string"))

# Show the new data frame on screen
#print(ie_ids)

annotation = indentifiableEntityID

print(annotation)

image = indentifiableEntityID[["body"]]

print(image)






