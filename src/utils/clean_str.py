import requests

def remove_special_chars(s: str) -> str:
    if '\"' in s:
        return s.replace('\"', '\\\"')
    elif '"' in s:
        return s.replace('"', '\\\"')
    else:
        return s
    
def clear_blazegraph_database(db_path):
    
    url = f"{db_path}/sparql"
    query = "DELETE WHERE { ?s ?p ?o }"

    response = requests.post(url, data=query, headers={"Content-Type": "application/sparql-update"})

    if response.status_code == 200:
        print("Database cleared successfully.")
    else:
        print("Failed to clear the database.")

# RUN
blazegraph_path = "http://127.0.0.1:9999/blazegraph"
clear_blazegraph_database(blazegraph_path)
