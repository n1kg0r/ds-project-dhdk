import requests

def clear_counter():
    with open('collection_counter.txt', 'w') as a:
        a.write('0')

    with open('manifest_counter.txt', 'w') as b:
        b.write('0')

    with open('canvas_counter.txt', 'w') as c:
        c.write('0')

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
clear_counter()