
from rdflib import Graph, URIRef, RDF, Literal 
from .clean_str import remove_special_chars

def create_Graph(json_object:dict, base_url, my_graph:Graph):
    
    # create an internal id for the collections using an external counter
    # .strip is for removing eventually white space
    with open('collection_counter.txt', 'r', encoding='utf-8') as a:
        collection_counter = int(a.read().strip())

    # create an internal id for the manifest using an external counter
    with open('manifest_counter.txt', 'r', encoding='utf-8') as b:
        manifest_counter = int(b.read().strip())

    # create an internal id for the canvases using an external counter
    with open('canvas_counter.txt', 'r', encoding='utf-8') as c:
        canvas_counter = int(c.read().strip())


    # classes
    Collection = URIRef("https://github.com/n1kg0r/ds-project-dhdk/classes/Collection")
    Manifest = URIRef("https://github.com/n1kg0r/ds-project-dhdk/classes/Manifest")
    Canvas = URIRef("https://github.com/n1kg0r/ds-project-dhdk/classes/Canvas")

    # attributes related to classes
    label = URIRef("https://github.com/n1kg0r/ds-project-dhdk/attributes/label")

    # relations among classes
    items = URIRef("https://github.com/n1kg0r/ds-project-dhdk/relations/items")
    has_id = URIRef("http://purl.org/dc/elements/1.1/identifier")

    # create a variable for the id
    collection_id = json_object['id'] 

    # create an internal id with an external counter
    collection_counter += 1
    collection_IntId = json_object['type'] + f"_{collection_counter}"
    Coll_internalId = URIRef(base_url + collection_IntId)

    # create a list from the dictionary of label and catch the value of the key "none" (language) in a variable
    label_list = list(json_object['label'].values())  
    value_label = label_list[0][0]

    # remove the square brackets from the label value
    value_label = remove_special_chars(str(value_label))

    # create the graph with the triples
    my_graph.add((Coll_internalId, has_id, Literal(collection_id)))
    my_graph.add((Coll_internalId, RDF.type, Collection))
    my_graph.add((Coll_internalId, label, Literal(str(value_label))))

    
    # second step is entering the collection items list (enter the manifest) -> entering a list of dictionaries
    # here i take the id and I store it in a variable
    for manifest in json_object["items"]:
        manifest_id = manifest['id']

        # here i raise the counter for the manifest internal id
        manifest_counter += 1
        manifest_IntId = manifest['type'] + f"_{manifest_counter}" 
        Man_internalId = URIRef(base_url + manifest_IntId)

        #add the "has Item" to connect Collection to Manifest
        my_graph.add((Coll_internalId, items, Man_internalId))

        # create a list from the dictionary of label and catch the value of the key "none" (language) in a variable
        M_label_list = list(manifest['label'].values())  
        M_value_label = M_label_list[0][0]

        # remove the square brackets from the label value
        M_value_label = remove_special_chars(str(M_value_label))
        

        # create the graph with the triples
        my_graph.add((Man_internalId, has_id, Literal(manifest_id)))
        my_graph.add((Man_internalId, RDF.type, Manifest))
        my_graph.add((Man_internalId, label, Literal(str(M_value_label))))

        # third step is entering the manifest items list (enter the canvases) -> entering a list of dictionaries
        # here i take the id and I store it in a variable
        for canvas in manifest["items"]:
            canvas_id = canvas['id']

            # here i raise the counter for the manifest internal id
            canvas_counter += 1
            canvas_IntId = canvas['type'] + f"_{canvas_counter}" 
            Can_internalId = URIRef(base_url + canvas_IntId)

            #add the "has Item" to connect Collection to Manifest
            my_graph.add((Man_internalId, items, Can_internalId))

            # create a list from the dictionary of label and catch the value of the key "none" (language) in a variable
            C_label_list = list(canvas['label'].values())  
            C_value_label = C_label_list[0][0]

            # remove the square brackets from the label value
            C_value_label = remove_special_chars(str(C_value_label))


            # create the graph with the triples
            my_graph.add((Can_internalId, has_id, Literal(canvas_id)))
            my_graph.add((Can_internalId, RDF.type, Canvas))
            my_graph.add((Can_internalId, label, Literal(str(C_value_label))))

    #upload the counters text file
    with open('collection_counter.txt', 'w') as a:
        a.write(str(collection_counter))

    with open('manifest_counter.txt', 'w') as b:
        b.write(str(manifest_counter))

    with open('canvas_counter.txt', 'w') as c:
        c.write(str(canvas_counter))





    




