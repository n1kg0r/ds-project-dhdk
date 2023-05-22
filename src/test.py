import unittest
from os import sep
from impl import AnnotationProcessor, MetadataProcessor, RelationalQueryProcessor
from impl import CollectionProcessor, TriplestoreQueryProcessor
from impl import GenericQueryProcessor, QueryProcessor,Processor
from pandas import DataFrame
from impl import IdentifiableEntity, EntityWithMetadata, Canvas, Collection, Image, Annotation, Manifest

annotations = "data" + sep + "annotations.csv"
collection = "data" + sep + "collection-1.json"
metadata = "data" + sep + "metadata.csv"
relational = "." + sep + "relational.db"
graph = "http://192.168.1.52:9999/blazegraph/"

ANNOTATIONS_CSV_PATH = "data/annotations.csv"
METADATA_CSV_PATH = "data/metadata.csv"
RELATIONAL_DB_PATH = 'relational.db'
GRAPH_DB_URL = "http://127.0.0.1:9999/blazegraph/sparql"
COLLECTION_ONE_PATH = "data/collection-1.json"
COLLECTION_TWO_PATH = "data/collection-2.json"



rel_qp = RelationalQueryProcessor()
rel_qp.setDbPathOrUrl('./data/test.db')

grp_qp = TriplestoreQueryProcessor()
grp_qp.setDbPathOrUrl('http://127.0.0.1:9999/blazegraph/sparql')

generic = GenericQueryProcessor()
generic.addQueryProcessor(rel_qp)
generic.addQueryProcessor(grp_qp)


class TestProjectBasic(unittest.TestCase):
    def init_processors_basic_case(self):
        rel_qp = RelationalQueryProcessor()
        rel_qp.setDbPathOrUrl(RELATIONAL_DB_PATH)

        grp_qp = TriplestoreQueryProcessor()
        print(grp_qp.setDbPathOrUrl('http://127.0.0.1:9999/blazegraph/sparql'))

        generic = GenericQueryProcessor()
        generic.addQueryProcessor(rel_qp)
        generic.addQueryProcessor(grp_qp)


    def populate_databases(self):
        ann_dp = AnnotationProcessor()
        ann_dp.setDbPathOrUrl(RELATIONAL_DB_PATH)
        ann_dp.uploadData(ANNOTATIONS_CSV_PATH)
        met_dp = MetadataProcessor()
        met_dp.setDbPathOrUrl(RELATIONAL_DB_PATH)
        met_dp.uploadData(METADATA_CSV_PATH)
        col_dp = CollectionProcessor()
        col_dp.setDbPathOrUrl(GRAPH_DB_URL)
        col_dp.uploadData(COLLECTION_ONE_PATH)
        col_dp.uploadData(COLLECTION_TWO_PATH)

    def test_get_all_annotations(self):
        get_all_annotations_res = generic.getAllAnnotations()
        print(len(get_all_annotations_res))
        for i in range(min(5, len(get_all_annotations_res))):
            print()
            print(get_all_annotations_res[i].id)
            print(get_all_annotations_res[i].motivation)
            print(get_all_annotations_res[i].target.id)
            print(get_all_annotations_res[i].body.id)
            print()

    def test_get_all_canvases(self):
        get_all_canvas_res = generic.getAllCanvas()
        print(len(get_all_canvas_res))

        # uncomment for sample tests
        for i in range(min(5, len(get_all_canvas_res))):
            print()
            print(get_all_canvas_res[i].id)
            print(get_all_canvas_res[i].label)
            print(get_all_canvas_res[i].title)
            print(get_all_canvas_res[i].creators)
            print()
        for canvas in get_all_canvas_res:
            if canvas.title and len(canvas.title):
                print()
                print(canvas.id)
                print(canvas.label)
                print(canvas.title)
                print(canvas.creators)
                print(type(canvas.creators))

    def test_generic_add_qp_clean_qp(self):
        self.assertEqual(self.generic.queryProcessors, [])

        self.generic.addQueryProcessor(self.rel_qp)
        self.addCleanupgeneric.addQueryProcessor(self.grp_qp)
        print(self.generic.queryProcessors)

        self.generic.cleanQueryProcessors()
        print(self.generic.queryProcessors)

        self.generic.addQueryProcessor(self.rel_qp)
        self.generic.addQueryProcessor(self.grp_qp)
        print(self.generic.queryProcessors)


# tp = TestProjectBasic()
# # tp.test_get_all_annotations()
# tp.test_get_all_canvases()


ie = IdentifiableEntity('')
# print(ie.getId())
image = Image('img')
# print(image.getId())
a = Annotation('0', '0', ie, image)

ewe = EntityWithMetadata('ewe', 'label', '', list())
# print(ewe.label)

p = Processor()
p.getDbPathOrUrl()
# print(p.setDbPathOrUrl('.db'))
# print(Processor.setDbPathOrUrl(p, 'http://192.168.1.52:9999/blazegraph/'))
# print(Processor.setDbPathOrUrl(p, 'haha hehe '))

qp = QueryProcessor()
qp.setDbPathOrUrl("./data/test.db")
#print(qp.getEntityById('https://dl.ficlit.unibo.it/iiif/2/28429/annotation/p0001-image'))
#print(qp.getEntityById('haha'))
qp.setDbPathOrUrl(GRAPH_DB_URL)
#print(qp.getEntityById('https://dl.ficlit.unibo.it/iiif/2/28429/annotation/p0001-image'))
# print(qp.getEntityById('https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1'))

r = RelationalQueryProcessor()
print(r.setDbPathOrUrl('relational.db'))
#print(r.getAllAnnotations())
#print(r.getAnnotationsWithBody('https://dl.ficlit.unibo.it/iiif/2/45498/full/699,800/0/default.jpg'))
#print(r.getAnnotationsWithBodyAndTarget('https://dl.ficlit.unibo.it/iiif/2/45498/full/699,800/0/default.jpg','HAHA' ))
#print(r.getAnnotationsWithTarget('https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1'))
#print(r.getEntitiesWithCreator('Doe, John'))
# print(r.getEntitiesWithTitle('Il Canzoniere'))
#print(r.getEntities())

t = TriplestoreQueryProcessor()
print(t.setDbPathOrUrl(GRAPH_DB_URL))
#print(t.getAllCanvases())
#print(t.getAllCollections())
#print(t.getAllManifests())
#print(t.getCanvasesInCollection('https://dl.ficlit.unibo.it/iiif/28429/collection'))
#print(t.getCanvasesInManifest('https://dl.ficlit.unibo.it/iiif/2/28429/manifest'))
#print(t.getManifestsInCollection('https://dl.ficlit.unibo.it/iiif/28429/collection'))
#print(t.getEntitiesWithLabel("Raimondi, Giuseppe. Quaderno manoscritto, \"Caserma Scalo : 1930-1968\""))
#print(t.getEntitiesWithCanvas('https://dl.ficlit.unibo.it/iiif/2/19428/canvas/p3'))
# print(t.getEntitiesWithId('https://dl.ficlit.unibo.it/iiif/2/19428/canvas/p3'))
#print(t.getAllEntities())

#a = AnnotationProcessor()
#print(a.setDbPathOrUrl(RELATIONAL_DB_PATH))
#print(a.uploadData(ANNOTATIONS_CSV_PATH))

#m = MetadataProcessor()
#print(m.setDbPathOrUrl(RELATIONAL_DB_PATH))
# print(m.uploadData(METADATA_CSV_PATH))

g = GenericQueryProcessor()
# print(g.addQueryProcessor(t))
# print(g.addQueryProcessor(r))
# print(g.queryProcessors)
# print(g.cleanQueryProcessors())
# print(g.queryProcessors)

print(g.addQueryProcessor(t))
print(g.addQueryProcessor(r))
#print(g.getAllAnnotations())



# get_all_annotations_res = g.getAllAnnotations()
# print(len(get_all_annotations_res))
# for i in range(min(5, len(get_all_annotations_res))):
#     print(vars(get_all_annotations_res[i]))


# get_all_annotations_res = g.getAllCanvas()
# print(len(get_all_annotations_res))
# for i in range(min(5, len(get_all_annotations_res))):
#     print(vars(get_all_annotations_res[i]))


# get_all_annotations_res = g.getAllCollections()
# print(len(get_all_annotations_res))
# for i in range(min(5, len(get_all_annotations_res))):
#     print(vars(get_all_annotations_res[i]))

# get_all_annotations_res = g.getAllManifests()
# print(len(get_all_annotations_res))
# for i in range(min(5, len(get_all_annotations_res))):
#     print(vars(get_all_annotations_res[i]))

# get_all_annotations_res = g.getAllImages()
# print(len(get_all_annotations_res))
# for i in range(min(5, len(get_all_annotations_res))):
#     print(vars(get_all_annotations_res[i]))

# get_all_annotations_res = g.getAnnotationsToCanvas('https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1')
# print(len(get_all_annotations_res))
# for i in range(min(5, len(get_all_annotations_res))):
#     print(vars(get_all_annotations_res[i]))

# get_all_annotations_res = g.getAnnotationsToManifest('https://dl.ficlit.unibo.it/iiif/2/28429/manifest')
# print(len(get_all_annotations_res))
# for i in range(min(5, len(get_all_annotations_res))):
#     print(vars(get_all_annotations_res[i]))

# get_all_annotations_res = g.getAnnotationsWithBody('"https://dl.ficlit.unibo.it/iiif/2/45498/full/699,800/0/default.jpg"')
# print(len(get_all_annotations_res))
# for i in range(min(5, len(get_all_annotations_res))):
#     print(vars(get_all_annotations_res[i]))

# get_all_annotations_res = g.getAnnotationsWithBodyAndTarget('https://dl.ficlit.unibo.it/iiif/2/45498/full/699,800/0/default.jpg', 'https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1')
# print(len(get_all_annotations_res))
# for i in range(min(5, len(get_all_annotations_res))):
#     print(vars(get_all_annotations_res[i]))


# get_all_annotations_res = g.getAnnotationsWithTarget('https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1')
# print(len(get_all_annotations_res))
# for i in range(min(5, len(get_all_annotations_res))):
#     print(vars(get_all_annotations_res[i]))

# get_all_annotations_res = g.getCanvasesInCollection('https://dl.ficlit.unibo.it/iiif/28429/collection')
# print(len(get_all_annotations_res))
# for i in range(min(5, len(get_all_annotations_res))):
#     print(vars(get_all_annotations_res[i]))

# get_all_annotations_res = g.getCanvasesInManifest('https://dl.ficlit.unibo.it/iiif/2/28429/manifest')
# print(len(get_all_annotations_res))
# for i in range(min(5, len(get_all_annotations_res))):
#     print(vars(get_all_annotations_res[i]))

# get_all_annotations_res = g.getEntityById('https://dl.ficlit.unibo.it/iiif/2/19428/manifest')
# print(vars(get_all_annotations_res))
# get_all_annotations_res = g.getEntityById('https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1')
# print(vars(get_all_annotations_res))

# get_all_annotations_res = g.getEntitiesWithCreator(' Doe, Jane')
# print(len(get_all_annotations_res))
# for i in range(min(5, len(get_all_annotations_res))):
#     print(vars(get_all_annotations_res[i]))


# get_all_annotations_res = g.getEntitiesWithLabel("Raimondi, Giuseppe. Quaderno manoscritto, \"Caserma Scalo : 1930-1968\"")
# print(len(get_all_annotations_res))
# for i in range(min(5, len(get_all_annotations_res))):
#     print(vars(get_all_annotations_res[i]))

# get_all_annotations_res = g.getEntitiesWithTitle("Dante Alighieri: Opere")
# print(len(get_all_annotations_res))
# for i in range(min(5, len(get_all_annotations_res))):
#     print(vars(get_all_annotations_res[i]))

# get_all_annotations_res = g.getImagesAnnotatingCanvas("https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1")
# print(len(get_all_annotations_res))
# for i in range(min(5, len(get_all_annotations_res))):
#     print(vars(get_all_annotations_res[i]))

# get_all_annotations_res = g.getManifestsInCollection("https://dl.ficlit.unibo.it/iiif/28429/collection")
# print(len(get_all_annotations_res))
# for i in range(min(5, len(get_all_annotations_res))):
#     print(vars(get_all_annotations_res[i]))


            # print()
            # print(get_all_annotations_res[i].id)
            # print(get_all_annotations_res[i].motivation)
            # print(get_all_annotations_res[i].target.id)
            # print(get_all_annotations_res[i].body.id)
            # print()

# get_all_collections_res = g.getAllCollections()
# print(len(get_all_collections_res))

# # uncomment for sample tests
# # check collections
# for i in range(min(5, len(get_all_collections_res))):
#     print()
#     print(get_all_collections_res[i].id)
#     print(get_all_collections_res[i].label)
#     print(get_all_collections_res[i].title)
#     print(get_all_collections_res[i].creators)
#     print(get_all_collections_res[i].items)
#     print()


# c = CollectionProcessor()
# c.setDbPathOrUrl(GRAPH_DB_URL)
# c.uploadData(COLLECTION_ONE_PATH)
# c.uploadData(COLLECTION_TWO_PATH)

# test getAllCanvas -- seemingly ok
#
# uncomment to_csv in function definition for test with csv
#
# uncomment for length test
# get_all_canvas_res = generic.getAllCanvas()
# print(len(get_all_canvas_res))

# # uncomment for sample tests
# for i in range(min(5, len(get_all_canvas_res))):
#     print()
#     print(get_all_canvas_res[i].id)
#     print(get_all_canvas_res[i].label)
#     print(get_all_canvas_res[i].title)
#     print(get_all_canvas_res[i].creators)
#     print()
# for canvas in get_all_canvas_res:
#     if canvas.title and len(canvas.title):
#         print()
#         print(canvas.id)
#         print(canvas.label)
#         print(canvas.title)
#         print(canvas.creators)
#         print(type(canvas.creators))





# get_all_collections_res = generic.getAllCollections()
# print(len(get_all_collections_res))

# # uncomment for sample tests
# # check collections
# for i in range(min(5, len(get_all_collections_res))):
#     print()
#     print(get_all_collections_res[i].id)
#     print(get_all_collections_res[i].label)
#     print(get_all_collections_res[i].title)
#     print(get_all_collections_res[i].creators)
#     print(get_all_collections_res[i].items)
#     print()

# # check manifests
# for i in range(min(5, len(get_all_collections_res[0].items))):
#     print()
#     print(get_all_collections_res[0].items[i].id)
#     print(get_all_collections_res[0].items[i].label)
#     print(get_all_collections_res[0].items[i].title)
#     print(get_all_collections_res[0].items[i].creators)
#     print(get_all_collections_res[0].items[i].items)
#     print()

# # check canvases
# for i in range(min(5, len(get_all_collections_res[0].items[0].items))):
#     print()
#     print(get_all_collections_res[0].items[0].items[i].id)
#     print(get_all_collections_res[0].items[0].items[i].label)
#     print(get_all_collections_res[0].items[0].items[i].title)
#     print(get_all_collections_res[0].items[0].items[i].creators)
#     print()



# get_all_images_res = generic.getAllImages()
# print(len(get_all_images_res))
# # sample check
# for i in range(min(5, len(get_all_images_res))):
#     print()
#     print(get_all_images_res[i].id)
#     print()





# get_all_manifests_res = generic.getAllManifests()
# print(len(get_all_manifests_res))
# # check manifests
# for i in range(min(5, len(get_all_manifests_res))):
#     print()
#     print(get_all_manifests_res[i].id)
#     print(get_all_manifests_res[i].label)
#     print(get_all_manifests_res[i].title)
#     print(get_all_manifests_res[i].creators)
#     print(get_all_manifests_res[i].items)
#     print()

# # check canvases
# for i in range(min(5, len(get_all_manifests_res[0].items))):
#     print()
#     print(get_all_manifests_res[0].items[i].id)
#     print(get_all_manifests_res[0].items[i].label)
#     print(get_all_manifests_res[0].items[i].title)
#     print(get_all_manifests_res[0].items[i].creators)
#     print()


# # consider dropping annotation-id column, otherwise ok
# get_annotations_res = generic.getAnnotationsToCanvas('https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1')
# print(len(get_annotations_res))
# for i in range(min(5, len(get_annotations_res))):
#     print()
#     print(get_annotations_res[i].id)
#     print(get_annotations_res[i].motivation)
#     print(get_annotations_res[i].target.id)
#     print(get_annotations_res[i].body.id)
#     print()


# # if jpeg is in collection but not in .csv -- what then?
# get_annotations_res = generic.getAnnotationsWithBody("https://dl.ficlit.unibo.it/iiif/2/45498/full/699,800/0/default.jpg")
# print(len(get_annotations_res))
# for i in range(min(5, len(get_annotations_res))):
#     print()
#     print(get_annotations_res[i].id)
#     print(get_annotations_res[i].motivation)
#     print(get_annotations_res[i].target.id)
#     print(get_annotations_res[i].body.id)
#     print()

# get_annotations_res = generic.getAnnotationsWithBodyAndTarget("https://dl.ficlit.unibo.it/iiif/2/45498/full/699,800/0/default.jpg", "https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1")
# print(len(get_annotations_res))
# for i in range(min(5, len(get_annotations_res))):
#     print()
#     print(get_annotations_res[i].id)
#     print(get_annotations_res[i].motivation)
#     print(get_annotations_res[i].target.id)
#     print(get_annotations_res[i].body.id)
#     print()

# get_annotations_res = generic.getAnnotationsWithTarget("https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1")
# print(len(get_annotations_res))
# for i in range(min(5, len(get_annotations_res))):
#     print()
#     print(get_annotations_res[i].id)
#     print(get_annotations_res[i].motivation)
#     print(get_annotations_res[i].target.id)
#     print(get_annotations_res[i].body.id)
#     print()



# TODO: change query processor, add getentitiesbyid

# TODO: check getCanvasesInCollection
# uncomment to_csv inside the function to look up the results

# get_annotations_res = generic.getAnnotationsToCollection('https://dl.ficlit.unibo.it/iiif/19428-19425/collection')
# print(len(get_annotations_res))
# for i in range(min(5, len(get_annotations_res))):
#     print()
#     print(get_annotations_res[i].id)
#     print(get_annotations_res[i].motivation)
#     print(get_annotations_res[i].target.id)
#     print(get_annotations_res[i].body.id)
#     print()


# get_annotations_res = generic.getAnnotationsToManifest('https://dl.ficlit.unibo.it/iiif/2/28429/manifest')
# print(len(get_annotations_res))
# for i in range(min(5, len(get_annotations_res))):
#     print()
#     print(get_annotations_res[i].id)
#     print(get_annotations_res[i].motivation)
#     print(get_annotations_res[i].target.id)
#     print(get_annotations_res[i].body.id)
#     print()

# get_annotations_res = generic.getAnnotationsToManifest('https://dl.ficlit.unibo.it/iiif/2/28429/manifest')
# print(len(get_annotations_res))
# for i in range(min(5, len(get_annotations_res))):
#     print()
#     print(get_annotations_res[i].id)
#     print(get_annotations_res[i].motivation)
#     print(get_annotations_res[i].target.id)
#     print(get_annotations_res[i].body.id)
#     print()


## testing GQP.getEntitiesById
# get_all_images_res = generic.getEntityById('https://dl.ficlit.unibo.it/iiif/28429/collection')
# print(get_all_images_res.id)
# print(get_all_images_res)
# print()
# get_all_images_res = generic.getEntityById('https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p8')
# print(get_all_images_res.id)
# print(get_all_images_res)
# get_all_images_res = generic.getEntityById('just_an_example')
# print(get_all_images_res.id)
# print(get_all_images_res)
# print()


## testing QP
# q = QueryProcessor()
# q.setDbPathOrUrl('./data/test.db')
# print(q.getEntityById('just_a_test'))
# q.setDbPathOrUrl('http://127.0.0.1:9999/blazegraph/sparql')
# print(q.getEntityById('https://dl.ficlit.unibo.it/iiif/2/28429/manifest'))
# print(q.getEntityById('just_a_test'))





        