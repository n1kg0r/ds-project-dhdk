import unittest
from os import sep
from impl import AnnotationProcessor, MetadataProcessor, RelationalQueryProcessor
from impl import CollectionProcessor, TriplestoreQueryProcessor
from impl import GenericQueryProcessor
from pandas import DataFrame
from impl import IdentifiableEntity, EntityWithMetadata, Canvas, Collection, Image, Annotation, Manifest

annotations = "data" + sep + "annotations.csv"
collection = "data" + sep + "collection-1.json"
metadata = "data" + sep + "metadata.csv"
relational = "." + sep + "relational.db"
graph = "http://192.168.1.52:9999/blazegraph/"

# uncomment if databases not populated
# rel_path = "./data/test.db"
# ann_dp = AnnotationProcessor()
# ann_dp.setDbPathOrUrl(rel_path)
# ann_dp.uploadData("data/annotations.csv")
# met_dp = MetadataProcessor()
# met_dp.setDbPathOrUrl(rel_path)
# met_dp.uploadData("data/metadata.csv")
# grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
# col_dp = CollectionProcessor()
# col_dp.setDbPathOrUrl(grp_endpoint)
# col_dp.uploadData("data/collection-1.json")
# col_dp.uploadData("data/collection-2.json")


rel_qp = RelationalQueryProcessor()
rel_qp.setDbPathOrUrl('./data/test.db')

grp_qp = TriplestoreQueryProcessor()
print(grp_qp.setDbPathOrUrl('http://127.0.0.1:9999/blazegraph/sparql'))

generic = GenericQueryProcessor()
generic.addQueryProcessor(rel_qp)
generic.addQueryProcessor(grp_qp)

# getAllAnnotations test ok
# get_all_annotations_res = generic.getAllAnnotations()
# print(len(get_all_annotations_res))
# for i in range(min(5, len(get_all_annotations_res))):
#     print()
#     print(get_all_annotations_res[i].id)
#     print(get_all_annotations_res[i].motivation)
#     print(get_all_annotations_res[i].target.id)
#     print(get_all_annotations_res[i].body.id)
#     print()


# test getAllCanvas -- seemingly ok
# consult with Erica about the data as they are
#
# uncomment to_csv in function definition for test with csv
#
# uncomment for length test
# get_all_canvas_res = generic.getAllCanvas()
# print(len(get_all_canvas_res))
#
# uncomment for sample tests
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

# uncomment for sample tests
# check collections
# for i in range(min(5, len(get_all_collections_res))):
#     print()
#     print(get_all_collections_res[i].id)
#     print(get_all_collections_res[i].label)
#     print(get_all_collections_res[i].title)
#     print(get_all_collections_res[i].creators)
#     print(get_all_collections_res[i].items)
#     print()
#
# check manifests
# for i in range(min(5, len(get_all_collections_res[0].items))):
#     print()
#     print(get_all_collections_res[0].items[i].id)
#     print(get_all_collections_res[0].items[i].label)
#     print(get_all_collections_res[0].items[i].title)
#     print(get_all_collections_res[0].items[i].creators)
#     print(get_all_collections_res[0].items[i].items)
#     print()
#
# check canvases
# for i in range(min(5, len(get_all_collections_res[0].items[0].items))):
#     print()
#     print(get_all_collections_res[0].items[0].items[i].id)
#     print(get_all_collections_res[0].items[0].items[i].label)
#     print(get_all_collections_res[0].items[0].items[i].title)
#     print(get_all_collections_res[0].items[0].items[i].creators)
#     print()



# get_all_images_res = generic.getAllImages()
# print(len(get_all_images_res))
# sample check
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
#
# # check canvases
# for i in range(min(5, len(get_all_manifests_res[0].items))):
#     print()
#     print(get_all_manifests_res[0].items[i].id)
#     print(get_all_manifests_res[0].items[i].label)
#     print(get_all_manifests_res[0].items[i].title)
#     print(get_all_manifests_res[0].items[i].creators)
#     print()


# consider dropping annotation-id column, otherwise ok
# get_annotations_res = generic.getAnnotationsToCanvas('https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p1')
# print(len(get_annotations_res))
# for i in range(min(5, len(get_annotations_res))):
#     print()
#     print(get_annotations_res[i].id)
#     print(get_annotations_res[i].motivation)
#     print(get_annotations_res[i].target.id)
#     print(get_annotations_res[i].body.id)
#     print()


# if jpeg is in collection but not in .csv -- what then?
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

# TODO: check two methods, revisit previous ones (drop extra column)
# TODO: change query processor, add getentitiesbyid





class TestProjectBasic(unittest.TestCase):
    def test_generic(self):
        self.assertEqual(generic.queryProcessors, [])

        generic.addQueryProcessor(rel_qp)
        generic.addQueryProcessor(grp_qp)
        print(generic.queryProcessors)

        generic.cleanQueryProcessors()
        print(generic.queryProcessors)

        generic.addQueryProcessor(rel_qp)
        generic.addQueryProcessor(grp_qp)
        print(generic.queryProcessors)

        