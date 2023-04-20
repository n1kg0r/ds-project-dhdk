from EntityWithMetadata import EntityWithMetadata

class Canvas(EntityWithMetadata):
    def __init__(self, id:str, label:str, title:str, creators:list[str]):
        super().__init__(id, label, title, creators)

class Manifest(EntityWithMetadata):
    def __init__(self, id:str, label:str, title:str, creators:list[str], items:list[Canvas]):
        super().__init__(id, label, title, creators)
        self.items = items
    def getItems(self):
        return self.items

class Collection(EntityWithMetadata):
    def __init__(self, id:str, label:str, title:str, creators:list[str], items:list[Manifest]):
        super().__init__(id, label, title, creators)
        self.items = items
    def getItems(self):
        return self.items

# c = Canvas(10.5, 'b', 'c', [1,2,3])
# print(c.creators)