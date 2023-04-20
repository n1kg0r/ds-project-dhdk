from EntityWithMetadata import EntityWithMetadata

class Canvas(EntityWithMetadata):
    def __init__(self, id:str, label, title, creators):
        pass

class Manifest(EntityWithMetadata):
    def __init__(self, id:str, label, title, creators, items:list[Canvas]):
        super().__init__(id, label, title, creators)
        self.items = items
    def getItems(self):
        return self.items

class Collection(EntityWithMetadata):
    def __init__(self, id:str, label, title, creators, items:list[Manifest]):
        super().__init__(id, label, title, creators)
        self.items = items
    def getItems(self):
        return self.items

# c = Canvas('a', 'b', 'c', [1,2,3])
# print(c.creators)