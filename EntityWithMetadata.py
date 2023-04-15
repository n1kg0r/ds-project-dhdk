class EntityWithMetadata(IdentifiableEntity):
    def _init_(self, id, label, title, creators):
        self.label = label 
        self.title = title
        self.creators = list()
        if creators:
            for creator in creators:
                self.creators.append(creator)

    super().__init__(id)

    def getLabel(self):
        return self.label
    
    def getTitle(self):
        if self.title:
            return self.title
        else:
            return None
    
    def getCreators(self):
        return self.creators
    
