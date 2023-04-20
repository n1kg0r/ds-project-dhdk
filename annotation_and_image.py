from identifiable_entity import IdentifiableEntity

class Annotation(IdentifiableEntity):
    def __init__(self, id, motivation:str, target:IdentifiableEntity, body:Image):
        self.motivation = motivation
        self.target = target
        self.body = body
        super().__init__(id)
    def getBody(self):
        return self.body
    def getMotivation(self):
        return self.motivation
    def getTarget(self):
        return self.target
    
class Image(IdentifiableEntity):
    pass
