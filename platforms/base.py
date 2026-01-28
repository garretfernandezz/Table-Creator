from abc import ABC, abstractmethod

class TableCreator(ABC):

    @abstractmethod
    def create_table(self, table):
        pass
