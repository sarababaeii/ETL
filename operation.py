from enum import Enum


class Operation(Enum):
    insert = "insert"
    update = "update"
    delete = "delete"
    none = ""

    def __str__(self):
        return str(self.value)