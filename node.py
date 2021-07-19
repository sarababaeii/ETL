class Node:
    def __init__(self, table_schema, table_name, operation):
        self.table_schema = table_schema
        self.table_name = table_name
        self.operation = operation
        self.node_name = table_schema + "." + table_name + "." + str(operation)
        self.parents = []

    @staticmethod
    def get_node_with_operation(table_nodes, operation):
        for node in table_nodes:
            if str(node.operation) == str(operation):
                return node
        return None

    def set_parent(self, parent):
        self.parents.append(parent.node_name)

    def print_node(self):
        print(self.node_name, self.parents)
