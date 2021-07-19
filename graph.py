from graphlib import TopologicalSorter


class Graph:
    def __init__(self):
        self.nodes = []

    def insert_node(self, node):
        self.nodes.append(node)

    def get_node_with_name(self, node_name):
        for node in self.nodes:
            if node.node_name == node_name:
                return node
        return None

    def get_node_for_table(self, schema_table):
        address = schema_table.split(".")
        table_nodes = []
        for node in self.nodes:
            if node.table_schema == address[0] and node.table_name == address[1]:
                table_nodes.append(node)
        return table_nodes

    def get_dag_dictionary(self):
        graph = {}
        for node in self.nodes:
            graph[node.node_name] = node.parents
        return graph

    def topological_sort(self):
        graph = self.get_dag_dictionary()
        topological_sorter = TopologicalSorter(graph)
        return tuple(topological_sorter.static_order())
