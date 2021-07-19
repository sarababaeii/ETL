from database import Database
from graph import Graph
from node import Node
from operation import Operation


def create_tasks_graph(tables):
    graph = Graph()
    source = Node("create", "connection", Operation.none)
    sink = Node("close", "connection", Operation.none)
    graph.insert_node(source)
    graph.insert_node(sink)
    for table in tables:
        table_nodes = create_nodes_for_table(table)
        set_inner_table_edges(table_nodes, source, sink)
        insert_table_nodes_to_graph(graph, table_nodes)
    return graph


def create_nodes_for_table(table):
    insert_node = Node(table["table_schema"], table["table_name"], Operation.insert)
    update_node = Node(table["table_schema"], table["table_name"], Operation.update)
    delete_node = Node(table["table_schema"], table["table_name"], Operation.delete)
    nodes = {str(Operation.insert): insert_node, str(Operation.update): update_node, str(Operation.delete): delete_node}
    return nodes


def insert_table_nodes_to_graph(graph, table_nodes):
    graph.insert_node(table_nodes[str(Operation.insert)])
    graph.insert_node(table_nodes[str(Operation.update)])
    graph.insert_node(table_nodes[str(Operation.delete)])


def set_inner_table_edges(table_nodes, source, sink):
    sink.set_parent(table_nodes[str(Operation.insert)])
    table_nodes[str(Operation.insert)].set_parent(table_nodes[str(Operation.update)])
    table_nodes[str(Operation.update)].set_parent(table_nodes[str(Operation.delete)])
    table_nodes[str(Operation.delete)].set_parent(source)


def set_edges(graph, foreign_keys):
    for foreign_key in foreign_keys:
        set_outer_table_edges(graph, foreign_key)


def set_outer_table_edges(graph, foreign_key):
    foreign_table_nodes, primary_table_nodes = get_nodes_of_foreign_key_constraint(graph, foreign_key)
    foreign_table_insert_node = Node.get_node_with_operation(foreign_table_nodes, Operation.insert)
    foreign_table_delete_node = Node.get_node_with_operation(foreign_table_nodes, Operation.delete)
    primary_table_insert_node = Node.get_node_with_operation(primary_table_nodes, Operation.insert)
    primary_table_delete_node = Node.get_node_with_operation(primary_table_nodes, Operation.delete)

    primary_table_delete_node.set_parent(foreign_table_delete_node)
    foreign_table_insert_node.set_parent(primary_table_insert_node)


def get_nodes_of_foreign_key_constraint(graph, foreign_key):
    foreign_table_name = foreign_key["foreign_table"]
    primary_table_name = foreign_key["primary_table"]
    foreign_table_nodes = graph.get_node_for_table(foreign_table_name)
    primary_table_nodes = graph.get_node_for_table(primary_table_name)
    return foreign_table_nodes, primary_table_nodes


def get_etl_tasks(database):
    tables = database.get_tables()
    graph = create_tasks_graph(tables)
    f_keys = database.get_foreign_keys()
    set_edges(graph, f_keys)
    return graph


def etl(source, warehouse):
    tasks = get_etl_tasks(source)
    sorted_tasks = tasks.topological_sort()
    for i in range(len(sorted_tasks)):
        node = tasks.get_node_with_name(sorted_tasks[i])
        do_operation(source, warehouse, node)


def do_operation(source, warehouse, node):
    print(str(node.operation), "on", node.table_schema, ".", node.table_name)
    source.fetch_all_rows(node)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    source = Database("localhost", "Source", "postgres", "ioi2018")
    warehouse = Database("localhost", "Warehouse", "postgres", "ioi2018")
    etl(source, warehouse)

# def get_tree(connection):
#     tree = get_tables(connection)
#     for table in tree:
#         table["columns"] = get_columns(connection, table["table_schema"], table["table_name"])
#     return tree


# def get_constraints(connection, table_schema, table_name):
#     where_dict = {"table_schema": table_schema, "table_name": table_name}
#     cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
#     cursor.execute("""SELECT *
#                           FROM pg_catalog.pg_constraint con
#                                 INNER JOIN pg_catalog.pg_class rel
#                                            ON rel.oid = con.conrelid
#                                 INNER JOIN pg_catalog.pg_namespace nsp
#                                            ON nsp.oid = connamespace
#                           WHERE nsp.nspname = %(table_schema)s
#                                 AND rel.relname = %(table_name)s""",
#                    where_dict)
#     constraints = cursor.fetchall()
#     cursor.close()
#     return constraints


# def print_tables(tables):
#     for row in tables:
#         print("{}.{}".format(row["table_schema"], row["table_name"]))


# def print_columns(columns):
#     for row in columns:
#         print("Column Name:              {}".format(row["column_name"]))
#         print("Ordinal Position:         {}".format(row["ordinal_position"]))
#         print("Is Nullable:              {}".format(row["is_nullable"]))
#         print("Data Type:                {}".format(row["data_type"]))
#         print("Character Maximum Length: {}\n".format(row["character_maximum_length"]))


# def print_tree(tree):
#     for table in tree:
#         print("{}.{}".format(table["table_schema"], table["table_name"]))
#         for column in table["columns"]:
#             print(" |-{} ({})".format(column["column_name"], column["data_type"]))
