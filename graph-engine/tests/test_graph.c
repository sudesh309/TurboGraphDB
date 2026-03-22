/**
 * TurboGraphDB Test Suite
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "../include/graph.h"

#define TEST_PASS "\033[32mPASS\033[0m"
#define TEST_FAIL "\033[31mFAIL\033[0m"

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name, condition) do { \
    printf("  Testing %s... ", name); \
    if (condition) { \
        printf("%s\n", TEST_PASS); \
        tests_passed++; \
    } else { \
        printf("%s\n", TEST_FAIL); \
        tests_failed++; \
    } \
} while(0)

/* Test basic graph creation */
void test_graph_create() {
    printf("\n=== Testing Graph Creation ===\n");
    
    Graph* graph = graph_create("test_db");
    TEST("graph creation", graph != NULL);
    TEST("graph name", strcmp(graph->name, "test_db") == 0);
    TEST("initial node count", graph_node_count(graph) == 0);
    TEST("initial edge count", graph_edge_count(graph) == 0);
    
    graph_destroy(graph);
}

/* Test node operations */
void test_node_operations() {
    printf("\n=== Testing Node Operations ===\n");
    
    Graph* graph = graph_create("test_db");
    
    /* Create nodes */
    Node* n1 = graph_create_node(graph, "Person");
    TEST("node creation", n1 != NULL);
    TEST("node id", n1->id == 1);
    TEST("node label", strcmp(n1->label, "Person") == 0);
    
    Node* n2 = graph_create_node(graph, "Person");
    TEST("second node", n2 != NULL && n2->id == 2);
    
    Node* n3 = graph_create_node(graph, "Movie");
    TEST("different label node", n3 != NULL && strcmp(n3->label, "Movie") == 0);
    
    TEST("node count", graph_node_count(graph) == 3);
    
    /* Get node */
    Node* retrieved = graph_get_node(graph, 1);
    TEST("get node by id", retrieved == n1);
    
    /* Node properties */
    node_set_property(n1, "name", prop_string("Alice"));
    node_set_property(n1, "age", prop_int64(30));
    
    PropertyValue name = node_get_property(n1, "name");
    TEST("string property", name.type == PROP_TYPE_STRING && strcmp(name.value.string_val, "Alice") == 0);
    prop_free(&name);
    
    PropertyValue age = node_get_property(n1, "age");
    TEST("int property", age.type == PROP_TYPE_INT64 && age.value.int_val == 30);
    prop_free(&age);
    
    TEST("property exists", node_has_property(n1, "name"));
    TEST("property not exists", !node_has_property(n1, "nonexistent"));
    
    /* Get nodes by label */
    NodeResult person_nodes = graph_get_nodes_by_label(graph, "Person");
    TEST("nodes by label", person_nodes.count == 2);
    node_result_free(&person_nodes);
    
    NodeResult movie_nodes = graph_get_nodes_by_label(graph, "Movie");
    TEST("different label count", movie_nodes.count == 1);
    node_result_free(&movie_nodes);
    
    /* Delete node */
    bool deleted = graph_delete_node(graph, 3);
    TEST("delete node", deleted);
    TEST("node count after delete", graph_node_count(graph) == 2);
    TEST("deleted node not found", graph_get_node(graph, 3) == NULL);
    
    graph_destroy(graph);
}

/* Test edge operations */
void test_edge_operations() {
    printf("\n=== Testing Edge Operations ===\n");
    
    Graph* graph = graph_create("test_db");
    
    Node* n1 = graph_create_node(graph, "Person");
    Node* n2 = graph_create_node(graph, "Person");
    Node* n3 = graph_create_node(graph, "Movie");
    
    /* Create edges */
    Edge* e1 = graph_create_edge(graph, n1->id, n2->id, "KNOWS", 1.0);
    TEST("edge creation", e1 != NULL);
    TEST("edge id", e1->id == 1);
    TEST("edge label", strcmp(e1->label, "KNOWS") == 0);
    TEST("edge source", e1->source == n1->id);
    TEST("edge target", e1->target == n2->id);
    
    Edge* e2 = graph_create_edge(graph, n1->id, n3->id, "LIKES", 0.5);
    TEST("second edge", e2 != NULL);
    
    Edge* e3 = graph_create_edge(graph, n2->id, n3->id, "LIKES", 0.8);
    TEST("third edge", e3 != NULL);
    
    TEST("edge count", graph_edge_count(graph) == 3);
    
    /* Edge properties */
    edge_set_property(e1, "since", prop_string("2020"));
    edge_set_property(e1, "weight", prop_float64(0.9));
    
    PropertyValue since = edge_get_property(e1, "since");
    TEST("edge string property", since.type == PROP_TYPE_STRING && strcmp(since.value.string_val, "2020") == 0);
    prop_free(&since);
    
    /* Out degree / In degree */
    TEST("out degree", n1->out_degree == 2);
    TEST("in degree for n3", n3->in_degree == 2);
    
    /* Get edges */
    EdgeResult out_edges = graph_get_out_edges(graph, n1->id);
    TEST("out edges count", out_edges.count == 2);
    edge_result_free(&out_edges);
    
    EdgeResult in_edges = graph_get_in_edges(graph, n3->id);
    TEST("in edges count", in_edges.count == 2);
    edge_result_free(&in_edges);
    
    EdgeResult edges_between = graph_get_edges_between(graph, n1->id, n2->id);
    TEST("edges between", edges_between.count == 1);
    edge_result_free(&edges_between);
    
    /* Delete edge */
    bool deleted = graph_delete_edge(graph, e2->id);
    TEST("delete edge", deleted);
    TEST("edge count after delete", graph_edge_count(graph) == 2);
    TEST("out degree after delete", n1->out_degree == 1);
    
    graph_destroy(graph);
}

/* Test traversal algorithms */
void test_traversal() {
    printf("\n=== Testing Traversal Algorithms ===\n");
    
    Graph* graph = graph_create("test_db");
    
    /* Create a simple graph:
     *   1 -> 2 -> 3
     *   1 -> 4
     *   2 -> 4
     *   3 -> 5
     */
    Node* n1 = graph_create_node(graph, "Node");
    Node* n2 = graph_create_node(graph, "Node");
    Node* n3 = graph_create_node(graph, "Node");
    Node* n4 = graph_create_node(graph, "Node");
    Node* n5 = graph_create_node(graph, "Node");
    
    graph_create_edge(graph, n1->id, n2->id, "CONNECTS", 1.0);
    graph_create_edge(graph, n2->id, n3->id, "CONNECTS", 1.0);
    graph_create_edge(graph, n1->id, n4->id, "CONNECTS", 1.0);
    graph_create_edge(graph, n2->id, n4->id, "CONNECTS", 1.0);
    graph_create_edge(graph, n3->id, n5->id, "CONNECTS", 1.0);
    
    /* BFS */
    NodeResult bfs_result = graph_bfs(graph, n1->id, 0);
    TEST("BFS visits all nodes", bfs_result.count == 5);
    TEST("BFS starts with source", bfs_result.nodes[0]->id == n1->id);
    node_result_free(&bfs_result);
    
    /* BFS with depth limit */
    NodeResult bfs_depth = graph_bfs(graph, n1->id, 1);
    TEST("BFS depth 1", bfs_depth.count == 3); /* n1, n2, n4 */
    node_result_free(&bfs_depth);
    
    /* DFS */
    NodeResult dfs_result = graph_dfs(graph, n1->id, 0);
    TEST("DFS visits all nodes", dfs_result.count == 5);
    node_result_free(&dfs_result);
    
    /* Shortest path */
    Path* path = graph_shortest_path(graph, n1->id, n5->id);
    TEST("shortest path found", path != NULL);
    if (path) {
        TEST("path length", path->length == 4); /* 1 -> 2 -> 3 -> 5 */
        char* path_str = path_to_string(path);
        printf("    Path: %s\n", path_str);
        free(path_str);
        path_free(path);
    }
    
    graph_destroy(graph);
}

/* Test serialization */
void test_serialization() {
    printf("\n=== Testing Serialization ===\n");
    
    Graph* graph = graph_create("test_db");
    
    /* Create some data */
    Node* n1 = graph_create_node(graph, "Person");
    node_set_property(n1, "name", prop_string("Alice"));
    node_set_property(n1, "age", prop_int64(30));
    
    Node* n2 = graph_create_node(graph, "Person");
    node_set_property(n2, "name", prop_string("Bob"));
    
    Edge* e1 = graph_create_edge(graph, n1->id, n2->id, "KNOWS", 1.0);
    edge_set_property(e1, "since", prop_string("2020"));
    
    /* Save */
    bool saved = graph_save(graph, "/tmp/test_graph.tgdb");
    TEST("save graph", saved);
    
    /* Load */
    Graph* loaded = graph_load("/tmp/test_graph.tgdb");
    TEST("load graph", loaded != NULL);
    
    if (loaded) {
        TEST("loaded node count", graph_node_count(loaded) == 2);
        TEST("loaded edge count", graph_edge_count(loaded) == 1);
        
        Node* loaded_n1 = graph_get_node(loaded, n1->id);
        TEST("loaded node exists", loaded_n1 != NULL);
        
        if (loaded_n1) {
            PropertyValue name = node_get_property(loaded_n1, "name");
            TEST("loaded property", name.type == PROP_TYPE_STRING && 
                 strcmp(name.value.string_val, "Alice") == 0);
            prop_free(&name);
        }
        
        graph_destroy(loaded);
    }
    
    graph_destroy(graph);
    
    /* Cleanup */
    remove("/tmp/test_graph.tgdb");
}

/* Test JSON export */
void test_json_export() {
    printf("\n=== Testing JSON Export ===\n");
    
    Graph* graph = graph_create("test_db");
    
    Node* n1 = graph_create_node(graph, "Person");
    node_set_property(n1, "name", prop_string("Alice"));
    
    Node* n2 = graph_create_node(graph, "Person");
    node_set_property(n2, "name", prop_string("Bob"));
    
    graph_create_edge(graph, n1->id, n2->id, "KNOWS", 1.0);
    
    char* json = graph_to_json(graph);
    TEST("json export", json != NULL && strlen(json) > 0);
    TEST("json contains name", strstr(json, "Alice") != NULL);
    TEST("json contains edge", strstr(json, "KNOWS") != NULL);
    
    printf("    JSON: %s\n", json);
    
    free(json);
    graph_destroy(graph);
}

/* Test memory statistics */
void test_memory_stats() {
    printf("\n=== Testing Memory Stats ===\n");
    
    Graph* graph = graph_create("test_db");
    
    for (int i = 0; i < 100; i++) {
        Node* n = graph_create_node(graph, "Node");
        node_set_property(n, "value", prop_int64(i));
    }
    
    for (int i = 0; i < 99; i++) {
        graph_create_edge(graph, i + 1, i + 2, "CONNECTS", 1.0);
    }
    
    GraphStats stats = graph_get_stats(graph);
    TEST("node count stats", stats.node_count == 100);
    TEST("edge count stats", stats.edge_count == 99);
    TEST("memory calculated", stats.total_memory > 0);
    
    printf("    Total memory: %zu bytes\n", stats.total_memory);
    printf("    Node memory: %zu bytes\n", stats.node_memory);
    printf("    Edge memory: %zu bytes\n", stats.edge_memory);
    printf("    Index memory: %zu bytes\n", stats.index_memory);
    
    graph_destroy(graph);
}

/* Performance test */
void test_performance() {
    printf("\n=== Testing Performance ===\n");
    
    Graph* graph = graph_create("perf_test");
    
    const int NODE_COUNT = 10000;
    const int EDGE_COUNT = 50000;
    
    /* Node creation */
    clock_t start = clock();
    for (int i = 0; i < NODE_COUNT; i++) {
        Node* n = graph_create_node(graph, "Node");
        node_set_property(n, "id", prop_int64(i));
        node_set_property(n, "name", prop_string("NodeName"));
    }
    clock_t end = clock();
    double node_time = (double)(end - start) / CLOCKS_PER_SEC;
    printf("  Created %d nodes in %.3f seconds (%.0f nodes/sec)\n",
           NODE_COUNT, node_time, NODE_COUNT / node_time);
    
    /* Edge creation */
    start = clock();
    for (int i = 0; i < EDGE_COUNT; i++) {
        NodeId source = (rand() % NODE_COUNT) + 1;
        NodeId target = (rand() % NODE_COUNT) + 1;
        graph_create_edge(graph, source, target, "CONNECTS", 1.0);
    }
    end = clock();
    double edge_time = (double)(end - start) / CLOCKS_PER_SEC;
    printf("  Created %d edges in %.3f seconds (%.0f edges/sec)\n",
           EDGE_COUNT, edge_time, EDGE_COUNT / edge_time);
    
    /* BFS traversal */
    start = clock();
    NodeResult bfs_result = graph_bfs(graph, 1, 0);
    end = clock();
    double bfs_time = (double)(end - start) / CLOCKS_PER_SEC;
    printf("  BFS from node 1 visited %zu nodes in %.4f seconds\n",
           bfs_result.count, bfs_time);
    node_result_free(&bfs_result);
    
    /* Shortest path */
    start = clock();
    Path* path = graph_shortest_path(graph, 1, NODE_COUNT / 2);
    end = clock();
    double path_time = (double)(end - start) / CLOCKS_PER_SEC;
    if (path) {
        printf("  Shortest path (1 -> %d) found in %.4f seconds, length: %zu\n",
               NODE_COUNT / 2, path_time, path->length);
        path_free(path);
    } else {
        printf("  No path found\n");
    }
    
    /* Memory stats */
    GraphStats stats = graph_get_stats(graph);
    printf("  Total memory used: %.2f MB\n", stats.total_memory / (1024.0 * 1024.0));
    
    graph_destroy(graph);
}

int main() {
    printf("\n");
    printf("╔════════════════════════════════════════════════════════╗\n");
    printf("║         TurboGraphDB - Test Suite                      ║\n");
    printf("║     High-Performance Graph Database Engine             ║\n");
    printf("╚════════════════════════════════════════════════════════╝\n");
    
    srand(42); /* Fixed seed for reproducibility */
    
    test_graph_create();
    test_node_operations();
    test_edge_operations();
    test_traversal();
    test_serialization();
    test_json_export();
    test_memory_stats();
    test_performance();
    
    printf("\n╔════════════════════════════════════════════════════════╗\n");
    printf("║                    Test Results                        ║\n");
    printf("╠════════════════════════════════════════════════════════╣\n");
    printf("║  Passed: %3d                                           ║\n", tests_passed);
    printf("║  Failed: %3d                                           ║\n", tests_failed);
    printf("║  Total:  %3d                                           ║\n", tests_passed + tests_failed);
    printf("╚════════════════════════════════════════════════════════╝\n\n");
    
    return tests_failed > 0 ? 1 : 0;
}
