/**
 * TurboGraphDB Example Application: Social Network
 *
 * Demonstrates:
 *   - Node/edge creation with properties
 *   - BFS/DFS traversal
 *   - Shortest path (unweighted and weighted)
 *   - Label-based lookups
 *   - JSON export
 *   - Serialization (save/load)
 *   - Memory stats
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../graph-engine/include/graph.h"

/* -------- Helpers -------- */

static void print_section(const char* title) {
    printf("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
    printf("  %s\n", title);
    printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
}

static void print_node(Graph* g, NodeId id) {
    Node* n = graph_get_node(g, id);
    if (!n) return;
    PropertyValue name = node_get_property(n, "name");
    printf("  Node %-2llu  [%s]  name=%s\n",
           (unsigned long long)n->id,
           n->label,
           name.type == PROP_TYPE_STRING ? name.value.string_val : "?");
    prop_free(&name);
}

/* -------- Build the graph -------- */

typedef struct {
    NodeId alice, bob, carol, dave, eve, frank, grace;
    NodeId london, paris, berlin, rome, madrid;
} Ids;

static Ids build_social_network(Graph* g) {
    Ids ids;

    /* --- People --- */
    Node* alice = graph_create_node(g, "Person");
    node_set_property(alice, "name",  prop_string("Alice"));
    node_set_property(alice, "age",   prop_int64(30));
    node_set_property(alice, "score", prop_float64(9.2));

    Node* bob = graph_create_node(g, "Person");
    node_set_property(bob, "name", prop_string("Bob"));
    node_set_property(bob, "age",  prop_int64(25));
    node_set_property(bob, "score", prop_float64(7.8));

    Node* carol = graph_create_node(g, "Person");
    node_set_property(carol, "name", prop_string("Carol"));
    node_set_property(carol, "age",  prop_int64(35));
    node_set_property(carol, "score", prop_float64(8.5));

    Node* dave = graph_create_node(g, "Person");
    node_set_property(dave, "name", prop_string("Dave"));
    node_set_property(dave, "age",  prop_int64(28));
    node_set_property(dave, "score", prop_float64(6.1));

    Node* eve = graph_create_node(g, "Person");
    node_set_property(eve, "name", prop_string("Eve"));
    node_set_property(eve, "age",  prop_int64(22));
    node_set_property(eve, "score", prop_float64(8.9));

    Node* frank = graph_create_node(g, "Person");
    node_set_property(frank, "name", prop_string("Frank"));
    node_set_property(frank, "age",  prop_int64(40));
    node_set_property(frank, "score", prop_float64(7.3));

    Node* grace = graph_create_node(g, "Person");
    node_set_property(grace, "name", prop_string("Grace"));
    node_set_property(grace, "age",  prop_int64(33));
    node_set_property(grace, "score", prop_float64(9.7));

    /* --- Cities --- */
    Node* london = graph_create_node(g, "City");
    node_set_property(london, "name",       prop_string("London"));
    node_set_property(london, "population", prop_int64(9000000));

    Node* paris = graph_create_node(g, "City");
    node_set_property(paris, "name",       prop_string("Paris"));
    node_set_property(paris, "population", prop_int64(2200000));

    Node* berlin = graph_create_node(g, "City");
    node_set_property(berlin, "name",       prop_string("Berlin"));
    node_set_property(berlin, "population", prop_int64(3700000));

    Node* rome = graph_create_node(g, "City");
    node_set_property(rome, "name",       prop_string("Rome"));
    node_set_property(rome, "population", prop_int64(2800000));

    Node* madrid = graph_create_node(g, "City");
    node_set_property(madrid, "name",       prop_string("Madrid"));
    node_set_property(madrid, "population", prop_int64(3200000));

    /* --- KNOWS relationships --- */
    Edge* e;
    e = graph_create_edge(g, alice->id, bob->id,   "KNOWS", 1.0);
    edge_set_property(e, "since", prop_string("2018"));

    e = graph_create_edge(g, alice->id, carol->id, "KNOWS", 1.0);
    edge_set_property(e, "since", prop_string("2015"));

    e = graph_create_edge(g, bob->id,   dave->id,  "KNOWS", 1.0);
    edge_set_property(e, "since", prop_string("2020"));

    e = graph_create_edge(g, carol->id, eve->id,   "KNOWS", 1.0);
    edge_set_property(e, "since", prop_string("2019"));

    e = graph_create_edge(g, dave->id,  frank->id, "KNOWS", 1.0);
    edge_set_property(e, "since", prop_string("2021"));

    e = graph_create_edge(g, eve->id,   grace->id, "KNOWS", 1.0);
    edge_set_property(e, "since", prop_string("2022"));

    e = graph_create_edge(g, bob->id,   grace->id, "KNOWS", 1.0);
    edge_set_property(e, "since", prop_string("2017"));

    /* --- LIVES_IN relationships --- */
    graph_create_edge(g, alice->id, london->id, "LIVES_IN", 1.0);
    graph_create_edge(g, bob->id,   paris->id,  "LIVES_IN", 1.0);
    graph_create_edge(g, carol->id, berlin->id, "LIVES_IN", 1.0);
    graph_create_edge(g, dave->id,  rome->id,   "LIVES_IN", 1.0);
    graph_create_edge(g, eve->id,   madrid->id, "LIVES_IN", 1.0);
    graph_create_edge(g, frank->id, london->id, "LIVES_IN", 1.0);
    graph_create_edge(g, grace->id, paris->id,  "LIVES_IN", 1.0);

    /* --- City distances (weighted for Dijkstra) --- */
    graph_create_edge(g, london->id, paris->id,  "FLIGHT", 344.0);
    graph_create_edge(g, paris->id,  berlin->id, "FLIGHT", 878.0);
    graph_create_edge(g, paris->id,  madrid->id, "FLIGHT", 1054.0);
    graph_create_edge(g, berlin->id, rome->id,   "FLIGHT", 1185.0);
    graph_create_edge(g, madrid->id, rome->id,   "FLIGHT", 1362.0);
    graph_create_edge(g, london->id, berlin->id, "FLIGHT", 930.0);

    ids.alice  = alice->id;  ids.bob   = bob->id;
    ids.carol  = carol->id;  ids.dave  = dave->id;
    ids.eve    = eve->id;    ids.frank = frank->id;
    ids.grace  = grace->id;
    ids.london = london->id; ids.paris  = paris->id;
    ids.berlin = berlin->id; ids.rome   = rome->id;
    ids.madrid = madrid->id;
    return ids;
}

/* -------- Demo functions -------- */

static void demo_overview(Graph* g) {
    print_section("1. Graph Overview");
    printf("  Nodes : %zu\n", graph_node_count(g));
    printf("  Edges : %zu\n", graph_edge_count(g));
    printf("  Props : %zu\n", graph_property_count(g));
}

static void demo_label_query(Graph* g) {
    print_section("2. Label-Based Lookup");

    NodeResult people = graph_get_nodes_by_label(g, "Person");
    printf("  People (%zu):\n", people.count);
    for (size_t i = 0; i < people.count; i++) print_node(g, people.nodes[i]->id);
    node_result_free(&people);

    NodeResult cities = graph_get_nodes_by_label(g, "City");
    printf("  Cities (%zu):\n", cities.count);
    for (size_t i = 0; i < cities.count; i++) print_node(g, cities.nodes[i]->id);
    node_result_free(&cities);
}

static void demo_bfs(Graph* g, Ids* ids) {
    print_section("3. BFS Traversal from Alice");

    NodeResult bfs = graph_bfs(g, ids->alice, 0);
    printf("  BFS visits %zu node(s):\n", bfs.count);
    for (size_t i = 0; i < bfs.count; i++) print_node(g, bfs.nodes[i]->id);
    node_result_free(&bfs);

    printf("\n  BFS depth-limited (depth=2) from Alice:\n");
    NodeResult bfs2 = graph_bfs(g, ids->alice, 2);
    printf("  Visits %zu node(s):\n", bfs2.count);
    for (size_t i = 0; i < bfs2.count; i++) print_node(g, bfs2.nodes[i]->id);
    node_result_free(&bfs2);
}

static void demo_dfs(Graph* g, Ids* ids) {
    print_section("4. DFS Traversal from Alice");

    NodeResult dfs = graph_dfs(g, ids->alice, 0);
    printf("  DFS visits %zu node(s):\n", dfs.count);
    for (size_t i = 0; i < dfs.count; i++) print_node(g, dfs.nodes[i]->id);
    node_result_free(&dfs);
}

static void demo_shortest_path(Graph* g, Ids* ids) {
    print_section("5. Shortest Path (Alice -> Grace, unweighted)");

    Path* path = graph_shortest_path(g, ids->alice, ids->grace);
    if (path) {
        char* s = path_to_string(path);
        printf("  Path (length %zu): %s\n", path->length, s);
        free(s);
        path_free(path);
    } else {
        printf("  No path found.\n");
    }

    print_section("6. Weighted Shortest Path (London -> Rome by flight km)");
    Path* wpath = graph_shortest_path_weighted(g, ids->london, ids->rome);
    if (wpath) {
        char* s = path_to_string(wpath);
        printf("  Path (length %zu): %s\n", wpath->length, s);
        free(s);
        path_free(wpath);
    } else {
        printf("  No path found.\n");
    }
}

static void demo_all_paths(Graph* g, Ids* ids) {
    print_section("7. All Paths (Alice -> Grace, max depth 6)");

    PathResult pr = graph_all_paths(g, ids->alice, ids->grace, 6);
    printf("  Found %zu path(s):\n", pr.count);
    for (size_t i = 0; i < pr.count; i++) {
        char* s = path_to_string(&pr.paths[i]);
        printf("    [%zu] %s\n", i + 1, s);
        free(s);
    }
    path_result_free(&pr);
}

static void demo_edges(Graph* g, Ids* ids) {
    print_section("8. Alice's Out-Edges");

    EdgeResult out = graph_get_out_edges(g, ids->alice);
    printf("  Out-edges (%zu):\n", out.count);
    for (size_t i = 0; i < out.count; i++) {
        Edge* e = out.edges[i];
        Node* tgt = graph_get_node(g, e->target);
        PropertyValue tname = node_get_property(tgt, "name");
        printf("    --[%s]--> %s\n",
               e->label,
               tname.type == PROP_TYPE_STRING ? tname.value.string_val : "?");
        prop_free(&tname);
    }
    edge_result_free(&out);
}

static void demo_json(Graph* g) {
    print_section("9. JSON Export (truncated to 500 chars)");

    char* json = graph_to_json(g);
    if (json) {
        int len = (int)strlen(json);
        printf("  (Total JSON length: %d bytes)\n", len);
        char snippet[501];
        int show = len < 500 ? len : 500;
        strncpy(snippet, json, show);
        snippet[show] = '\0';
        printf("  %s%s\n", snippet, len > 500 ? " ..." : "");
        free(json);
    }
}

static void demo_serialization(Graph* g) {
    print_section("10. Serialization (save → load → verify)");

    const char* path = "/tmp/social_network.tgdb";
    if (graph_save(g, path)) {
        printf("  Saved to %s\n", path);
        Graph* g2 = graph_load(path);
        if (g2) {
            printf("  Loaded:  nodes=%zu  edges=%zu\n",
                   graph_node_count(g2), graph_edge_count(g2));
            graph_destroy(g2);
        } else {
            printf("  Load failed!\n");
        }
        remove(path);
    } else {
        printf("  Save failed!\n");
    }
}

static void demo_memory_stats(Graph* g) {
    print_section("11. Memory Statistics");

    GraphStats s = graph_get_stats(g);
    printf("  Nodes      : %zu\n",   s.node_count);
    printf("  Edges      : %zu\n",   s.edge_count);
    printf("  Properties : %zu\n",   s.property_count);
    printf("  Node  mem  : %zu B\n", s.node_memory);
    printf("  Edge  mem  : %zu B\n", s.edge_memory);
    printf("  Index mem  : %zu B\n", s.index_memory);
    printf("  Total mem  : %.2f KB\n", s.total_memory / 1024.0);
}

/* -------- Main -------- */

int main(void) {
    printf("\n");
    printf("╔══════════════════════════════════════════════════╗\n");
    printf("║   TurboGraphDB  ·  Social Network Example App   ║\n");
    printf("╚══════════════════════════════════════════════════╝\n");

    Graph* g = graph_create("SocialNetwork");
    Ids ids = build_social_network(g);

    demo_overview(g);
    demo_label_query(g);
    demo_bfs(g, &ids);
    demo_dfs(g, &ids);
    demo_shortest_path(g, &ids);
    demo_all_paths(g, &ids);
    demo_edges(g, &ids);
    demo_json(g);
    demo_serialization(g);
    demo_memory_stats(g);

    graph_destroy(g);

    printf("\n  Done.\n\n");
    return 0;
}
