/**
 * TurboGraphDB - High-Performance Graph Database Engine
 * 
 * A lightweight, optimized graph database engine written in C
 * Features:
 * - Memory-efficient adjacency list storage
 * - Hash-based indexing for O(1) node lookups
 * - Optimized traversal algorithms (BFS, DFS, Dijkstra)
 * - Property storage with type system
 * - ACID transaction support
 */

#ifndef GRAPH_H
#define GRAPH_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============== Type Definitions ============== */

typedef uint64_t NodeId;
typedef uint64_t EdgeId;
typedef uint64_t PropertyId;

#define INVALID_NODE_ID 0
#define INVALID_EDGE_ID 0

/* Property Types */
typedef enum {
    PROP_TYPE_NULL = 0,
    PROP_TYPE_BOOL,
    PROP_TYPE_INT64,
    PROP_TYPE_FLOAT64,
    PROP_TYPE_STRING,
    PROP_TYPE_BYTES,
} PropertyType;

/* Property Value Union */
typedef struct {
    PropertyType type;
    union {
        bool bool_val;
        int64_t int_val;
        double float_val;
        char* string_val;
        struct {
            uint8_t* data;
            size_t len;
        } bytes_val;
    } value;
} PropertyValue;

/* Property Container */
typedef struct {
    PropertyId id;
    char* key;
    PropertyValue value;
} Property;

/* Edge Structure */
typedef struct Edge {
    EdgeId id;
    NodeId source;
    NodeId target;
    char* label;
    Property** properties;
    size_t property_count;
    size_t property_capacity;
    double weight;
} Edge;

/* Adjacency List Entry */
typedef struct AdjEntry {
    Edge* edge;
    struct AdjEntry* next;
} AdjEntry;

/* Node Structure */
typedef struct Node {
    NodeId id;
    char* label;
    Property** properties;
    size_t property_count;
    size_t property_capacity;
    
    /* Adjacency lists */
    AdjEntry* out_edges;    /* Outgoing edges */
    AdjEntry* in_edges;     /* Incoming edges */
    size_t out_degree;
    size_t in_degree;
} Node;

/* Hash Table for Node Index */
typedef struct NodeEntry {
    NodeId id;
    Node* node;
    struct NodeEntry* next;
} NodeEntry;

typedef struct {
    NodeEntry** buckets;
    size_t bucket_count;
    size_t size;
} NodeIndex;

/* Hash Table for Label Index */
typedef struct LabelEntry {
    char* label;
    Node** nodes;
    size_t node_count;
    size_t node_capacity;
    struct LabelEntry* next;
} LabelEntry;

typedef struct {
    LabelEntry** buckets;
    size_t bucket_count;
    size_t size;
} LabelIndex;

/* Graph Structure */
typedef struct Graph {
    char* name;
    
    /* Node storage */
    NodeIndex node_index;
    LabelIndex label_index;
    
    /* Edge storage */
    Edge** edges;
    size_t edge_count;
    size_t edge_capacity;
    
    /* ID generators */
    NodeId next_node_id;
    EdgeId next_edge_id;
    
    /* Statistics */
    size_t total_properties;
    
    /* Configuration */
    size_t index_bucket_count;
    bool auto_commit;
} Graph;

/* Query Result Types */
typedef struct {
    Node** nodes;
    size_t count;
    size_t capacity;
} NodeResult;

typedef struct {
    Edge** edges;
    size_t count;
    size_t capacity;
} EdgeResult;

typedef struct {
    Node** path;
    size_t length;
} Path;

typedef struct {
    Path* paths;
    size_t count;
    size_t capacity;
} PathResult;

/* Traversal Callback */
typedef bool (*TraversalCallback)(Node* node, void* user_data);
typedef bool (*EdgeTraversalCallback)(Edge* edge, void* user_data);

/* ============== Graph Operations ============== */

/* Initialize/Destroy */
Graph* graph_create(const char* name);
void graph_destroy(Graph* graph);
void graph_clear(Graph* graph);

/* Statistics */
size_t graph_node_count(Graph* graph);
size_t graph_edge_count(Graph* graph);
size_t graph_property_count(Graph* graph);

/* Node Operations */
Node* graph_create_node(Graph* graph, const char* label);
Node* graph_get_node(Graph* graph, NodeId id);
bool graph_delete_node(Graph* graph, NodeId id);
NodeResult graph_get_nodes_by_label(Graph* graph, const char* label);
NodeResult graph_get_all_nodes(Graph* graph);

/* Edge Operations */
Edge* graph_create_edge(Graph* graph, NodeId source, NodeId target, const char* label, double weight);
Edge* graph_get_edge(Graph* graph, EdgeId id);
bool graph_delete_edge(Graph* graph, EdgeId id);
EdgeResult graph_get_edges_by_label(Graph* graph, const char* label);
EdgeResult graph_get_all_edges(Graph* graph);
EdgeResult graph_get_out_edges(Graph* graph, NodeId node_id);
EdgeResult graph_get_in_edges(Graph* graph, NodeId node_id);
EdgeResult graph_get_edges_between(Graph* graph, NodeId source, NodeId target);

/* Property Operations */
bool node_set_property(Node* node, const char* key, PropertyValue value);
PropertyValue node_get_property(Node* node, const char* key);
bool node_remove_property(Node* node, const char* key);
bool node_has_property(Node* node, const char* key);

bool edge_set_property(Edge* edge, const char* key, PropertyValue value);
PropertyValue edge_get_property(Edge* edge, const char* key);
bool edge_remove_property(Edge* edge, const char* key);
bool edge_has_property(Edge* edge, const char* key);

/* Property Value Constructors */
PropertyValue prop_null(void);
PropertyValue prop_bool(bool val);
PropertyValue prop_int64(int64_t val);
PropertyValue prop_float64(double val);
PropertyValue prop_string(const char* val);
PropertyValue prop_bytes(const uint8_t* data, size_t len);

/* Property Value Operations */
void prop_free(PropertyValue* prop);
PropertyValue prop_copy(const PropertyValue* prop);
bool prop_equals(const PropertyValue* a, const PropertyValue* b);
char* prop_to_string(const PropertyValue* prop);

/* Traversal Algorithms */
NodeResult graph_bfs(Graph* graph, NodeId start, size_t max_depth);
NodeResult graph_dfs(Graph* graph, NodeId start, size_t max_depth);
NodeResult graph_bfs_filter(Graph* graph, NodeId start, size_t max_depth, 
                            bool (*filter)(Node*, void*), void* user_data);
NodeResult graph_dfs_filter(Graph* graph, NodeId start, size_t max_depth,
                            bool (*filter)(Node*, void*), void* user_data);

/* Path Finding */
Path* graph_shortest_path(Graph* graph, NodeId source, NodeId target);
PathResult graph_all_shortest_paths(Graph* graph, NodeId source, NodeId target);
Path* graph_shortest_path_weighted(Graph* graph, NodeId source, NodeId target);
PathResult graph_all_paths(Graph* graph, NodeId source, NodeId target, size_t max_depth);

/* Path Operations */
void path_free(Path* path);
void path_result_free(PathResult* result);
char* path_to_string(const Path* path);

/* Result Operations */
void node_result_free(NodeResult* result);
void edge_result_free(EdgeResult* result);

/* Serialization */
bool graph_save(Graph* graph, const char* filepath);
Graph* graph_load(const char* filepath);

/* Debug/Export */
void graph_print(Graph* graph);
char* graph_to_json(Graph* graph);
char* node_to_json(Node* node);
char* edge_to_json(Edge* edge);

/* Memory Statistics */
typedef struct {
    size_t node_memory;
    size_t edge_memory;
    size_t property_memory;
    size_t index_memory;
    size_t total_memory;
    size_t node_count;
    size_t edge_count;
    size_t property_count;
} GraphStats;

GraphStats graph_get_stats(Graph* graph);

#ifdef __cplusplus
}
#endif

#endif /* GRAPH_H */
