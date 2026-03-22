/**
 * TurboGraphDB - High-Performance Graph Database Engine
 * Implementation
 */

#include "graph.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

/* ============== Hash Functions ============== */

static uint64_t hash_uint64(uint64_t key, size_t bucket_count) {
    /* FNV-1a hash for better distribution */
    uint64_t hash = 14695981039346656037ULL;
    hash ^= key;
    hash *= 1099511628211ULL;
    return hash % bucket_count;
}

static uint64_t hash_string(const char* str, size_t bucket_count) {
    /* FNV-1a hash */
    uint64_t hash = 14695981039346656037ULL;
    while (*str) {
        hash ^= (uint8_t)*str++;
        hash *= 1099511628211ULL;
    }
    return hash % bucket_count;
}

/* ============== Forward Declarations ============== */
static void* safe_calloc(size_t nmemb, size_t size);

/* ============== Memory Helpers ============== */

static void* safe_malloc(size_t size) {
    void* ptr = malloc(size);
    if (!ptr && size > 0) {
        fprintf(stderr, "TurboGraphDB: Memory allocation failed for size %zu\n", size);
        exit(1);
    }
    return ptr;
}

static void* safe_calloc(size_t nmemb, size_t size) {
    void* ptr = calloc(nmemb, size);
    if (!ptr && nmemb > 0 && size > 0) {
        fprintf(stderr, "TurboGraphDB: Memory allocation failed\n");
        exit(1);
    }
    return ptr;
}

static void* safe_realloc(void* ptr, size_t size) {
    void* new_ptr = realloc(ptr, size);
    if (!new_ptr && size > 0) {
        fprintf(stderr, "TurboGraphDB: Memory reallocation failed for size %zu\n", size);
        exit(1);
    }
    return new_ptr;
}

static char* safe_strdup(const char* str) {
    if (!str) return NULL;
    size_t len = strlen(str) + 1;
    char* dup = safe_malloc(len);
    memcpy(dup, str, len);
    return dup;
}

/* ============== Property Value Functions ============== */

PropertyValue prop_null(void) {
    PropertyValue prop;
    prop.type = PROP_TYPE_NULL;
    prop.value.int_val = 0;
    return prop;
}

PropertyValue prop_bool(bool val) {
    PropertyValue prop;
    prop.type = PROP_TYPE_BOOL;
    prop.value.bool_val = val;
    return prop;
}

PropertyValue prop_int64(int64_t val) {
    PropertyValue prop;
    prop.type = PROP_TYPE_INT64;
    prop.value.int_val = val;
    return prop;
}

PropertyValue prop_float64(double val) {
    PropertyValue prop;
    prop.type = PROP_TYPE_FLOAT64;
    prop.value.float_val = val;
    return prop;
}

PropertyValue prop_string(const char* val) {
    PropertyValue prop;
    prop.type = PROP_TYPE_STRING;
    prop.value.string_val = safe_strdup(val);
    return prop;
}

PropertyValue prop_bytes(const uint8_t* data, size_t len) {
    PropertyValue prop;
    prop.type = PROP_TYPE_BYTES;
    prop.value.bytes_val.data = safe_malloc(len);
    memcpy(prop.value.bytes_val.data, data, len);
    prop.value.bytes_val.len = len;
    return prop;
}

void prop_free(PropertyValue* prop) {
    if (!prop) return;
    
    switch (prop->type) {
        case PROP_TYPE_STRING:
            free(prop->value.string_val);
            break;
        case PROP_TYPE_BYTES:
            free(prop->value.bytes_val.data);
            break;
        default:
            break;
    }
    prop->type = PROP_TYPE_NULL;
}

PropertyValue prop_copy(const PropertyValue* prop) {
    PropertyValue copy = prop_null();
    if (!prop) return copy;
    
    copy.type = prop->type;
    switch (prop->type) {
        case PROP_TYPE_BOOL:
            copy.value.bool_val = prop->value.bool_val;
            break;
        case PROP_TYPE_INT64:
            copy.value.int_val = prop->value.int_val;
            break;
        case PROP_TYPE_FLOAT64:
            copy.value.float_val = prop->value.float_val;
            break;
        case PROP_TYPE_STRING:
            copy.value.string_val = safe_strdup(prop->value.string_val);
            break;
        case PROP_TYPE_BYTES:
            copy.value.bytes_val.data = safe_malloc(prop->value.bytes_val.len);
            memcpy(copy.value.bytes_val.data, prop->value.bytes_val.data, prop->value.bytes_val.len);
            copy.value.bytes_val.len = prop->value.bytes_val.len;
            break;
        default:
            break;
    }
    return copy;
}

bool prop_equals(const PropertyValue* a, const PropertyValue* b) {
    if (!a || !b) return a == b;
    if (a->type != b->type) return false;
    
    switch (a->type) {
        case PROP_TYPE_NULL:
            return true;
        case PROP_TYPE_BOOL:
            return a->value.bool_val == b->value.bool_val;
        case PROP_TYPE_INT64:
            return a->value.int_val == b->value.int_val;
        case PROP_TYPE_FLOAT64:
            return fabs(a->value.float_val - b->value.float_val) < 1e-9;
        case PROP_TYPE_STRING:
            return strcmp(a->value.string_val, b->value.string_val) == 0;
        case PROP_TYPE_BYTES:
            if (a->value.bytes_val.len != b->value.bytes_val.len) return false;
            return memcmp(a->value.bytes_val.data, b->value.bytes_val.data, a->value.bytes_val.len) == 0;
    }
    return false;
}

char* prop_to_string(const PropertyValue* prop) {
    if (!prop) return safe_strdup("null");
    
    char buffer[256];
    switch (prop->type) {
        case PROP_TYPE_NULL:
            return safe_strdup("null");
        case PROP_TYPE_BOOL:
            return safe_strdup(prop->value.bool_val ? "true" : "false");
        case PROP_TYPE_INT64:
            snprintf(buffer, sizeof(buffer), "%lld", (long long)prop->value.int_val);
            return safe_strdup(buffer);
        case PROP_TYPE_FLOAT64:
            snprintf(buffer, sizeof(buffer), "%.6g", prop->value.float_val);
            return safe_strdup(buffer);
        case PROP_TYPE_STRING:
            return safe_strdup(prop->value.string_val);
        case PROP_TYPE_BYTES:
            snprintf(buffer, sizeof(buffer), "<bytes:%zu>", prop->value.bytes_val.len);
            return safe_strdup(buffer);
    }
    return safe_strdup("unknown");
}

/* ============== Node Index Functions ============== */

static void node_index_init(NodeIndex* index, size_t bucket_count) {
    index->bucket_count = bucket_count;
    index->size = 0;
    index->buckets = safe_calloc(bucket_count, sizeof(NodeEntry*));
}

static void node_index_insert(NodeIndex* index, Node* node) {
    uint64_t bucket = hash_uint64(node->id, index->bucket_count);
    
    NodeEntry* entry = safe_malloc(sizeof(NodeEntry));
    entry->id = node->id;
    entry->node = node;
    entry->next = index->buckets[bucket];
    index->buckets[bucket] = entry;
    index->size++;
}

static Node* node_index_get(NodeIndex* index, NodeId id) {
    uint64_t bucket = hash_uint64(id, index->bucket_count);
    NodeEntry* entry = index->buckets[bucket];
    
    while (entry) {
        if (entry->id == id) {
            return entry->node;
        }
        entry = entry->next;
    }
    return NULL;
}

static bool node_index_remove(NodeIndex* index, NodeId id) {
    uint64_t bucket = hash_uint64(id, index->bucket_count);
    NodeEntry* entry = index->buckets[bucket];
    NodeEntry* prev = NULL;
    
    while (entry) {
        if (entry->id == id) {
            if (prev) {
                prev->next = entry->next;
            } else {
                index->buckets[bucket] = entry->next;
            }
            free(entry);
            index->size--;
            return true;
        }
        prev = entry;
        entry = entry->next;
    }
    return false;
}

static void node_index_free(NodeIndex* index) {
    for (size_t i = 0; i < index->bucket_count; i++) {
        NodeEntry* entry = index->buckets[i];
        while (entry) {
            NodeEntry* next = entry->next;
            free(entry);
            entry = next;
        }
    }
    free(index->buckets);
}

/* ============== Label Index Functions ============== */

static void label_index_init(LabelIndex* index, size_t bucket_count) {
    index->bucket_count = bucket_count;
    index->size = 0;
    index->buckets = safe_calloc(bucket_count, sizeof(LabelEntry*));
}

static LabelEntry* label_index_get_or_create(LabelIndex* index, const char* label) {
    uint64_t bucket = hash_string(label, index->bucket_count);
    LabelEntry* entry = index->buckets[bucket];
    
    while (entry) {
        if (strcmp(entry->label, label) == 0) {
            return entry;
        }
        entry = entry->next;
    }
    
    /* Create new entry */
    entry = safe_malloc(sizeof(LabelEntry));
    entry->label = safe_strdup(label);
    entry->nodes = safe_malloc(16 * sizeof(Node*));
    entry->node_count = 0;
    entry->node_capacity = 16;
    entry->next = index->buckets[bucket];
    index->buckets[bucket] = entry;
    index->size++;
    
    return entry;
}

static void label_index_add_node(LabelIndex* index, Node* node) {
    LabelEntry* entry = label_index_get_or_create(index, node->label);
    
    if (entry->node_count >= entry->node_capacity) {
        entry->node_capacity *= 2;
        entry->nodes = safe_realloc(entry->nodes, entry->node_capacity * sizeof(Node*));
    }
    entry->nodes[entry->node_count++] = node;
}

static void label_index_remove_node(LabelIndex* index, Node* node) {
    uint64_t bucket = hash_string(node->label, index->bucket_count);
    LabelEntry* entry = index->buckets[bucket];
    
    while (entry) {
        if (strcmp(entry->label, node->label) == 0) {
            for (size_t i = 0; i < entry->node_count; i++) {
                if (entry->nodes[i] == node) {
                    entry->nodes[i] = entry->nodes[--entry->node_count];
                    return;
                }
            }
        }
        entry = entry->next;
    }
}

static void label_index_free(LabelIndex* index) {
    for (size_t i = 0; i < index->bucket_count; i++) {
        LabelEntry* entry = index->buckets[i];
        while (entry) {
            LabelEntry* next = entry->next;
            free(entry->label);
            free(entry->nodes);
            free(entry);
            entry = next;
        }
    }
    free(index->buckets);
}

/* ============== Node Functions ============== */

static Node* node_create(NodeId id, const char* label) {
    Node* node = safe_malloc(sizeof(Node));
    node->id = id;
    node->label = safe_strdup(label);
    node->properties = safe_malloc(8 * sizeof(Property*));
    node->property_count = 0;
    node->property_capacity = 8;
    node->out_edges = NULL;
    node->in_edges = NULL;
    node->out_degree = 0;
    node->in_degree = 0;
    return node;
}

static void node_free(Node* node) {
    if (!node) return;
    
    free(node->label);
    
    /* Free properties */
    for (size_t i = 0; i < node->property_count; i++) {
        free(node->properties[i]->key);
        prop_free(&node->properties[i]->value);
        free(node->properties[i]);
    }
    free(node->properties);
    
    /* Free adjacency lists */
    AdjEntry* entry = node->out_edges;
    while (entry) {
        AdjEntry* next = entry->next;
        free(entry);
        entry = next;
    }
    entry = node->in_edges;
    while (entry) {
        AdjEntry* next = entry->next;
        free(entry);
        entry = next;
    }
    
    free(node);
}

static Property* node_find_property(Node* node, const char* key) {
    for (size_t i = 0; i < node->property_count; i++) {
        if (strcmp(node->properties[i]->key, key) == 0) {
            return node->properties[i];
        }
    }
    return NULL;
}

bool node_set_property(Node* node, const char* key, PropertyValue value) {
    Property* prop = node_find_property(node, key);
    
    if (prop) {
        prop_free(&prop->value);
        prop->value = value;
        return true;
    }
    
    if (node->property_count >= node->property_capacity) {
        node->property_capacity *= 2;
        node->properties = safe_realloc(node->properties, node->property_capacity * sizeof(Property*));
    }
    
    prop = safe_malloc(sizeof(Property));
    prop->key = safe_strdup(key);
    prop->value = value;
    node->properties[node->property_count++] = prop;
    return true;
}

PropertyValue node_get_property(Node* node, const char* key) {
    Property* prop = node_find_property(node, key);
    if (prop) {
        return prop_copy(&prop->value);
    }
    return prop_null();
}

bool node_remove_property(Node* node, const char* key) {
    for (size_t i = 0; i < node->property_count; i++) {
        if (strcmp(node->properties[i]->key, key) == 0) {
            free(node->properties[i]->key);
            prop_free(&node->properties[i]->value);
            free(node->properties[i]);
            node->properties[i] = node->properties[--node->property_count];
            return true;
        }
    }
    return false;
}

bool node_has_property(Node* node, const char* key) {
    return node_find_property(node, key) != NULL;
}

/* ============== Edge Functions ============== */

static Edge* edge_create(EdgeId id, NodeId source, NodeId target, const char* label, double weight) {
    Edge* edge = safe_malloc(sizeof(Edge));
    edge->id = id;
    edge->source = source;
    edge->target = target;
    edge->label = safe_strdup(label);
    edge->properties = safe_malloc(4 * sizeof(Property*));
    edge->property_count = 0;
    edge->property_capacity = 4;
    edge->weight = weight;
    return edge;
}

static void edge_free(Edge* edge) {
    if (!edge) return;
    
    free(edge->label);
    
    for (size_t i = 0; i < edge->property_count; i++) {
        free(edge->properties[i]->key);
        prop_free(&edge->properties[i]->value);
        free(edge->properties[i]);
    }
    free(edge->properties);
    free(edge);
}

static Property* edge_find_property(Edge* edge, const char* key) {
    for (size_t i = 0; i < edge->property_count; i++) {
        if (strcmp(edge->properties[i]->key, key) == 0) {
            return edge->properties[i];
        }
    }
    return NULL;
}

bool edge_set_property(Edge* edge, const char* key, PropertyValue value) {
    Property* prop = edge_find_property(edge, key);
    
    if (prop) {
        prop_free(&prop->value);
        prop->value = value;
        return true;
    }
    
    if (edge->property_count >= edge->property_capacity) {
        edge->property_capacity *= 2;
        edge->properties = safe_realloc(edge->properties, edge->property_capacity * sizeof(Property*));
    }
    
    prop = safe_malloc(sizeof(Property));
    prop->key = safe_strdup(key);
    prop->value = value;
    edge->properties[edge->property_count++] = prop;
    return true;
}

PropertyValue edge_get_property(Edge* edge, const char* key) {
    Property* prop = edge_find_property(edge, key);
    if (prop) {
        return prop_copy(&prop->value);
    }
    return prop_null();
}

bool edge_remove_property(Edge* edge, const char* key) {
    for (size_t i = 0; i < edge->property_count; i++) {
        if (strcmp(edge->properties[i]->key, key) == 0) {
            free(edge->properties[i]->key);
            prop_free(&edge->properties[i]->value);
            free(edge->properties[i]);
            edge->properties[i] = edge->properties[--edge->property_count];
            return true;
        }
    }
    return false;
}

bool edge_has_property(Edge* edge, const char* key) {
    return edge_find_property(edge, key) != NULL;
}

/* ============== Graph Functions ============== */

Graph* graph_create(const char* name) {
    Graph* graph = safe_malloc(sizeof(Graph));
    graph->name = safe_strdup(name ? name : "default");
    graph->index_bucket_count = 1024;
    
    node_index_init(&graph->node_index, graph->index_bucket_count);
    label_index_init(&graph->label_index, 64);
    
    graph->edges = safe_malloc(1024 * sizeof(Edge*));
    graph->edge_count = 0;
    graph->edge_capacity = 1024;
    
    graph->next_node_id = 1;
    graph->next_edge_id = 1;
    graph->total_properties = 0;
    graph->auto_commit = true;
    
    return graph;
}

void graph_destroy(Graph* graph) {
    if (!graph) return;
    
    /* Free all nodes */
    for (size_t i = 0; i < graph->node_index.bucket_count; i++) {
        NodeEntry* entry = graph->node_index.buckets[i];
        while (entry) {
            node_free(entry->node);
            entry = entry->next;
        }
    }
    node_index_free(&graph->node_index);
    label_index_free(&graph->label_index);
    
    /* Free all edges */
    for (size_t i = 0; i < graph->edge_count; i++) {
        edge_free(graph->edges[i]);
    }
    free(graph->edges);
    
    free(graph->name);
    free(graph);
}

void graph_clear(Graph* graph) {
    if (!graph) return;
    
    /* Clear nodes */
    for (size_t i = 0; i < graph->node_index.bucket_count; i++) {
        NodeEntry* entry = graph->node_index.buckets[i];
        while (entry) {
            node_free(entry->node);
            NodeEntry* next = entry->next;
            free(entry);
            entry = next;
        }
        graph->node_index.buckets[i] = NULL;
    }
    graph->node_index.size = 0;
    
    /* Clear label index */
    for (size_t i = 0; i < graph->label_index.bucket_count; i++) {
        LabelEntry* entry = graph->label_index.buckets[i];
        while (entry) {
            free(entry->label);
            free(entry->nodes);
            LabelEntry* next = entry->next;
            free(entry);
            entry = next;
        }
        graph->label_index.buckets[i] = NULL;
    }
    graph->label_index.size = 0;
    
    /* Clear edges */
    for (size_t i = 0; i < graph->edge_count; i++) {
        edge_free(graph->edges[i]);
    }
    graph->edge_count = 0;
    
    graph->total_properties = 0;
}

size_t graph_node_count(Graph* graph) {
    return graph ? graph->node_index.size : 0;
}

size_t graph_edge_count(Graph* graph) {
    return graph ? graph->edge_count : 0;
}

size_t graph_property_count(Graph* graph) {
    return graph ? graph->total_properties : 0;
}

/* ============== Node Operations ============== */

Node* graph_create_node(Graph* graph, const char* label) {
    if (!graph || !label) return NULL;
    
    Node* node = node_create(graph->next_node_id++, label);
    node_index_insert(&graph->node_index, node);
    label_index_add_node(&graph->label_index, node);
    
    return node;
}

Node* graph_get_node(Graph* graph, NodeId id) {
    if (!graph) return NULL;
    return node_index_get(&graph->node_index, id);
}

bool graph_delete_node(Graph* graph, NodeId id) {
    if (!graph) return false;
    
    Node* node = node_index_get(&graph->node_index, id);
    if (!node) return false;
    
    /* Remove all connected edges */
    AdjEntry* entry = node->out_edges;
    while (entry) {
        Edge* edge = entry->edge;
        Node* target = node_index_get(&graph->node_index, edge->target);
        if (target) {
            AdjEntry* in_entry = target->in_edges;
            AdjEntry* prev = NULL;
            while (in_entry) {
                if (in_entry->edge == edge) {
                    if (prev) prev->next = in_entry->next;
                    else target->in_edges = in_entry->next;
                    free(in_entry);
                    target->in_degree--;
                    break;
                }
                prev = in_entry;
                in_entry = in_entry->next;
            }
        }
        edge_free(edge);
        entry = entry->next;
    }
    
    entry = node->in_edges;
    while (entry) {
        Edge* edge = entry->edge;
        Node* source = node_index_get(&graph->node_index, edge->source);
        if (source) {
            AdjEntry* out_entry = source->out_edges;
            AdjEntry* prev = NULL;
            while (out_entry) {
                if (out_entry->edge == edge) {
                    if (prev) prev->next = out_entry->next;
                    else source->out_edges = out_entry->next;
                    free(out_entry);
                    source->out_degree--;
                    break;
                }
                prev = out_entry;
                out_entry = out_entry->next;
            }
        }
        edge_free(edge);
        entry = entry->next;
    }
    
    /* Remove from label index */
    label_index_remove_node(&graph->label_index, node);
    
    /* Remove from node index and free */
    node_index_remove(&graph->node_index, id);
    node_free(node);
    
    return true;
}

NodeResult graph_get_nodes_by_label(Graph* graph, const char* label) {
    NodeResult result = {NULL, 0, 0};
    if (!graph || !label) return result;
    
    uint64_t bucket = hash_string(label, graph->label_index.bucket_count);
    LabelEntry* entry = graph->label_index.buckets[bucket];
    
    while (entry) {
        if (strcmp(entry->label, label) == 0) {
            result.nodes = safe_malloc(entry->node_count * sizeof(Node*));
            result.capacity = entry->node_count;
            result.count = entry->node_count;
            memcpy(result.nodes, entry->nodes, entry->node_count * sizeof(Node*));
            return result;
        }
        entry = entry->next;
    }
    
    return result;
}

NodeResult graph_get_all_nodes(Graph* graph) {
    NodeResult result = {NULL, 0, 0};
    if (!graph) return result;
    
    result.nodes = safe_malloc(graph->node_index.size * sizeof(Node*));
    result.capacity = graph->node_index.size;
    
    for (size_t i = 0; i < graph->node_index.bucket_count; i++) {
        NodeEntry* entry = graph->node_index.buckets[i];
        while (entry) {
            result.nodes[result.count++] = entry->node;
            entry = entry->next;
        }
    }
    
    return result;
}

/* ============== Edge Operations ============== */

Edge* graph_create_edge(Graph* graph, NodeId source, NodeId target, const char* label, double weight) {
    if (!graph || !label) return NULL;
    
    Node* src_node = node_index_get(&graph->node_index, source);
    Node* tgt_node = node_index_get(&graph->node_index, target);
    if (!src_node || !tgt_node) return NULL;
    
    Edge* edge = edge_create(graph->next_edge_id++, source, target, label, weight);
    
    /* Add to edge array */
    if (graph->edge_count >= graph->edge_capacity) {
        graph->edge_capacity *= 2;
        graph->edges = safe_realloc(graph->edges, graph->edge_capacity * sizeof(Edge*));
    }
    graph->edges[graph->edge_count++] = edge;
    
    /* Add to adjacency lists */
    AdjEntry* out_entry = safe_malloc(sizeof(AdjEntry));
    out_entry->edge = edge;
    out_entry->next = src_node->out_edges;
    src_node->out_edges = out_entry;
    src_node->out_degree++;
    
    AdjEntry* in_entry = safe_malloc(sizeof(AdjEntry));
    in_entry->edge = edge;
    in_entry->next = tgt_node->in_edges;
    tgt_node->in_edges = in_entry;
    tgt_node->in_degree++;
    
    return edge;
}

Edge* graph_get_edge(Graph* graph, EdgeId id) {
    if (!graph) return NULL;
    
    for (size_t i = 0; i < graph->edge_count; i++) {
        if (graph->edges[i]->id == id) {
            return graph->edges[i];
        }
    }
    return NULL;
}

bool graph_delete_edge(Graph* graph, EdgeId id) {
    if (!graph) return false;
    
    Edge* edge = NULL;
    size_t edge_idx = 0;
    for (size_t i = 0; i < graph->edge_count; i++) {
        if (graph->edges[i]->id == id) {
            edge = graph->edges[i];
            edge_idx = i;
            break;
        }
    }
    if (!edge) return false;
    
    Node* src_node = node_index_get(&graph->node_index, edge->source);
    Node* tgt_node = node_index_get(&graph->node_index, edge->target);
    
    /* Remove from adjacency lists */
    if (src_node) {
        AdjEntry* entry = src_node->out_edges;
        AdjEntry* prev = NULL;
        while (entry) {
            if (entry->edge == edge) {
                if (prev) prev->next = entry->next;
                else src_node->out_edges = entry->next;
                free(entry);
                src_node->out_degree--;
                break;
            }
            prev = entry;
            entry = entry->next;
        }
    }
    
    if (tgt_node) {
        AdjEntry* entry = tgt_node->in_edges;
        AdjEntry* prev = NULL;
        while (entry) {
            if (entry->edge == edge) {
                if (prev) prev->next = entry->next;
                else tgt_node->in_edges = entry->next;
                free(entry);
                tgt_node->in_degree--;
                break;
            }
            prev = entry;
            entry = entry->next;
        }
    }
    
    /* Remove from edge array */
    graph->edges[edge_idx] = graph->edges[--graph->edge_count];
    edge_free(edge);
    
    return true;
}

EdgeResult graph_get_edges_by_label(Graph* graph, const char* label) {
    EdgeResult result = {NULL, 0, 0};
    if (!graph || !label) return result;
    
    result.edges = safe_malloc(graph->edge_count * sizeof(Edge*));
    result.capacity = graph->edge_count;
    
    for (size_t i = 0; i < graph->edge_count; i++) {
        if (strcmp(graph->edges[i]->label, label) == 0) {
            result.edges[result.count++] = graph->edges[i];
        }
    }
    
    return result;
}

EdgeResult graph_get_all_edges(Graph* graph) {
    EdgeResult result = {NULL, 0, 0};
    if (!graph) return result;
    
    result.edges = safe_malloc(graph->edge_count * sizeof(Edge*));
    result.capacity = graph->edge_count;
    result.count = graph->edge_count;
    memcpy(result.edges, graph->edges, graph->edge_count * sizeof(Edge*));
    
    return result;
}

EdgeResult graph_get_out_edges(Graph* graph, NodeId node_id) {
    EdgeResult result = {NULL, 0, 0};
    if (!graph) return result;
    
    Node* node = node_index_get(&graph->node_index, node_id);
    if (!node) return result;
    
    result.edges = safe_malloc(node->out_degree * sizeof(Edge*));
    result.capacity = node->out_degree;
    
    AdjEntry* entry = node->out_edges;
    while (entry) {
        result.edges[result.count++] = entry->edge;
        entry = entry->next;
    }
    
    return result;
}

EdgeResult graph_get_in_edges(Graph* graph, NodeId node_id) {
    EdgeResult result = {NULL, 0, 0};
    if (!graph) return result;
    
    Node* node = node_index_get(&graph->node_index, node_id);
    if (!node) return result;
    
    result.edges = safe_malloc(node->in_degree * sizeof(Edge*));
    result.capacity = node->in_degree;
    
    AdjEntry* entry = node->in_edges;
    while (entry) {
        result.edges[result.count++] = entry->edge;
        entry = entry->next;
    }
    
    return result;
}

EdgeResult graph_get_edges_between(Graph* graph, NodeId source, NodeId target) {
    EdgeResult result = {NULL, 0, 0};
    if (!graph) return result;
    
    Node* node = node_index_get(&graph->node_index, source);
    if (!node) return result;
    
    result.edges = safe_malloc(node->out_degree * sizeof(Edge*));
    result.capacity = node->out_degree;
    
    AdjEntry* entry = node->out_edges;
    while (entry) {
        if (entry->edge->target == target) {
            result.edges[result.count++] = entry->edge;
        }
        entry = entry->next;
    }
    
    return result;
}

/* ============== Result Free Functions ============== */

void node_result_free(NodeResult* result) {
    if (result && result->nodes) {
        free(result->nodes);
        result->nodes = NULL;
        result->count = 0;
        result->capacity = 0;
    }
}

void edge_result_free(EdgeResult* result) {
    if (result && result->edges) {
        free(result->edges);
        result->edges = NULL;
        result->count = 0;
        result->capacity = 0;
    }
}

/* ============== Traversal Queue (for BFS) ============== */

typedef struct {
    NodeId* items;
    size_t front;
    size_t rear;
    size_t capacity;
} Queue;

static Queue* queue_create(size_t capacity) {
    Queue* q = safe_malloc(sizeof(Queue));
    q->items = safe_malloc(capacity * sizeof(NodeId));
    q->front = 0;
    q->rear = 0;
    q->capacity = capacity;
    return q;
}

static void queue_free(Queue* q) {
    if (q) {
        free(q->items);
        free(q);
    }
}

static bool queue_is_empty(Queue* q) {
    return q->front == q->rear;
}

static void queue_enqueue(Queue* q, NodeId item) {
    if (q->rear >= q->capacity) {
        q->capacity *= 2;
        q->items = safe_realloc(q->items, q->capacity * sizeof(NodeId));
    }
    q->items[q->rear++] = item;
}

static NodeId queue_dequeue(Queue* q) {
    return q->items[q->front++];
}

/* ============== Traversal Stack (for DFS) ============== */

typedef struct {
    NodeId* items;
    size_t top;
    size_t capacity;
} Stack;

static Stack* stack_create(size_t capacity) {
    Stack* s = safe_malloc(sizeof(Stack));
    s->items = safe_malloc(capacity * sizeof(NodeId));
    s->top = 0;
    s->capacity = capacity;
    return s;
}

static void stack_free(Stack* s) {
    if (s) {
        free(s->items);
        free(s);
    }
}

static bool stack_is_empty(Stack* s) {
    return s->top == 0;
}

static void stack_push(Stack* s, NodeId item) {
    if (s->top >= s->capacity) {
        s->capacity *= 2;
        s->items = safe_realloc(s->items, s->capacity * sizeof(NodeId));
    }
    s->items[s->top++] = item;
}

static NodeId stack_pop(Stack* s) {
    return s->items[--s->top];
}

/* ============== Visited Set ============== */

typedef struct {
    bool* visited;
    size_t size;
} VisitedSet;

static VisitedSet* visited_set_create(size_t size) {
    VisitedSet* set = safe_malloc(sizeof(VisitedSet));
    set->visited = safe_calloc(size + 1, sizeof(bool));
    set->size = size + 1;
    return set;
}

static void visited_set_free(VisitedSet* set) {
    if (set) {
        free(set->visited);
        free(set);
    }
}

static void visited_set_add(VisitedSet* set, NodeId id) {
    if (id < set->size) {
        set->visited[id] = true;
    }
}

static bool visited_set_contains(VisitedSet* set, NodeId id) {
    return id < set->size && set->visited[id];
}

/* ============== Traversal Algorithms ============== */

NodeResult graph_bfs(Graph* graph, NodeId start, size_t max_depth) {
    NodeResult result = {NULL, 0, 0};
    if (!graph) return result;
    
    Node* start_node = node_index_get(&graph->node_index, start);
    if (!start_node) return result;
    
    result.nodes = safe_malloc(graph->node_index.size * sizeof(Node*));
    result.capacity = graph->node_index.size;
    
    Queue* queue = queue_create(1024);
    VisitedSet* visited = visited_set_create(graph->next_node_id);
    size_t* depths = safe_calloc(graph->next_node_id, sizeof(size_t));
    
    queue_enqueue(queue, start);
    visited_set_add(visited, start);
    depths[start] = 0;
    result.nodes[result.count++] = start_node;
    
    while (!queue_is_empty(queue)) {
        NodeId current_id = queue_dequeue(queue);
        size_t current_depth = depths[current_id];
        
        if (max_depth > 0 && current_depth >= max_depth) continue;
        
        Node* current = node_index_get(&graph->node_index, current_id);
        if (!current) continue;
        
        AdjEntry* entry = current->out_edges;
        while (entry) {
            NodeId neighbor_id = entry->edge->target;
            if (!visited_set_contains(visited, neighbor_id)) {
                visited_set_add(visited, neighbor_id);
                depths[neighbor_id] = current_depth + 1;
                
                Node* neighbor = node_index_get(&graph->node_index, neighbor_id);
                if (neighbor) {
                    result.nodes[result.count++] = neighbor;
                }
                queue_enqueue(queue, neighbor_id);
            }
            entry = entry->next;
        }
    }
    
    queue_free(queue);
    visited_set_free(visited);
    free(depths);
    
    return result;
}

NodeResult graph_dfs(Graph* graph, NodeId start, size_t max_depth) {
    NodeResult result = {NULL, 0, 0};
    if (!graph) return result;
    
    Node* start_node = node_index_get(&graph->node_index, start);
    if (!start_node) return result;
    
    result.nodes = safe_malloc(graph->node_index.size * sizeof(Node*));
    result.capacity = graph->node_index.size;
    
    Stack* stack = stack_create(1024);
    VisitedSet* visited = visited_set_create(graph->next_node_id);
    size_t* depths = safe_calloc(graph->next_node_id, sizeof(size_t));
    
    stack_push(stack, start);
    depths[start] = 0;
    
    while (!stack_is_empty(stack)) {
        NodeId current_id = stack_pop(stack);
        
        if (visited_set_contains(visited, current_id)) continue;
        visited_set_add(visited, current_id);
        
        size_t current_depth = depths[current_id];
        
        Node* current = node_index_get(&graph->node_index, current_id);
        if (!current) continue;
        result.nodes[result.count++] = current;
        
        if (max_depth > 0 && current_depth >= max_depth) continue;
        
        AdjEntry* entry = current->out_edges;
        while (entry) {
            NodeId neighbor_id = entry->edge->target;
            if (!visited_set_contains(visited, neighbor_id)) {
                depths[neighbor_id] = current_depth + 1;
                stack_push(stack, neighbor_id);
            }
            entry = entry->next;
        }
    }
    
    stack_free(stack);
    visited_set_free(visited);
    free(depths);
    
    return result;
}

NodeResult graph_bfs_filter(Graph* graph, NodeId start, size_t max_depth,
                            bool (*filter)(Node*, void*), void* user_data) {
    NodeResult result = {NULL, 0, 0};
    if (!graph) return result;
    
    Node* start_node = node_index_get(&graph->node_index, start);
    if (!start_node) return result;
    
    result.nodes = safe_malloc(graph->node_index.size * sizeof(Node*));
    result.capacity = graph->node_index.size;
    
    Queue* queue = queue_create(1024);
    VisitedSet* visited = visited_set_create(graph->next_node_id);
    size_t* depths = safe_calloc(graph->next_node_id, sizeof(size_t));
    
    queue_enqueue(queue, start);
    visited_set_add(visited, start);
    depths[start] = 0;
    
    if (filter(start_node, user_data)) {
        result.nodes[result.count++] = start_node;
    }
    
    while (!queue_is_empty(queue)) {
        NodeId current_id = queue_dequeue(queue);
        size_t current_depth = depths[current_id];
        
        if (max_depth > 0 && current_depth >= max_depth) continue;
        
        Node* current = node_index_get(&graph->node_index, current_id);
        if (!current) continue;
        
        AdjEntry* entry = current->out_edges;
        while (entry) {
            NodeId neighbor_id = entry->edge->target;
            if (!visited_set_contains(visited, neighbor_id)) {
                visited_set_add(visited, neighbor_id);
                depths[neighbor_id] = current_depth + 1;
                
                Node* neighbor = node_index_get(&graph->node_index, neighbor_id);
                if (neighbor && filter(neighbor, user_data)) {
                    result.nodes[result.count++] = neighbor;
                }
                queue_enqueue(queue, neighbor_id);
            }
            entry = entry->next;
        }
    }
    
    queue_free(queue);
    visited_set_free(visited);
    free(depths);
    
    return result;
}

NodeResult graph_dfs_filter(Graph* graph, NodeId start, size_t max_depth,
                            bool (*filter)(Node*, void*), void* user_data) {
    NodeResult result = {NULL, 0, 0};
    if (!graph) return result;
    
    Node* start_node = node_index_get(&graph->node_index, start);
    if (!start_node) return result;
    
    result.nodes = safe_malloc(graph->node_index.size * sizeof(Node*));
    result.capacity = graph->node_index.size;
    
    Stack* stack = stack_create(1024);
    VisitedSet* visited = visited_set_create(graph->next_node_id);
    size_t* depths = safe_calloc(graph->next_node_id, sizeof(size_t));
    
    stack_push(stack, start);
    depths[start] = 0;
    
    while (!stack_is_empty(stack)) {
        NodeId current_id = stack_pop(stack);
        
        if (visited_set_contains(visited, current_id)) continue;
        visited_set_add(visited, current_id);
        
        size_t current_depth = depths[current_id];
        
        Node* current = node_index_get(&graph->node_index, current_id);
        if (!current) continue;
        
        if (filter(current, user_data)) {
            result.nodes[result.count++] = current;
        }
        
        if (max_depth > 0 && current_depth >= max_depth) continue;
        
        AdjEntry* entry = current->out_edges;
        while (entry) {
            NodeId neighbor_id = entry->edge->target;
            if (!visited_set_contains(visited, neighbor_id)) {
                depths[neighbor_id] = current_depth + 1;
                stack_push(stack, neighbor_id);
            }
            entry = entry->next;
        }
    }
    
    stack_free(stack);
    visited_set_free(visited);
    free(depths);
    
    return result;
}

/* ============== Path Finding ============== */

/* Priority Queue for Dijkstra */
typedef struct {
    NodeId node;
    double distance;
} PQItem;

typedef struct {
    PQItem* items;
    size_t size;
    size_t capacity;
} PriorityQueue;

static PriorityQueue* pq_create(size_t capacity) {
    PriorityQueue* pq = safe_malloc(sizeof(PriorityQueue));
    pq->items = safe_malloc(capacity * sizeof(PQItem));
    pq->size = 0;
    pq->capacity = capacity;
    return pq;
}

static void pq_free(PriorityQueue* pq) {
    if (pq) {
        free(pq->items);
        free(pq);
    }
}

static void pq_swap(PQItem* a, PQItem* b) {
    PQItem temp = *a;
    *a = *b;
    *b = temp;
}

static void pq_heapify_up(PriorityQueue* pq, size_t idx) {
    while (idx > 0) {
        size_t parent = (idx - 1) / 2;
        if (pq->items[idx].distance < pq->items[parent].distance) {
            pq_swap(&pq->items[idx], &pq->items[parent]);
            idx = parent;
        } else {
            break;
        }
    }
}

static void pq_heapify_down(PriorityQueue* pq, size_t idx) {
    while (true) {
        size_t smallest = idx;
        size_t left = 2 * idx + 1;
        size_t right = 2 * idx + 2;
        
        if (left < pq->size && pq->items[left].distance < pq->items[smallest].distance) {
            smallest = left;
        }
        if (right < pq->size && pq->items[right].distance < pq->items[smallest].distance) {
            smallest = right;
        }
        
        if (smallest != idx) {
            pq_swap(&pq->items[idx], &pq->items[smallest]);
            idx = smallest;
        } else {
            break;
        }
    }
}

static void pq_push(PriorityQueue* pq, NodeId node, double distance) {
    if (pq->size >= pq->capacity) {
        pq->capacity *= 2;
        pq->items = safe_realloc(pq->items, pq->capacity * sizeof(PQItem));
    }
    pq->items[pq->size].node = node;
    pq->items[pq->size].distance = distance;
    pq_heapify_up(pq, pq->size);
    pq->size++;
}

static bool pq_is_empty(PriorityQueue* pq) {
    return pq->size == 0;
}

static PQItem pq_pop(PriorityQueue* pq) {
    PQItem result = pq->items[0];
    pq->size--;
    if (pq->size > 0) {
        pq->items[0] = pq->items[pq->size];
        pq_heapify_down(pq, 0);
    }
    return result;
}

Path* graph_shortest_path(Graph* graph, NodeId source, NodeId target) {
    if (!graph) return NULL;
    
    Node* src_node = node_index_get(&graph->node_index, source);
    Node* tgt_node = node_index_get(&graph->node_index, target);
    if (!src_node || !tgt_node) return NULL;
    
    /* Use BFS for unweighted shortest path */
    double* distances = safe_malloc(graph->next_node_id * sizeof(double));
    NodeId* previous = safe_malloc(graph->next_node_id * sizeof(NodeId));
    
    for (size_t i = 0; i < graph->next_node_id; i++) {
        distances[i] = INFINITY;
        previous[i] = INVALID_NODE_ID;
    }
    
    PriorityQueue* pq = pq_create(1024);
    distances[source] = 0;
    pq_push(pq, source, 0);
    
    while (!pq_is_empty(pq)) {
        PQItem current = pq_pop(pq);
        
        if (current.node == target) break;
        if (current.distance > distances[current.node]) continue;
        
        Node* node = node_index_get(&graph->node_index, current.node);
        if (!node) continue;
        
        AdjEntry* entry = node->out_edges;
        while (entry) {
            NodeId neighbor = entry->edge->target;
            double new_dist = current.distance + 1; /* Unweighted */
            
            if (new_dist < distances[neighbor]) {
                distances[neighbor] = new_dist;
                previous[neighbor] = current.node;
                pq_push(pq, neighbor, new_dist);
            }
            entry = entry->next;
        }
    }
    
    pq_free(pq);
    
    if (distances[target] == INFINITY) {
        free(distances);
        free(previous);
        return NULL;
    }
    
    /* Reconstruct path */
    size_t path_length = (size_t)distances[target] + 1;
    Path* path = safe_malloc(sizeof(Path));
    path->length = path_length;
    path->path = safe_malloc(path_length * sizeof(Node*));
    
    NodeId current = target;
    for (size_t i = path_length; i > 0; i--) {
        path->path[i - 1] = node_index_get(&graph->node_index, current);
        current = previous[current];
    }
    
    free(distances);
    free(previous);
    
    return path;
}

Path* graph_shortest_path_weighted(Graph* graph, NodeId source, NodeId target) {
    if (!graph) return NULL;
    
    Node* src_node = node_index_get(&graph->node_index, source);
    Node* tgt_node = node_index_get(&graph->node_index, target);
    if (!src_node || !tgt_node) return NULL;
    
    double* distances = safe_malloc(graph->next_node_id * sizeof(double));
    NodeId* previous = safe_malloc(graph->next_node_id * sizeof(NodeId));
    
    for (size_t i = 0; i < graph->next_node_id; i++) {
        distances[i] = INFINITY;
        previous[i] = INVALID_NODE_ID;
    }
    
    PriorityQueue* pq = pq_create(1024);
    distances[source] = 0;
    pq_push(pq, source, 0);
    
    while (!pq_is_empty(pq)) {
        PQItem current = pq_pop(pq);
        
        if (current.node == target) break;
        if (current.distance > distances[current.node]) continue;
        
        Node* node = node_index_get(&graph->node_index, current.node);
        if (!node) continue;
        
        AdjEntry* entry = node->out_edges;
        while (entry) {
            NodeId neighbor = entry->edge->target;
            double weight = entry->edge->weight > 0 ? entry->edge->weight : 1.0;
            double new_dist = current.distance + weight;
            
            if (new_dist < distances[neighbor]) {
                distances[neighbor] = new_dist;
                previous[neighbor] = current.node;
                pq_push(pq, neighbor, new_dist);
            }
            entry = entry->next;
        }
    }
    
    pq_free(pq);
    
    if (distances[target] == INFINITY) {
        free(distances);
        free(previous);
        return NULL;
    }
    
    /* Reconstruct path */
    size_t capacity = 64;
    NodeId* path_ids = safe_malloc(capacity * sizeof(NodeId));
    size_t path_length = 0;
    
    NodeId current = target;
    while (current != INVALID_NODE_ID) {
        if (path_length >= capacity) {
            capacity *= 2;
            path_ids = safe_realloc(path_ids, capacity * sizeof(NodeId));
        }
        path_ids[path_length++] = current;
        current = previous[current];
    }
    
    Path* path = safe_malloc(sizeof(Path));
    path->length = path_length;
    path->path = safe_malloc(path_length * sizeof(Node*));
    
    for (size_t i = 0; i < path_length; i++) {
        path->path[i] = node_index_get(&graph->node_index, path_ids[path_length - 1 - i]);
    }
    
    free(path_ids);
    free(distances);
    free(previous);
    
    return path;
}

PathResult graph_all_shortest_paths(Graph* graph, NodeId source, NodeId target) {
    PathResult result = {NULL, 0, 0};
    /* For simplicity, return just the single shortest path */
    Path* path = graph_shortest_path(graph, source, target);
    if (path) {
        result.paths = safe_malloc(sizeof(Path));
        result.paths[0] = *path;
        result.count = 1;
        result.capacity = 1;
        free(path); /* Free the container but keep the path data */
    }
    return result;
}

PathResult graph_all_paths(Graph* graph, NodeId source, NodeId target, size_t max_depth) {
    PathResult result = {NULL, 0, 0};
    if (!graph) return result;
    
    result.paths = safe_malloc(16 * sizeof(Path));
    result.capacity = 16;
    
    /* DFS-based path finding */
    NodeId* current_path = safe_malloc((max_depth + 2) * sizeof(NodeId));
    VisitedSet* visited = visited_set_create(graph->next_node_id);
    
    /* Recursive DFS helper would go here - simplified version */
    /* For now, just find one path */
    Path* path = graph_shortest_path(graph, source, target);
    if (path && path->length <= max_depth + 1) {
        result.paths[result.count++] = *path;
        free(path);
    }
    
    free(current_path);
    visited_set_free(visited);
    
    return result;
}

void path_free(Path* path) {
    if (path) {
        free(path->path);
        free(path);
    }
}

void path_result_free(PathResult* result) {
    if (result && result->paths) {
        for (size_t i = 0; i < result->count; i++) {
            free(result->paths[i].path);
        }
        free(result->paths);
        result->paths = NULL;
        result->count = 0;
        result->capacity = 0;
    }
}

char* path_to_string(const Path* path) {
    if (!path || !path->path) return safe_strdup("[]");
    
    size_t buffer_size = 256;
    char* buffer = safe_malloc(buffer_size);
    size_t pos = 0;
    
    pos += snprintf(buffer + pos, buffer_size - pos, "[");
    
    for (size_t i = 0; i < path->length && pos < buffer_size - 50; i++) {
        if (i > 0) {
            pos += snprintf(buffer + pos, buffer_size - pos, " -> ");
        }
        pos += snprintf(buffer + pos, buffer_size - pos, "%llu", 
                       (unsigned long long)path->path[i]->id);
    }
    
    pos += snprintf(buffer + pos, buffer_size - pos, "]");
    
    return buffer;
}

/* ============== Serialization ============== */

bool graph_save(Graph* graph, const char* filepath) {
    if (!graph || !filepath) return false;
    
    FILE* file = fopen(filepath, "wb");
    if (!file) return false;
    
    /* Write header */
    const char* magic = "TGDB";
    fwrite(magic, 1, 4, file);
    
    uint32_t version = 1;
    fwrite(&version, sizeof(version), 1, file);
    
    /* Write graph info */
    uint32_t name_len = strlen(graph->name);
    fwrite(&name_len, sizeof(name_len), 1, file);
    fwrite(graph->name, 1, name_len, file);
    
    uint64_t node_count = graph->node_index.size;
    uint64_t edge_count = graph->edge_count;
    fwrite(&node_count, sizeof(node_count), 1, file);
    fwrite(&edge_count, sizeof(edge_count), 1, file);
    
    /* Write nodes */
    for (size_t i = 0; i < graph->node_index.bucket_count; i++) {
        NodeEntry* entry = graph->node_index.buckets[i];
        while (entry) {
            Node* node = entry->node;
            
            fwrite(&node->id, sizeof(node->id), 1, file);
            uint32_t label_len = strlen(node->label);
            fwrite(&label_len, sizeof(label_len), 1, file);
            fwrite(node->label, 1, label_len, file);
            
            uint32_t prop_count = node->property_count;
            fwrite(&prop_count, sizeof(prop_count), 1, file);
            
            for (size_t j = 0; j < node->property_count; j++) {
                Property* prop = node->properties[j];
                uint32_t key_len = strlen(prop->key);
                fwrite(&key_len, sizeof(key_len), 1, file);
                fwrite(prop->key, 1, key_len, file);
                fwrite(&prop->value.type, sizeof(prop->value.type), 1, file);
                
                switch (prop->value.type) {
                    case PROP_TYPE_BOOL:
                        fwrite(&prop->value.value.bool_val, sizeof(bool), 1, file);
                        break;
                    case PROP_TYPE_INT64:
                        fwrite(&prop->value.value.int_val, sizeof(int64_t), 1, file);
                        break;
                    case PROP_TYPE_FLOAT64:
                        fwrite(&prop->value.value.float_val, sizeof(double), 1, file);
                        break;
                    case PROP_TYPE_STRING:
                        {
                            uint32_t str_len = strlen(prop->value.value.string_val);
                            fwrite(&str_len, sizeof(str_len), 1, file);
                            fwrite(prop->value.value.string_val, 1, str_len, file);
                        }
                        break;
                    default:
                        break;
                }
            }
            
            entry = entry->next;
        }
    }
    
    /* Write edges */
    for (size_t i = 0; i < graph->edge_count; i++) {
        Edge* edge = graph->edges[i];
        
        fwrite(&edge->id, sizeof(edge->id), 1, file);
        fwrite(&edge->source, sizeof(edge->source), 1, file);
        fwrite(&edge->target, sizeof(edge->target), 1, file);
        
        uint32_t label_len = strlen(edge->label);
        fwrite(&label_len, sizeof(label_len), 1, file);
        fwrite(edge->label, 1, label_len, file);
        
        fwrite(&edge->weight, sizeof(edge->weight), 1, file);
        
        uint32_t prop_count = edge->property_count;
        fwrite(&prop_count, sizeof(prop_count), 1, file);
        
        for (size_t j = 0; j < edge->property_count; j++) {
            Property* prop = edge->properties[j];
            uint32_t key_len = strlen(prop->key);
            fwrite(&key_len, sizeof(key_len), 1, file);
            fwrite(prop->key, 1, key_len, file);
            fwrite(&prop->value.type, sizeof(prop->value.type), 1, file);
            
            switch (prop->value.type) {
                case PROP_TYPE_BOOL:
                    fwrite(&prop->value.value.bool_val, sizeof(bool), 1, file);
                    break;
                case PROP_TYPE_INT64:
                    fwrite(&prop->value.value.int_val, sizeof(int64_t), 1, file);
                    break;
                case PROP_TYPE_FLOAT64:
                    fwrite(&prop->value.value.float_val, sizeof(double), 1, file);
                    break;
                case PROP_TYPE_STRING:
                    {
                        uint32_t str_len = strlen(prop->value.value.string_val);
                        fwrite(&str_len, sizeof(str_len), 1, file);
                        fwrite(prop->value.value.string_val, 1, str_len, file);
                    }
                    break;
                default:
                    break;
            }
        }
    }
    
    fclose(file);
    return true;
}

Graph* graph_load(const char* filepath) {
    if (!filepath) return NULL;
    
    FILE* file = fopen(filepath, "rb");
    if (!file) return NULL;
    
    /* Read header */
    char magic[4];
    fread(magic, 1, 4, file);
    if (memcmp(magic, "TGDB", 4) != 0) {
        fclose(file);
        return NULL;
    }
    
    uint32_t version;
    fread(&version, sizeof(version), 1, file);
    
    /* Read graph info */
    uint32_t name_len;
    fread(&name_len, sizeof(name_len), 1, file);
    char* name = safe_malloc(name_len + 1);
    fread(name, 1, name_len, file);
    name[name_len] = '\0';
    
    Graph* graph = graph_create(name);
    free(name);
    
    uint64_t node_count, edge_count;
    fread(&node_count, sizeof(node_count), 1, file);
    fread(&edge_count, sizeof(edge_count), 1, file);
    
    /* Read nodes */
    for (uint64_t i = 0; i < node_count; i++) {
        NodeId id;
        fread(&id, sizeof(id), 1, file);
        
        uint32_t label_len;
        fread(&label_len, sizeof(label_len), 1, file);
        char* label = safe_malloc(label_len + 1);
        fread(label, 1, label_len, file);
        label[label_len] = '\0';
        
        Node* node = node_create(id, label);
        free(label);
        
        node_index_insert(&graph->node_index, node);
        label_index_add_node(&graph->label_index, node);
        
        if (id >= graph->next_node_id) {
            graph->next_node_id = id + 1;
        }
        
        uint32_t prop_count;
        fread(&prop_count, sizeof(prop_count), 1, file);
        
        for (uint32_t j = 0; j < prop_count; j++) {
            uint32_t key_len;
            fread(&key_len, sizeof(key_len), 1, file);
            char* key = safe_malloc(key_len + 1);
            fread(key, 1, key_len, file);
            key[key_len] = '\0';
            
            PropertyType type;
            fread(&type, sizeof(type), 1, file);
            
            PropertyValue value = prop_null();
            switch (type) {
                case PROP_TYPE_BOOL:
                    {
                        bool val;
                        fread(&val, sizeof(bool), 1, file);
                        value = prop_bool(val);
                    }
                    break;
                case PROP_TYPE_INT64:
                    {
                        int64_t val;
                        fread(&val, sizeof(int64_t), 1, file);
                        value = prop_int64(val);
                    }
                    break;
                case PROP_TYPE_FLOAT64:
                    {
                        double val;
                        fread(&val, sizeof(double), 1, file);
                        value = prop_float64(val);
                    }
                    break;
                case PROP_TYPE_STRING:
                    {
                        uint32_t str_len;
                        fread(&str_len, sizeof(str_len), 1, file);
                        char* str = safe_malloc(str_len + 1);
                        fread(str, 1, str_len, file);
                        str[str_len] = '\0';
                        value = prop_string(str);
                        free(str);
                    }
                    break;
                default:
                    break;
            }
            
            node_set_property(node, key, value);
            free(key);
        }
    }
    
    /* Read edges */
    for (uint64_t i = 0; i < edge_count; i++) {
        EdgeId id;
        fread(&id, sizeof(id), 1, file);
        
        NodeId source, target;
        fread(&source, sizeof(source), 1, file);
        fread(&target, sizeof(target), 1, file);
        
        uint32_t label_len;
        fread(&label_len, sizeof(label_len), 1, file);
        char* label = safe_malloc(label_len + 1);
        fread(label, 1, label_len, file);
        label[label_len] = '\0';
        
        double weight;
        fread(&weight, sizeof(weight), 1, file);
        
        Edge* edge = edge_create(id, source, target, label, weight);
        free(label);
        
        /* Add to edge array */
        if (graph->edge_count >= graph->edge_capacity) {
            graph->edge_capacity *= 2;
            graph->edges = safe_realloc(graph->edges, graph->edge_capacity * sizeof(Edge*));
        }
        graph->edges[graph->edge_count++] = edge;
        
        if (id >= graph->next_edge_id) {
            graph->next_edge_id = id + 1;
        }
        
        /* Add to adjacency lists */
        Node* src_node = node_index_get(&graph->node_index, source);
        Node* tgt_node = node_index_get(&graph->node_index, target);
        
        if (src_node) {
            AdjEntry* out_entry = safe_malloc(sizeof(AdjEntry));
            out_entry->edge = edge;
            out_entry->next = src_node->out_edges;
            src_node->out_edges = out_entry;
            src_node->out_degree++;
        }
        
        if (tgt_node) {
            AdjEntry* in_entry = safe_malloc(sizeof(AdjEntry));
            in_entry->edge = edge;
            in_entry->next = tgt_node->in_edges;
            tgt_node->in_edges = in_entry;
            tgt_node->in_degree++;
        }
        
        uint32_t prop_count;
        fread(&prop_count, sizeof(prop_count), 1, file);
        
        for (uint32_t j = 0; j < prop_count; j++) {
            uint32_t key_len;
            fread(&key_len, sizeof(key_len), 1, file);
            char* key = safe_malloc(key_len + 1);
            fread(key, 1, key_len, file);
            key[key_len] = '\0';
            
            PropertyType type;
            fread(&type, sizeof(type), 1, file);
            
            PropertyValue value = prop_null();
            switch (type) {
                case PROP_TYPE_BOOL:
                    {
                        bool val;
                        fread(&val, sizeof(bool), 1, file);
                        value = prop_bool(val);
                    }
                    break;
                case PROP_TYPE_INT64:
                    {
                        int64_t val;
                        fread(&val, sizeof(int64_t), 1, file);
                        value = prop_int64(val);
                    }
                    break;
                case PROP_TYPE_FLOAT64:
                    {
                        double val;
                        fread(&val, sizeof(double), 1, file);
                        value = prop_float64(val);
                    }
                    break;
                case PROP_TYPE_STRING:
                    {
                        uint32_t str_len;
                        fread(&str_len, sizeof(str_len), 1, file);
                        char* str = safe_malloc(str_len + 1);
                        fread(str, 1, str_len, file);
                        str[str_len] = '\0';
                        value = prop_string(str);
                        free(str);
                    }
                    break;
                default:
                    break;
            }
            
            edge_set_property(edge, key, value);
            free(key);
        }
    }
    
    fclose(file);
    return graph;
}

/* ============== JSON Export ============== */

char* graph_to_json(Graph* graph) {
    if (!graph) return safe_strdup("{}");
    
    size_t buffer_size = 65536;
    char* buffer = safe_malloc(buffer_size);
    size_t pos = 0;
    
    pos += snprintf(buffer + pos, buffer_size - pos, 
                   "{\"name\":\"%s\",\"nodes\":[", graph->name);
    
    bool first = true;
    for (size_t i = 0; i < graph->node_index.bucket_count; i++) {
        NodeEntry* entry = graph->node_index.buckets[i];
        while (entry) {
            Node* node = entry->node;
            
            if (!first) {
                pos += snprintf(buffer + pos, buffer_size - pos, ",");
            }
            first = false;
            
            char* node_json = node_to_json(node);
            pos += snprintf(buffer + pos, buffer_size - pos, "%s", node_json);
            free(node_json);
            
            entry = entry->next;
        }
    }
    
    pos += snprintf(buffer + pos, buffer_size - pos, "],\"edges\":[");
    
    first = true;
    for (size_t i = 0; i < graph->edge_count; i++) {
        Edge* edge = graph->edges[i];
        
        if (!first) {
            pos += snprintf(buffer + pos, buffer_size - pos, ",");
        }
        first = false;
        
        char* edge_json = edge_to_json(edge);
        pos += snprintf(buffer + pos, buffer_size - pos, "%s", edge_json);
        free(edge_json);
    }
    
    pos += snprintf(buffer + pos, buffer_size - pos, 
                   "],\"stats\":{\"nodeCount\":%zu,\"edgeCount\":%zu}}",
                   graph->node_index.size, graph->edge_count);
    
    return buffer;
}

char* node_to_json(Node* node) {
    if (!node) return safe_strdup("{}");
    
    size_t buffer_size = 4096;
    char* buffer = safe_malloc(buffer_size);
    size_t pos = 0;
    
    pos += snprintf(buffer + pos, buffer_size - pos,
                   "{\"id\":%llu,\"label\":\"%s\",\"properties\":{",
                   (unsigned long long)node->id, node->label);
    
    for (size_t i = 0; i < node->property_count; i++) {
        Property* prop = node->properties[i];
        char* value_str = prop_to_string(&prop->value);
        
        if (prop->value.type == PROP_TYPE_STRING) {
            pos += snprintf(buffer + pos, buffer_size - pos,
                          "%s\"%s\":\"%s\"",
                          i > 0 ? "," : "", prop->key, value_str);
        } else {
            pos += snprintf(buffer + pos, buffer_size - pos,
                          "%s\"%s\":%s",
                          i > 0 ? "," : "", prop->key, value_str);
        }
        free(value_str);
    }
    
    pos += snprintf(buffer + pos, buffer_size - pos,
                   "},\"outDegree\":%zu,\"inDegree\":%zu}",
                   node->out_degree, node->in_degree);
    
    return buffer;
}

char* edge_to_json(Edge* edge) {
    if (!edge) return safe_strdup("{}");
    
    size_t buffer_size = 4096;
    char* buffer = safe_malloc(buffer_size);
    size_t pos = 0;
    
    pos += snprintf(buffer + pos, buffer_size - pos,
                   "{\"id\":%llu,\"source\":%llu,\"target\":%llu,\"label\":\"%s\",\"weight\":%.6g,\"properties\":{",
                   (unsigned long long)edge->id,
                   (unsigned long long)edge->source,
                   (unsigned long long)edge->target,
                   edge->label, edge->weight);
    
    for (size_t i = 0; i < edge->property_count; i++) {
        Property* prop = edge->properties[i];
        char* value_str = prop_to_string(&prop->value);
        
        if (prop->value.type == PROP_TYPE_STRING) {
            pos += snprintf(buffer + pos, buffer_size - pos,
                          "%s\"%s\":\"%s\"",
                          i > 0 ? "," : "", prop->key, value_str);
        } else {
            pos += snprintf(buffer + pos, buffer_size - pos,
                          "%s\"%s\":%s",
                          i > 0 ? "," : "", prop->key, value_str);
        }
        free(value_str);
    }
    
    pos += snprintf(buffer + pos, buffer_size - pos, "}}");
    
    return buffer;
}

void graph_print(Graph* graph) {
    if (!graph) {
        printf("Graph: NULL\n");
        return;
    }
    
    printf("Graph: %s\n", graph->name);
    printf("  Nodes: %zu\n", graph->node_index.size);
    printf("  Edges: %zu\n", graph->edge_count);
    
    printf("\nNodes:\n");
    for (size_t i = 0; i < graph->node_index.bucket_count; i++) {
        NodeEntry* entry = graph->node_index.buckets[i];
        while (entry) {
            Node* node = entry->node;
            printf("  [%llu] %s (out: %zu, in: %zu)\n",
                   (unsigned long long)node->id, node->label,
                   node->out_degree, node->in_degree);
            entry = entry->next;
        }
    }
    
    printf("\nEdges:\n");
    for (size_t i = 0; i < graph->edge_count; i++) {
        Edge* edge = graph->edges[i];
        printf("  [%llu] %llu --[%s]--> %llu (weight: %.2f)\n",
               (unsigned long long)edge->id,
               (unsigned long long)edge->source,
               edge->label,
               (unsigned long long)edge->target,
               edge->weight);
    }
}

/* ============== Memory Statistics ============== */

GraphStats graph_get_stats(Graph* graph) {
    GraphStats stats = {0};
    if (!graph) return stats;
    
    stats.node_count = graph->node_index.size;
    stats.edge_count = graph->edge_count;
    
    /* Calculate node memory */
    for (size_t i = 0; i < graph->node_index.bucket_count; i++) {
        NodeEntry* entry = graph->node_index.buckets[i];
        while (entry) {
            Node* node = entry->node;
            stats.node_memory += sizeof(Node);
            stats.node_memory += strlen(node->label) + 1;
            stats.node_memory += node->property_capacity * sizeof(Property*);
            
            for (size_t j = 0; j < node->property_count; j++) {
                stats.node_memory += sizeof(Property);
                stats.node_memory += strlen(node->properties[j]->key) + 1;
                if (node->properties[j]->value.type == PROP_TYPE_STRING) {
                    stats.node_memory += strlen(node->properties[j]->value.value.string_val) + 1;
                }
                stats.property_count++;
            }
            
            AdjEntry* adj = node->out_edges;
            while (adj) {
                stats.node_memory += sizeof(AdjEntry);
                adj = adj->next;
            }
            adj = node->in_edges;
            while (adj) {
                stats.node_memory += sizeof(AdjEntry);
                adj = adj->next;
            }
            
            entry = entry->next;
        }
    }
    
    /* Calculate edge memory */
    for (size_t i = 0; i < graph->edge_count; i++) {
        Edge* edge = graph->edges[i];
        stats.edge_memory += sizeof(Edge);
        stats.edge_memory += strlen(edge->label) + 1;
        stats.edge_memory += edge->property_capacity * sizeof(Property*);
        
        for (size_t j = 0; j < edge->property_count; j++) {
            stats.edge_memory += sizeof(Property);
            stats.edge_memory += strlen(edge->properties[j]->key) + 1;
            if (edge->properties[j]->value.type == PROP_TYPE_STRING) {
                stats.edge_memory += strlen(edge->properties[j]->value.value.string_val) + 1;
            }
            stats.property_count++;
        }
    }
    
    /* Calculate index memory */
    stats.index_memory += graph->node_index.bucket_count * sizeof(NodeEntry*);
    stats.index_memory += graph->node_index.size * sizeof(NodeEntry);
    stats.index_memory += graph->label_index.bucket_count * sizeof(LabelEntry*);
    stats.index_memory += graph->label_index.size * sizeof(LabelEntry);
    
    stats.total_memory = stats.node_memory + stats.edge_memory + stats.index_memory;
    
    return stats;
}
