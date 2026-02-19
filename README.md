# TurboGraphDB
AI optimized Graph database

# Features
1. High-performance C implementation with optimized data structures
2. Hash-based indexing (FNV-1a) for O(1) node lookups
3. Adjacency list storage for efficient edge traversal
4. Property system supporting bool, int64, float64, string, bytes
5. Algorithms: BFS, DFS, Dijkstra shortest path
6. Serialization: Binary format for persistence, JSON export for API

# Architecture Overview

┌─────────────────────────────────────────────────────────┐
│                   Web Browser (React)                   │
│  Overview │ Nodes │ Edges │ Query │ Visualization      │
└────────────────────────┬────────────────────────────────┘
                         │ REST API
┌────────────────────────▼────────────────────────────────┐
│              Next.js API Server (Port 3000)             │
│   /api/graph  │  /api/graph/nodes  │  /api/graph/edges  │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│            TypeScript GraphDB (In-Memory)               │
│    Node/Edge Storage • Traversal • Path Finding         │
└────────────────────────┬────────────────────────────────┘
                         │ FFI (Optional)
┌────────────────────────▼────────────────────────────────┐
│           C Graph Engine (libgraphdb.so)                │
│    4.4M nodes/sec • Optimized algorithms                │
└─────────────────────────────────────────────────────────┘
--------------------------------------------------------------------------------------
#Python Client Code

from turbographdb_client import connect

db = connect("http://localhost:3000")

#Create nodes and edges
alice = db.create_node("Person", {"name": "Alice", "age": 30})
db.create_edge(alice.id, bob.id, "KNOWS", weight=0.8)

#Query operations

path = db.shortest_path(1, 5)
bfs_nodes = db.bfs(1)

---------------------------------------------------------------------------------------
# Web UI Features:
1. Interactive dashboard with real-time stats
2. Node and Edge management tabs
3. Query builder for graph algorithms
4. SVG-based graph visualization
5. Sample data loading & export
--------------------------------------------------------------------------------------
# API Endpoints
Endpoint          Methods                Description
/api/graph	      GET, POST, DELETE	      Graph stats & management
/api/graph/nodes	GET, POST, PUT, DELETE	Node CRUD operations
/api/graph/edges	GET, POST, PUT, DELETE	Edge CRUD operations
/api/graph/query	GET	BFS, DFS, shortest path, all paths, export
