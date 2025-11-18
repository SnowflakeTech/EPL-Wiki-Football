from neo4j import GraphDatabase
import networkx as nx

# 1. Kết nối Neo4j
driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "test1234"))

query = """
MATCH (p:Player)-[:PLAYED_FOR]->(c:Club)
RETURN p.id AS player, c.id AS club
"""

# 3. Tạo graph NetworkX
G = nx.Graph()

with driver.session() as session:
    for record in session.run(query):
        G.add_edge(record["player"], record["club"])

src = "player_david_raya"
dst = "player_bukayo_saka"

path = nx.shortest_path(G, src, dst)

print("Shortest path:", path)
