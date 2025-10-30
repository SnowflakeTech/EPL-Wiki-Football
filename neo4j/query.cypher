LOAD CSV WITH HEADERS FROM 'file:///edges/coached.csv' AS row
WITH row WHERE row.season_id IS NOT NULL
MATCH (h:Coach {coach_id: row.`:START_ID(Coach)`})
MATCH (c:Club {club_id: row.`:END_ID(Club)`})
MATCH (s:Season {season_id: row.season_id})
MERGE (h)-[:COACHED {years: row.years, is_current: row.is_current}]->(c)
MERGE (c)-[:PART_OF]->(s);

CALL db.labels();
CALL db.relationshipTypes();

