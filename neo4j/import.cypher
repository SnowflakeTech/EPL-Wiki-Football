LOAD CSV WITH HEADERS FROM 'file:///edges/part_of.csv' AS row
MATCH (c:Club {club_id: row.`:START_ID(Club)`})
MATCH (s:Season {season_id: row.`:END_ID(Season)`})
MERGE (c)-[:PART_OF {season: row.Season}]->(s);

LOAD CSV WITH HEADERS FROM 'file:///edges/played_for.csv' AS row
MATCH (p:Player {player_id: row.`:START_ID(Player)`})
MATCH (c:Club {club_id: row.`:END_ID(Club)`})
MERGE (p)-[:PLAYED_FOR {season: row.season, position: row.position}]->(c);

LOAD CSV WITH HEADERS FROM 'file:///edges/coached.csv' AS row
MATCH (co:Coach {coach_id: row.`:START_ID(Coach)`})
MATCH (c:Club {club_id: row.`:END_ID(Club)`})
MERGE (co)-[:COACHED {season: row.season, years: row.years, is_current: row.is_current}]->(c);
