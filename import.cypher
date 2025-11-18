///////////////////////////////////////////////////////////////////////////
// EPL GRAPH IMPORT SCRIPT
// File: import.cypher
// Compatible: Neo4j 5.x
///////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////
// 1. CREATE CONSTRAINTS
///////////////////////////////////////////////////////////////////////////

CREATE CONSTRAINT club_id_unique IF NOT EXISTS
FOR (c:Club) REQUIRE c.id IS UNIQUE;

CREATE CONSTRAINT player_id_unique IF NOT EXISTS
FOR (p:Player) REQUIRE p.id IS UNIQUE;

CREATE CONSTRAINT coach_id_unique IF NOT EXISTS
FOR (c:Coach) REQUIRE c.id IS UNIQUE;

CREATE CONSTRAINT season_id_unique IF NOT EXISTS
FOR (s:Season) REQUIRE s.id IS UNIQUE;


///////////////////////////////////////////////////////////////////////////
// 2. LOAD CLUB NODES
///////////////////////////////////////////////////////////////////////////

LOAD CSV WITH HEADERS FROM "file:///nodes/clubs.csv" AS row
MERGE (c:Club {id: row.club_id})
SET c.name      = row.Club,
    c.location1 = row.Location,
    c.capacity  = row.`Location 2`,
    c.stadium   = row.Stadium;


///////////////////////////////////////////////////////////////////////////
// 3. LOAD PLAYER NODES
///////////////////////////////////////////////////////////////////////////

LOAD CSV WITH HEADERS FROM "file:///nodes/players.csv" AS row
MERGE (p:Player {id: row.player_id})
SET p.name     = row.name,
    p.nation   = row.nation,
    p.position = row.position;


///////////////////////////////////////////////////////////////////////////
// 4. LOAD COACH NODES
///////////////////////////////////////////////////////////////////////////

LOAD CSV WITH HEADERS FROM "file:///nodes/coaches.csv" AS row
MERGE (c:Coach {id: row.coach_id})
SET c.name = row.name;


///////////////////////////////////////////////////////////////////////////
// 5. LOAD SEASON NODES
///////////////////////////////////////////////////////////////////////////

LOAD CSV WITH HEADERS FROM "file:///nodes/seasons.csv" AS row
MERGE (s:Season {id: row.season_id})
SET s.name       = row.name,
    s.start_year = row.start_year,
    s.end_year   = row.end_year,
    s.url        = row.url;


///////////////////////////////////////////////////////////////////////////
// 6. LOAD RELATIONSHIPS: PLAYED_FOR
///////////////////////////////////////////////////////////////////////////

LOAD CSV WITH HEADERS FROM "file:///edges/played_for.csv" AS row
MATCH (p:Player {id: row.player_id})
MATCH (c:Club   {id: row.club_id})
MATCH (s:Season {id: row.season})
MERGE (p)-[r:PLAYED_FOR {season: row.season}]
      ->(c)
SET r.position = row.position;


///////////////////////////////////////////////////////////////////////////
// 7. LOAD RELATIONSHIPS: COACHED
///////////////////////////////////////////////////////////////////////////

LOAD CSV WITH HEADERS FROM "file:///edges/coached.csv" AS row
MATCH (co:Coach {id: row.coach_id})
MATCH (cl:Club  {id: row.club_id})
MATCH (s:Season {id: row.season})
MERGE (co)-[r:COACHED {season: row.season}]
      ->(cl)
SET r.years      = row.years,
    r.is_current = row.is_current;


///////////////////////////////////////////////////////////////////////////
// 8. LOAD RELATIONSHIPS: PART_OF (Club → Season)
///////////////////////////////////////////////////////////////////////////

LOAD CSV WITH HEADERS FROM "file:///edges/part_of.csv" AS row
MATCH (c:Club   {id: row.`:START_ID(Club)`})
MATCH (s:Season {id: row.`:END_ID(Season)`})
MERGE (c)-[:PART_OF {season: row.Season}]->(s);


///////////////////////////////////////////////////////////////////////////
// 9. LOAD RELATIONS: CLUBS BY SEASON
///////////////////////////////////////////////////////////////////////////
// Your CSV uses "Season" (like 2024–25) so convert it to "EPL-xxxx"

LOAD CSV WITH HEADERS FROM "file:///relations/clubs_by_season.csv" AS row
MATCH (c:Club {id: row.club_id})
MATCH (s:Season {id: "EPL-" + row.Season})
MERGE (c)-[:PARTICIPATED_IN]->(s);


///////////////////////////////////////////////////////////////////////////
// 10. SUMMARY CHECK
///////////////////////////////////////////////////////////////////////////

MATCH (n) RETURN count(n) AS total_nodes;

MATCH ()-[r]->() RETURN type(r) AS relation, count(*) AS total ORDER BY total DESC;
