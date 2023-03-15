#!/usr/bin/env python
# coding: utf-8

# # Laboratory 1: Property Graphs
# ### Luis Alfredo Leon Villapún
# ### Liliia Aliakberova
# 
# # Part C 
# ***
# In this section we will create queries for a simple recommender
# 
# ## Creating the connector
# 
# As in previous parts, let's first create the connector to handle the messages with Neo4j.

# In[13]:


# Uncomment to install extension
#pip install ipython-cypher


# In[ ]:


get_ipython().run_line_magic('load_ext', 'cypher')


# In[ ]:


from connector import Neo4jConnector
from getpass import getpass

uri = "neo4j://localhost:7687"
user = "neo4j"
password = getpass("Input your password to connect")
conn = Neo4jConnector(uri, user, password)


# ## Queries
# 1. The first thing to do is to find/define the research communities. A community is defined by a set of keywords. Assume that the database community is defined through the following keywords: data management, indexing, data modeling, big data, data processing, data storage and data querying.
# 

# In[ ]:


# From documentation site: https://neo4j.com/docs/python-manual/current/

def query_1(tx):
    result = tx.run(
        """
MATCH (a:Author)<-[w:WRITTEN_BY]-(p:Paper)-[r:RELATED_TO]->(t:Topic)
WHERE t.Keyword CONTAINS 'Data management' OR t.Keyword CONTAINS 'Indexing' OR t.Keyword CONTAINS 'Data modeling' OR t.Keyword CONTAINS 'Big data' OR t.Keyword CONTAINS 'Data processing' OR t.Keyword CONTAINS 'Data storage' OR t.Keyword CONTAINS 'Data querying'
RETURN p.Title, a.AuthorName, t.Keyword
        """
    )
    records = list(result)
    summary = result.consume()
    return records, summary

with conn.driver.session(database="neo4j") as session:
    records, summary = session.execute_read(query_1)
    print(records, summary)


# 2. Next, we need to find the conferences and journals related to the database community (i.e., are specific to the field of databases). Assume that if 90% of the papers published in a conference/journal contain one of the keywords of the database community we consider that conference/journal as related to that community

# In[ ]:


# From documentation site: https://neo4j.com/docs/python-manual/current/

def query_2(tx):
    result = tx.run(
        """
        MATCH (d:Document)<-[w:PUBLISHED_AT]-(p:Paper)-[r:RELATED_TO]->(t:Topic)
        WITH d, t, count(p) as p1, collect(p) as papers
        UNWIND papers as p
        WITH d, t, count(p) as p2, p1
        WHERE (t.Keyword CONTAINS 'Data management' OR t.Keyword CONTAINS 'Indexing' OR t.Keyword CONTAINS 'Data modeling' OR t.Keyword CONTAINS 'Big data' OR t.Keyword CONTAINS 'Data processing' OR t.Keyword CONTAINS 'Data storage' OR t.Keyword CONTAINS 'Data querying')
        AND (p2/p1 >=0.9)
        RETURN DISTINCT d.Title as Paper_Title, d.DocumentType as Publication_Type
        """
    )
    records = list(result)
    summary = result.consume()
    return records, summary

with conn.driver.session(database="neo4j") as session:
    records, summary = session.execute_read(query_2)
    print(records, summary)


# 3. Next, we want to identify the top papers of these conferences/journals. We need to find the papers with the highest page rank provided the number of citations from the papers of the same community (papers in the conferences/journals of the database community). As a result we would obtain (highlight), say, the top-100 papers of the conferences of the database community.
# 
#    Performing the quiery consists of several following steps

#     I step is creating the node "Community" with the label "name"

# In[ ]:


def query_3I(tx):
    result = tx.run(
        """
        create (:Community {name:'data'}) 
        """
    )
    records = list(result)
    summary = result.consume()
    return records, summary

with conn.driver.session(database="neo4j") as session:
    records, summary = session.execute_read(query_3I)
    print(records, summary)


#     II step is creating the edge "CONSTITUTE"

# In[ ]:


def query_3II(tx):
    result = tx.run(
        """
        match (c:Community {name:'data'})
        match (t:Topic)
        where kw.word in ['Data management', 'Indexing', 'Data modeling', 'Big data', 'Data processing', 'Data storage' , 'Data querying']
        merge (t)-[:CONSTITUTE]->(c)  
        """
    )
    records = list(result)
    summary = result.consume()
    return records, summary

with conn.driver.session(database="neo4j") as session:
    records, summary = session.execute_read(query_3II)
    print(records, summary)


#     III step is creating a "Datacommunity" subgraph

# In[ ]:


def query_3III(tx):
    result = tx.run(
        """
        CALL gds.graph.project.cypher('Datacommunity',
        'MATCH (p:Paper)-[r:RELATED_TO]->(t:Topic)-[c:CONSTITUTE]->(com:Community) WHERE com.name="data" RETURN id(p) AS id',
        'MATCH (com2:Community)<-[c2:CONSTITUTE]-(t2:Topic)<-[r2:RELATED_TO]-(p2:Paper)<-[cit:CITED_BY]-(p1:Paper)-[r1:RELATED_TO]->(t1:Topic)-[c1:CONSTITUTE]->(com1:Community) WHERE com1.name="data" AND com2.name="data" RETURN id(p1) AS source, id(p2) AS target')
        YIELD
        graphName AS graph, nodeQuery, nodeCount AS nodes, relationshipQuery, relationshipCount AS rels  
        """
    )
    records = list(result)
    summary = result.consume()
    return records, summary

with conn.driver.session(database="neo4j") as session:
    records, summary = session.execute_read(query_3III)
    print(records, summary)


#     IV step is performimg the query

# In[ ]:


def query_3IV(tx):
    result = tx.run(
        """
        MATCH (d:Document)<-[w:PUBLISHED_AT]-(p:Paper)-[r:RELATED_TO]->(t:Topic)
        WITH d, t, count(p) as p1, collect(p) as papers
        UNWIND papers as p
        WITH p, d, t, count(p) as p2, p1
        WHERE (t.Keyword CONTAINS 'Data management' OR t.Keyword CONTAINS 'Indexing' OR t.Keyword CONTAINS 'Data modeling' OR t.Keyword CONTAINS 'Big data' OR t.Keyword CONTAINS 'Data processing' OR t.Keyword CONTAINS 'Data storage' OR t.Keyword CONTAINS 'Data querying')
        AND (p2/p1 >=0.9)
        WITH collect (p.Title) as Papers
        CALL gds.pageRank.stream('Datacommunity')
        YIELD nodeId, score
        WHERE gds.util.asNode(nodeId).Title in Papers
        RETURN gds.util.asNode(nodeId).Title as papers, score
        ORDER by score desc
        LIMIT 100   
        """
    )
    records = list(result)
    summary = result.consume()
    return records, summary

with conn.driver.session(database="neo4j") as session:
    records, summary = session.execute_read(query_3IV)
    print(records, summary)


# 4. Finally, an author of any of these top-100 papers is automatically considered a potential good match to review database papers. In addition, we want to identify gurus, i.e., very reputated authors that would be able to review for top conferences. We identify gurus as those authors that are authors of, at least, two papers among the top-100 identified.

# In[ ]:


def query_4(tx):
    result = tx.run(
        """
        MATCH (d:Document)<-[w:PUBLISHED_AT]-(p:Paper)-[r:RELATED_TO]->(t:Topic)
        WITH d, t, count(p) as p1, collect(p) as papers
        UNWIND papers as p
        WITH p, d, t, count(p) as p2, p1
        WHERE (t.Keyword CONTAINS 'Data management' OR t.Keyword CONTAINS 'Indexing' OR t.Keyword CONTAINS 'Data modeling' OR t.Keyword CONTAINS 'Big data' OR t.Keyword CONTAINS 'Data processing' OR t.Keyword CONTAINS 'Data storage' OR t.Keyword CONTAINS 'Data querying')
        AND (p2/p1 >=0.9)
        WITH collect (p.Title) as Papers
        CALL gds.pageRank.stream('Datacommunity')
        YIELD nodeId, score
        WHERE gds.util.asNode(nodeId).Title in Papers
        WITH gds.util.asNode(nodeId) as papers, score
        ORDER by score desc
        LIMIT 100
        MATCH (a:Author)<-[WRITTEN_BY]-(papers)
        WITH a, count(DISTINCT papers) as papers_number
        WHERE papers_number>1
        RETURN DISTINCT a.AuthorName as Gurus  
        """
    )
    records = list(result)
    summary = result.consume()
    return records, summary

with conn.driver.session(database="neo4j") as session:
    records, summary = session.execute_read(query_4)
    print(records, summary)

