#!/usr/bin/env python
# coding: utf-8

# # Laboratory 1: Property Graphs
# ### Luis Alfredo Leon Villapún
# ### Liliia Aliakberova
# 
# # Part B Querying
# * * *
# In this section we will perform the suggested queries
# 
# ## Creating the connector
# As in previous parts, let's first create the connector to handle the messages with Neo4j.

# In[1]:


# Uncomment to install extension
#!pip install ipython-cypher


# In[2]:


get_ipython().run_line_magic('load_ext', 'cypher')


# In[3]:


from connector import Neo4jConnector
from getpass import getpass

uri = "neo4j://localhost:7687"
user = "neo4j"
password = getpass("Input your password to connect")
conn = Neo4jConnector(uri, user, password)


# ## Queries
# 1. Find the top 3 most cited papers of each conference.

# In[4]:


# From documentation site: https://neo4j.com/docs/python-manual/current/

def query_1(tx):
    result = tx.run(
        """
        MATCH (p:Paper)<-[c:CITED_BY]-(a:Paper)-[r:PUBLISHED_AT]->(d:Document)
        WHERE d.DocumentType ="Conference"
        WITH d.ConferenceName AS name, a, count(c) AS citations
        ORDER BY name, citations DESC
        WITH name, collect({paper: a.Title, cited: citations}) AS papers
        RETURN name AS Conference, [p IN papers[..3] | p.paper] AS Papers, [p IN papers[..3] | p.cited] AS Cited
        """
    )
    records = list(result)
    summary = result.consume()
    return records, summary

with conn.driver.session(database="neo4j") as session:
    records, summary = session.execute_read(query_1)
    print(records, summary)


# 2. Find the each conference communities.

# In[5]:


# From documentation site: https://neo4j.com/docs/python-manual/current/

def query_2(tx):
    result = tx.run(
        """
        MATCH (a:Author)<-[w:WRITTEN_BY]-(p:Paper)-[r:PUBLISHED_AT]->(d:Document)
        WITH a,d.ConferenceName as Conference_Name, count(DISTINCT d.Volume) as Editions
        WHERE Editions > 3
        RETURN Conference_Name, collect(a.AuthorName) as Community_member
        """
    )
    records = list(result)
    summary = result.consume()
    return records, summary

with conn.driver.session(database="neo4j") as session:
    records, summary = session.execute_read(query_2)
    print(records, summary)


# 3. Find the impact factors of the journals in your graph

# In[ ]:


# From documentation site: https://neo4j.com/docs/python-manual/current/

def query_3(tx):
    result = tx.run(
        """
        MATCH (d:Document)<-[w:PUBLISHED_AT]-(p:Paper)-[b:CITED_BY]->(p1:Paper)
        WHERE d.DocumentType = 'Journal'
        WITH d.Title as Journal, toInteger(b.Year) as Citation_Year, count(b) AS Citations
        WITH Journal, Citation_Year, Citations, toInteger(Citation_Year)-1 as first, toInteger(Citation_Year)-2 as second
        MATCH (d2:Document)<-[z:PUBLISHED_AT]-(p2:Paper)
        WHERE d2.Title = Journal AND toInteger(p2.Year) IN [first, second]
        WITH Journal, Citation_Year,first, second, Citations, count(DISTINCT d2) AS Publications
        RETURN Journal, Citation_Year, (ToFloat(Citations)/ToFloat(Publications) )as Impact_factor
        ORDER BY Citation_Year DESC
        """
    )
    records = list(result)
    summary = result.consume()
    return records, summary

with conn.driver.session(database="neo4j") as session:
    records, summary = session.execute_read(query_3)
    print(records, summary)


# 4. Find the h-indexes of the authors in your graph

# In[10]:


# From documentation site: https://neo4j.com/docs/python-manual/current/

def query_4(tx):
    result = tx.run(
        """
        MATCH (a:Author)<-[w:WRITTEN_BY]-(p:Paper)-[b:CITED_BY]->(p2:Paper) WITH a, p, count(b) AS citations
        WITH a, p, citations ORDER BY citations DESC
        WITH a, count(p) AS total, collect(citations) AS list
        WITH a, total, list, [x in range(1, size(list)) WHERE x <= list[x - 1] | [list[x - 1], x] ] AS list_hindex
        WITH *, list_hindex[-1][1] AS h_index
        ORDER BY h_index DESC
        RETURN a.AuthorName as Author, h_index
        """
    )
    records = list(result)
    summary = result.consume()
    return records, summary

with conn.driver.session(database="neo4j") as session:
    records, summary = session.execute_read(query_4)
    print(records, summary)


# In[ ]:




