#!/usr/bin/env python
# coding: utf-8

# # Laboratory 1: Property Graphs
# ### Luis Alfredo Leon Villapún
# ### Liliia Aliakberova
# 
# # Part D Graph Algorithms
# * * *
# In this section we will explode the power of graphs by using some of the suggested algorithms in Neo4j.
# 
# ## Creating the connector
# As in previous parts, let's first create the connector to handle the messages with Neo4j.

# In[1]:


from connector import Neo4jConnector
from getpass import getpass

uri = "neo4j://localhost:7687"
user = "neo4j"
password = getpass("Input your password to connect")
conn = Neo4jConnector(uri, user, password)


# ## Triggering algorithms from the graph-data-science library
# 
# The graph data science library helps us execute common graph algorithms with Neo4j. This can be useful to analyze our graphs from this point of view, more like a graph rather than a database, providing useful information.
# 
# ### Betweeness algorithm
# 
# This is a very interesting algorithm that provides information about the "bridges" on a particular graph. This means, it will return nodes that have key connections in the graph, connection different regions accross it.

# In[6]:


# From documentation site: https://neo4j.com/docs/python-manual/current/

def betweeness_def(tx):
    result = tx.run(
        """
        CALL gds.graph.project('betweeness', 'Paper', {CITED_BY: {properties: 'year'}})
        """
    )
    records = list(result)
    summary = result.consume()
    return records, summary

try:
    with conn.driver.session(database="neo4j") as session:
        records, summary = session.execute_read(betweeness_def)
        print(records, summary)
except Exception as e:
    print("Seems like the graph was already saved on the catalogue.")


def betweeness(tx):
    result = tx.run(
        """
            CALL gds.betweenness.stream('betweeness')
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).Title AS name, score
            ORDER BY name ASC
        """
    )
    records = list(result)
    summary = result.consume()
    return records, summary

with conn.driver.session(database="neo4j") as session:
    records, summary = session.execute_read(betweeness)
    print(records, summary)


# ### Node similarity algorithm
# In this case, we are analysing the similarity between two "Paper" nodes over the edge "RELATED_TO".

# In[7]:


# From documentation site: https://neo4j.com/docs/python-manual/current/

def node_simil_def(tx):
    result = tx.run(
        """
        CALL gds.graph.project(
            'NSPaperTopic',
            ['Paper', 'Topic'],
            {
              RELATED_TO: {}
            }
        );
        """
    )
    records = list(result)
    summary = result.consume()
    return records, summary

try:
    with conn.driver.session(database="neo4j") as session:
        records, summary = session.execute_read(node_simil_def)
        print(records, summary)
except Exception as e:
    print("Seems like the graph was already saved on the catalogue.")
    
def node_simil(tx):
    result = tx.run(
        """
        CALL gds.nodeSimilarity.stream('NSPaperTopic', {bottomK:1})
        YIELD node1, node2, similarity
        RETURN gds.util.asNode(node1).Title AS Paper1, gds.util.asNode(node2).Title AS Paper2, similarity
        ORDER BY similarity DESC
        """
    )
    records = list(result)
    summary = result.consume()
    return records, summary

with conn.driver.session(database="neo4j") as session:
    records, summary = session.execute_read(node_simil)
    print(records, summary)


# In[ ]:




