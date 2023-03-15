#!/usr/bin/env python
# coding: utf-8

# # Laboratory 1: Property Graphs
# ### Luis Alfredo Leon Villapún
# ### Liliia Aliakberova
# 
# # Part A.3 Evolving the graph
# * * *
# In this section we have to modify some parts of the data and explore how adaptable a property graph is when receiving data with a different structure than the original one. An advantage of the schemaless structure is that we can implement updates and evolve the database without the need of rethinking the structure of the schema.
# 
# ## Creating the connector
# As in part A.2, let's first create the connector to handle the messages with Neo4j.

# In[1]:


from connector import Neo4jConnector
from getpass import getpass

uri = "neo4j://localhost:7687"
user = "neo4j"
password = getpass("Input your password to connect")
conn = Neo4jConnector(uri, user, password)


# ## Evolving the model
# 
# In this section we will perform the necessary queries to transform the data.

# ### Creating the affiliations
# This part will generate random affiliations for each author.

# In[2]:


def update_affiliations(conn):
    query = """
MATCH (a:Author)
WITH a, apoc.coll.randomItem([{name: 'Google', type: 'Company'}, {name: 'UPC', type: 'University'}, {name: 'ULB', type: 'University'}, {name: 'Microsoft', type: 'Company'}, {name: 'Meta', type: 'Company'}, {name: 'Amazon', type: 'Company'}]) AS affiliation
MERGE (o:Organization{Name: affiliation.name})
MERGE (a)-[:AFFILIATED_TO{Type:affiliation.type}]->(o)
    """
    session = conn.driver.session()
    response = list(session.run(query))
    session.close()
    print("Success")


# In[3]:


update_affiliations(conn)


# ### Adding  the content and submission status variables to the reviewed_by edges
# This section consists of a query to update the reviewed_by edges with the information regarding the content of the review, as well as the suggestion made by the reviewer.

# In[4]:


def update_reviews(conn):
    query = """
LOAD CSV WITH HEADERS FROM 'file:///dataset.csv' AS row
MATCH (p:Paper{Title: row.Title})
WITH p, row
MATCH (p)-[r:REVIEWED_BY]->(a: Author)
SET r.Content = apoc.coll.randomItem(["Excellent work.", "Good work.", "Satisfactory work.", "Poor work", "Check the references", "Correct the format."]),
    r.SuggestedDecision = apoc.coll.randomItem(["Approved", "Rejected"])
    """
    session = conn.driver.session()
    response = list(session.run(query))
    session.close()
    print("Success")


# In[5]:


update_reviews(conn)


# As we could see in this exercise, modifying or adding attributes to extend the model is very easy and straightforward, one of the advantages of working with graph databases.

# In[ ]:





# In[ ]:




