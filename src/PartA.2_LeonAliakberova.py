#!/usr/bin/env python
# coding: utf-8

# # Laboratory 1: Property Graphs
# ### Luis Alfredo Leon Villapún
# ### Liliia Aliakberova
# 
# # Part A.2 Instantiating / Loading
# * * *
# In this section we are asked to load the data into our desired graph. To do this, we are going to use mainly a modified version of the <a href="https://www.kaggle.com/datasets/dpixton/byu-engineering-publications-in-scopus-201721?resource=download">BYU Engineering Publications in Scopus 2017-21</a> from Kaggle.
# Please note that:  
# - this datasets contain modified information to suit this tasks requirements, so even though a part of this dataset is real, some of the enforced relationships will be fake. Keep in mind this is for the purpose of the lab.
# 
# ## Creating the connector
# Let's first create the connector to handle the messages with Neo4j.

# In[1]:


# Install if needed
# !pip install neo4j
# !pip install pandas


# In[1]:


from connector import Neo4jConnector
from getpass import getpass

uri = "neo4j://localhost:7687"
user = "neo4j"
password = getpass("Input your password to connect")
conn = Neo4jConnector(uri, user, password)


# In[2]:


# Uncomment to drop the database (you will have to rerun the loading cells)
conn.drop()


# ## Loading the CSV into Neo4j
# 
# In this step we are going to define the query to load the csv into the graph database. Note that the csv can generate most of the data from the original source. The only relations that have to be faked are "reviewed_by", since we don't have data on who was the reviewer of a paper, and the relation of "cited_by", because in the dataset the cited by column corresponds to the number of citations, not the actual papers who cited the paper per se.

# In[9]:


def load_byu_csv(conn):
    query = """
        LOAD CSV WITH HEADERS FROM 'file:///dataset.csv' AS row
        MERGE (paper: Paper {Title: row.Title})
        SET paper.Content = row.Link,
            paper.Abstract = row.Abstract,
            paper.Year = row.Year
        WITH row, paper
        UNWIND row.Authors AS authorstr
        UNWIND apoc.text.split(authorstr, ',') AS authorname
        MERGE (author: Author {AuthorName: authorname})
        MERGE (paper)-[:WRITTEN_BY]->(author)
        WITH paper, row WHERE row.Volume IS NOT NULL
        MERGE (doc: Document {DocumentType: row['Document Type'], Title: row['Source title'], ConferenceName: row['General conference name'], Volume: row['Volume'], Year: row['Year']})
        MERGE (paper)-[:PUBLISHED_AT]->(doc)
        WITH paper, row['Index Keywords'] as keywordsstr
        UNWIND apoc.text.split(keywordsstr, ';') AS keyword
        MERGE (topic: Topic {Keyword: keyword})
        MERGE (paper)-[:RELATED_TO]->(topic)
    """
    session = conn.driver.session()
    response = list(session.run(query))
    session.close()
    print("Success")


# In[10]:


load_byu_csv(conn)


# This method loads artificially the citations to the graph.

# In[11]:


def create_citations(conn):
    query="""
        MATCH (paps: Paper)
        WITH COLLECT(paps) AS Papers
        MATCH (paper: Paper)
        WITH toInteger(round(rand()*10)) AS citations, paper, Papers
        WITH paper, citations, apoc.coll.randomItems(Papers, citations) AS cited_by_papers
        UNWIND cited_by_papers as cited_by_paper
        WITH paper, cited_by_paper WHERE cited_by_paper.Title <> paper.Title
        MERGE (paper)-[c:CITED_BY{Year: apoc.coll.randomItem([2018, 2019, 2020, 2021, 2022])}]->(cited_by_paper)
    """
    session = conn.driver.session()
    response = list(session.run(query))
    session.close()
    print("Success")


# In[12]:


create_citations(conn)


# Similarly, we will create artificially the reviewers in the graph.

# In[13]:


def create_reviews(conn):
    query="""
MATCH (paper: Paper)
WITH paper, apoc.coll.randomItem([0, 1, 2, 3, 4]) AS nreviews
MATCH (a2: Author)
WITH COLLECT(a2) AS reviewerPool, paper, nreviews
WITH paper, apoc.coll.randomItems(reviewerPool, nreviews) AS reviewers
UNWIND reviewers AS reviewer
WITH paper, reviewer
OPTIONAL MATCH (paper)-[w:WRITTEN_BY]->(reviewer)
WITH paper, reviewer WHERE w IS NULL
MERGE (paper)-[:REVIEWED_BY]->(reviewer)
    """
    session = conn.driver.session()
    response = list(session.run(query))
    session.close()
    print("Success")


# In[14]:


create_reviews(conn)


# In[ ]:





# In[ ]:




