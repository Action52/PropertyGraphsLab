"""
Simple connector script. Creates a connection to a Neo4j server.
References:
   - https://neo4j.com/developer/python/
   - https://towardsdatascience.com/create-a-graph-database-in-neo4j-using-python-4172d40f89c4
"""

from neo4j import GraphDatabase
import logging


class Neo4jConnector:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        logging.basicConfig(filename="connector.log")

    def close(self):
        self.driver.close()

    def query(self, query):
        session = None
        response = None
        try:
            session = self.driver.session()
            response = list(session.run(query))
        except Exception as e:
            logging.error(f"Query Failed: {e}")
        finally:
            if session is not None:
                session.close()
            return response

    def drop(self):
        self.query(query="MATCH (n) -[e] -> () DELETE n, e")
        self.query(query="MATCH (n) DELETE n")
        logging.info("Deleted database. Success.")