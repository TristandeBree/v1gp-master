import urllib.parse
from flask import Flask, request, session, render_template, redirect, url_for, g
from flask_restful import Api, Resource, reqparse
import os
import psycopg2
from dotenv import load_dotenv
from random import sample

with open('password.txt', 'r') as f:
    password = f.read()

app = Flask(__name__)
api = Api(app)

# We define these variables to (optionally) connect to an external PostgreSQL
# instance.
envvals = ["PGUSER","PGPASSWORD","PGHOST","PGPORT","PGDATABASE"]
dbstring = 'dbname={4} user={0} password={1} host={2} port={3}'

# Since we are asked to pass a class rather than an instance of the class to the
# add_resource method, we open the connection to the database outside of the
# Recom class.
load_dotenv()
if os.getenv(envvals[0]) is not None:
    envvals = list(map(lambda x: str(os.getenv(x)), envvals))
    conn = psycopg2.connect(dbstring.format(*envvals))
else:
    conn = psycopg2.connect(database="huwebshop", user="postgres", password=password, host="localhost", port="5432")


class Recom(Resource):
    """ This class represents the REST API that provides the recommendations for
    the webshop. At the moment, the API simply returns a random set of products
    to recommend."""
    def encodecategory(self,c):
        """ This helper function encodes any category name into a URL-friendly
        string, making sensible and human-readable substitutions. """
        c = c.lower()
        c = c.replace(" ","-")
        c = c.replace(",","")
        c = c.replace("'","")
        c = c.replace("&","en")
        c = c.replace("ë","e")
        c = c.replace("=","-is-")
        c = c.replace("%","-procent-")
        c = c.replace("--","-")
        c = urllib.parse.quote(c)
        return c

    def decode_dict(self,cursor,cat_type):
        decode_dict = {}
        cursor.execute(
            f'''
            Select distinct {cat_type} from product
            where {cat_type} is not null
            '''
        )
        categories = [row[0] for row in cursor.fetchall()]
        for category in categories:
            decode_dict[self.encodecategory(category)] = category

        return decode_dict

    def get(self, profileid, categories, rtype,count):
        """ This function represents the handler for GET requests coming in
        through the API. It currently returns a random sample of products. """
        cursor = conn.cursor()
        if categories == 'None':
            cursor.execute('''SELECT product_id 
                              FROM product 
                              WHERE recommendable = True 
                              ORDER BY RANDOM() 
                              LIMIT %s;''', (count,))
        else:
            cursor.execute('''SELECT product_id 
                              FROM product 
                              ORDER BY random() 
                              LIMIT %s;''', (count,)) #change this
        prodids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        print(categories)
        print(rtype)
        return prodids, 200


# This method binds the Recom class to the REST API, to parse specifically
# requests in the format described below.
api.add_resource(Recom, "/<string:profileid>/<string:categories>/<string:rtype>/<int:count>")
