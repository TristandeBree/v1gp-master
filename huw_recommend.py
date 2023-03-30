# from flask import Flask, request, session, render_template, redirect, url_for, g
# from flask_restful import Api, Resource, reqparse
# import os
# from pymongo import MongoClient
# from dotenv import load_dotenv
# import psycopg2
#
# app = Flask(__name__)
# api = Api(app)
#
# # We define these variables to (optionally) connect to an external MongoDB
# # instance.
# envvals = ["MONGODBUSER","MONGODBPASSWORD","MONGODBSERVER"]
# dbstring = 'mongodb+srv://{0}:{1}@{2}/test?retryWrites=true&w=majority'
#
# # Since we are asked to pass a class rather than an instance of the class to the
# # add_resource method, we open the connection to the database outside of the
# # Recom class.
# load_dotenv()
# if os.getenv(envvals[0]) is not None:
#     envvals = list(map(lambda x: str(os.getenv(x)), envvals))
#     client = MongoClient(dbstring.format(*envvals))
# else:
#     client = MongoClient()
# database = client.huwebshop
#
# class Recom(Resource):
#     """ This class represents the REST API that provides the recommendations for
#     the webshop. At the moment, the API simply returns a random set of products
#     to recommend."""
#
#     def get(self, profileid, count):
#         """ This function represents the handler for GET requests coming in
#         through the API. It currently returns a random sample of products. """
#         randcursor = database.products.aggregate([{ '$sample': { 'size': count } }])
#         prodids = list(map(lambda x: x['_id'], list(randcursor)))
#         return prodids, 200
#
# # This method binds the Recom class to the REST API, to parse specifically
# # requests in the format described below.
# api.add_resource(Recom, "/<string:profileid>/<int:count>")

from flask import Flask, request, session, render_template, redirect, url_for, g
from flask_restful import Api, Resource, reqparse
import os
import psycopg2
from dotenv import load_dotenv
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

    def get(self, profileid, categorys, rtype,count):
        """ This function represents the handler for GET requests coming in
        through the API. It currently returns a random sample of products. """
        cursor = conn.cursor()
        if categorys == 'None':
            cursor.execute("SELECT product_id FROM product where recommendable = True ORDER BY RANDOM() LIMIT %s;", (count,))
        else:
            cursor.execute("select product_id from product where recommendable = True order by random() limit %s;", (count,)) #change this
        prodids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        print(categorys)
        print(rtype)
        return prodids, 200

# This method binds the Recom class to the REST API, to parse specifically
# requests in the format described below.
api.add_resource(Recom, "/<string:profileid>/<string:categorys>/<string:rtype>/<int:count>")
