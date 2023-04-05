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
envvals = ["PGUSER", "PGPASSWORD", "PGHOST", "PGPORT", "PGDATABASE"]
dbstring = 'dbname={4} user={0} password={1} host={2} port={3}'

# Since we are asked to pass a class rather than an instance of the class to the
# add_resource method, we open the connection to the database outside the Recom class.
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
        c = c.replace(" ", "-")
        c = c.replace(",", "")
        c = c.replace("'", "")
        c = c.replace("&", "en")
        c = c.replace("ë", "e")
        c = c.replace("=", "-is-")
        c = c.replace("%", "-procent-")
        c = c.replace("--", "-")
        c = urllib.parse.quote(c)
        return c

    def decode_dict(self, cursor, cat_type):
        decode_dict = {}
        cursor.execute(
            f'''
            SELECT DISTINCT {cat_type} FROM product
            WHERE {cat_type} IS NOT NULL
            '''
        )
        categories = [row[0] for row in cursor.fetchall()]
        for category in categories:
            decode_dict[self.encodecategory(category)] = category
        return decode_dict

    def popular(self, cursor, type, id, count):
        cursor.execute(f'''SELECT orders.productproduct_id, COUNT(orders.productproduct_id)
                                               FROM orders
                                               JOIN product on orders.productproduct_id = product.product_id
                                               WHERE recommendable = True AND {type} = '{id}'
                                               GROUP BY orders.productproduct_id
                                               ORDER BY count DESC
                                               LIMIT {count};
                            ''')
        return cursor.fetchall()

    def similar(self, cursor, type, id, count):
        # cursor.execute(f'''SELECT product_id
        #                    FROM product
        #                    WHERE product_id = {pro}
        # ''')
        # cursor.execute(f'''SELECT product_id
        #                    FROM product
        #                    WHERE recommendable = True
        #                    ORDER BY CASE WHEN brand = 'Aquafresh' THEN 1
        # 	                             ELSE 2
        # 	                             END
        # ''', (count,)) TODO: brand/product_id doorgeven
        cursor.execute(f'''SELECT product_id
                                               FROM product
                                               WHERE recommendable = True AND {type} = '{id}'
                                               ORDER BY RANDOM()
                                               LIMIT {count};
                            ''')
        return cursor.fetchall()

    def combination(self, cursor, id, count):
        cursor.execute(f'''SELECT sessionssession_id
                                               FROM orders
                                               WHERE productproduct_id = '{id}'
                                               LIMIT 10;
                            ''')
        sessions_bought_product = [row[0] for row in cursor.fetchall()]
        relevant_sessions = ''''''
        for session in sessions_bought_product:
            if relevant_sessions == '''''':
                relevant_sessions += f'''orders.sessionssession_id = '{session}' '''
            else:
                relevant_sessions += f'''OR orders.sessionssession_id = '{session}' '''
        if relevant_sessions != '''''':
            relevant_sessions = '''AND ''' + relevant_sessions
        cursor.execute(f'''SELECT productproduct_id
                                               FROM orders
                                               JOIN product AS prod ON orders.productproduct_id = prod.product_id
                                               WHERE prod.recommendable = True {relevant_sessions}
                                               ORDER BY Random()
                                               LIMIT {count};
                            ''')
        return cursor.fetchall()

    def behaviour(self,cursor,id,count):
        cursor.execute(f'''SELECT ev.event_product
                           FROM profile prof
                           JOIN identifier AS iden ON prof.profile_id = iden.profileprofile_id
                           JOIN sessions AS ses ON iden.bu_id = ses.bu_id
                           JOIN event AS ev ON ses.session_id = ev.sessionssession_id
                           JOIN product AS prod ON ev.event_product = prod.product_id
                           WHERE prod.recommendable = True AND prof.profile_id = '{id}'
                           LIMIT {count};
        ''')  # TODO: Minimaal 4 garanderen
        return cursor.fetchall()

    def personal(self,cursor,id,count):
        cursor.execute(f'''SELECT preference_type, preference_name
                                               FROM profile prof
                                               JOIN identifier AS iden ON prof.profile_id = iden.profileprofile_id
                                               JOIN sessions AS ses ON iden.bu_id = ses.bu_id
                                               JOIN preferences AS pref ON ses.session_id = pref.Sessionssession_id
                                               WHERE prof.profile_id = '{id}'
                                               LIMIT {count};
                            ''')
        preferences = [[row[0], row[1]] for row in cursor.fetchall()]
        preferred = ''''''
        for preference in preferences:
            if preferred == '''''':
                preferred += f''' {preference[0]} = '{preference[1]}' '''
            else:
                preferred += f'''OR {preference[0]} = '{preference[1]}' '''
        if preferred != '''''':
            preferred = '''AND ''' + preferred
        cursor.execute(f'''SELECT product_id
                                               FROM product
                                               WHERE recommendable = True AND (folder_active = 'Enabled' 
                                                    OR discount IS NOT Null)
                                                    {preferred}
                                               LIMIT {count};
                            ''')
        return cursor.fetchall()
    def get(self, profileid, categories, rtype, count):
        """ This function represents the handler for GET requests coming in
        through the API. It currently returns a random sample of products. """
        cursor = conn.cursor()
        if categories == 'None':
            cursor.execute(f'''SELECT product_id 
                               FROM product 
                               WHERE recommendable = True 
                               ORDER BY RANDOM()
                               LIMIT {count};
            ''')
            ids = cursor.fetchall()
        else:
            if '~' in categories:
                s = list(categories)
                s[categories.index('~')] = "/"
                categories = "".join(s)
            category_name_enc, category_number = categories.split('@')
            # Add 'sub_' category_number-1 times before adding category
            if category_number == '0':
                category_type = 'product_id'
            else:
                category_type = '' + ('sub_' * (int(category_number)-1)) + 'category'
                decoder = self.decode_dict(cursor, category_type)
                category_name_dec = decoder[category_name_enc]
            match rtype:
                # Anderen kochten ook
                case 'popular':
                    ids = self.popular(cursor, category_type, category_name_dec, count)
                # Soortgelijke producten
                case 'similar':
                    ids = self.similar(cursor,category_type,category_name_enc,count)
                # Combineert goed met
                case 'combination': # TODO:fixing in shopping cart
                    ids = self.combination(cursor,category_name_enc, count)
                # Passend bij uw gedrag
                case 'behaviour':
                    ids = self.behaviour(cursor, profileid,count)
                # Persoonlijk aanbevolen
                case 'personal':
                    ids = self.personal(cursor,profileid,count)
        print(ids)
        prodids = [row[0] for row in ids]
        print(prodids)
        cursor.close()
        return prodids, 200


# This method binds the Recom class to the REST API, to parse specifically
# requests in the format described below.
api.add_resource(Recom, "/<string:profileid>/<string:categories>/<string:rtype>/<int:count>")
