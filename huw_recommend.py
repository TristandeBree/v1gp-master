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
                           LIMIT {3*count};
                            ''')
        popular_items = cursor.fetchall()
        # Return the list with the requested amount of productid's or less if there are not enough items
        return sample(popular_items, min(count, len(popular_items)))

    def similar(self, cursor, type, id, count):
        cursor.execute(f'''SELECT brand, sub_category, product_name
                           FROM product
                           WHERE {type} = '{id}'
                           LIMIT 1;
        ''')
        columns_raw = cursor.fetchall()[0]
        columns = [columns_raw[0], columns_raw[1], columns_raw[2]]
        cursor.execute(f'''SELECT product_id
                           FROM product
                           WHERE recommendable = true
                             AND sub_category = '{columns[1]}'
                             AND (brand = '{columns[0]}' 
                             OR COALESCE(brand, LEFT(product_name, 3)) = LEFT('{columns[2]}', 3))
                           ORDER BY RANDOM()
                           LIMIT {count};
        ''')
        return cursor.fetchall()

    def combination(self, cursor, productid, count):
        """This function will return a list of product-id's that are frequently bought together
        with the given product-id.
        :vars: class self, database cursor, product id, amount of desired recommendations
        :returns: list of product-id's with length maximum of count
        """
        # This query returns the 100 most recent sessions in which the product was purchased
        cursor.execute(f'''SELECT orders.sessionssession_id, ses.end_session
                           FROM orders
                           JOIN sessions AS ses ON ses.session_id = orders.sessionssession_id
                           WHERE orders.productproduct_id = '{productid}'
                           ORDER BY ses.end_session DESC
                           LIMIT 100;
        ''')
        sessions_bought_product = [row[0] for row in cursor.fetchall()]
        relevant_sessions = ''''''
        for session in sessions_bought_product:
            if relevant_sessions == '''''':
                relevant_sessions += f'''orders.sessionssession_id = '{session}' '''
            else:
                relevant_sessions += f'''OR orders.sessionssession_id = '{session}' '''
        if relevant_sessions != '''''':
            relevant_sessions = '''AND (''' + relevant_sessions + ''')'''
        cursor.execute(f'''SELECT orders.productproduct_id, COUNT(orders.productproduct_id)
                           FROM orders
                           JOIN product AS prod ON orders.productproduct_id = prod.product_id
                           WHERE prod.recommendable = True AND NOT orders.productproduct_id = '{productid}' 
                                {relevant_sessions}
                           GROUP BY orders.productproduct_id
                           ORDER BY count DESC
                           LIMIT {3*count};
        ''')
        combination_products = cursor.fetchall()
        # Return the list with the requested amount of productid's or less if there are not enough items
        return sample(combination_products, min(count, len(combination_products)))

    def behaviour(self, cursor, id, count):
        cursor.execute(f'''SELECT ev.event_product
                           FROM profile prof
                           JOIN identifier AS iden ON prof.profile_id = iden.profileprofile_id
                           JOIN sessions AS ses ON iden.bu_id = ses.bu_id
                           JOIN event AS ev ON ses.session_id = ev.sessionssession_id
                           JOIN product AS prod ON ev.event_product = prod.product_id
                           WHERE prod.recommendable = True AND prof.profile_id = '{id}'
                           ORDER BY RANDOM()
                           LIMIT {count};
        ''')
        return cursor.fetchall()

    def personal(self, cursor, profile_id, count):
        """This function will return a list of product-id's that are based on the profile-id's preferences,
        and have currently an action.
        :vars: class self, database cursor, profile id, amount of desired recommendations
        :returns: list of product-id's with length maximum of count
        """
        # This query returns all the column from the table 'product' in the database
        cursor.execute(f'''SELECT column_name
                           FROM INFORMATION_SCHEMA.COLUMNS
                           WHERE TABLE_NAME = 'product';
        ''')
        existing_product_columns = [row[0] for row in cursor.fetchall()]
        # This query returns all the preference associated with a profile-id
        cursor.execute(f'''SELECT preference_type, preference_name
                           FROM profile prof
                           JOIN identifier AS iden ON prof.profile_id = iden.profileprofile_id
                           JOIN sessions AS ses ON iden.bu_id = ses.bu_id
                           JOIN preferences AS pref ON ses.session_id = pref.Sessionssession_id
                           WHERE prof.profile_id = '{profile_id}'
                           LIMIT 100;
        ''')
        preferences = [[row[0], row[1]] for row in cursor.fetchall()]
        # This makes part of a SQL-statement to include the preferences of the profile-id in the next statement
        preferred = ''''''
        for preference in preferences:
            if preference[0] in existing_product_columns:
                if preferred == '''''':
                    preferred += f''' {preference[0]} = '{preference[1]}' '''
                else:
                    preferred += f'''OR {preference[0]} = '{preference[1]}' '''
        if preferred != '''''':
            preferred = '''AND (''' + preferred + ''')'''
        # This query returns product-id's that have one of the preferences and an active action
        cursor.execute(f'''SELECT product_id
                           FROM product
                           WHERE recommendable = True AND (folder_active = 'Enabled' 
                                OR discount IS NOT Null)
                                {preferred}
                           LIMIT {3*count};
                            ''')
        personal_products = cursor.fetchall()
        return sample(personal_products, count)

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
                category_name_dec = category_name_enc
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
                    ids = self.similar(cursor, category_type, category_name_dec, count)
                # Combineert goed met
                case 'combination':
                    ids = self.combination(cursor, category_name_enc, count)
                # Passend bij uw gedrag
                case 'behaviour':
                    ids = self.behaviour(cursor, profileid, count)
                # Persoonlijk aanbevolen
                case 'personal':
                    ids = self.personal(cursor, profileid, count)
        prodids = [row[0] for row in ids]
        if len(prodids) < 4:
            if 'category' not in category_type:
                cursor.execute(f"""
                select category from product
                where product_id = '{category_name_dec}' 
                """)
                category = cursor.fetchall()
                ids = self.popular(cursor, 'category', category[0][0], 4 - len(prodids))
            else:
                ids = self.popular(cursor,category_type,category_name_dec,4 - len(prodids))
            for id in ids:
                prodids.append(id[0])
        cursor.close()
        return prodids, 200


# This method binds the Recom class to the REST API, to parse specifically
# requests in the format described below.
api.add_resource(Recom, "/<string:profileid>/<string:categories>/<string:rtype>/<int:count>")
