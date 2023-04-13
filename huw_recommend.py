import urllib.parse
from flask import Flask, request, session, render_template, redirect, url_for, g
from flask_restful import Api, Resource, reqparse
import os
import psycopg2
from dotenv import load_dotenv
from random import sample

# Create a password.txt with your pgadmin password
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
    def encodecategory(self, c):
        """ This function is copied from huw.py to help us determine
        what category was given to the recommendation engine.
        It takes a string and converts some symbols to make it URL-friendly.
        :vars: self, string(s)
        :returns: string"""
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
        """This function creates a dictionary of all (sub_(sub_))categories,
        by requesting all of that category type.
        It will then construct the dictionary with the encoded form as key, normal form as value.
        This is for decrypting the given categories into ones we can use in the queries.
        :vars: self, database cursor, category type(s)
        :returns: dictionary"""
        decode_dict = {}
        cursor.execute(f'''SELECT DISTINCT {cat_type} FROM product
                           WHERE {cat_type} IS NOT NULL
        ''')
        categories = [row[0] for row in cursor.fetchall()]
        for category in categories:
            decode_dict[self.encodecategory(category)] = category
        return decode_dict

    def popular(self, cursor, cat_type, cat_name, count):
        """This function will return a requested amount of product_id's that are most ordered.
        :vars: self, database cursor, category type(s), category name(s), amount of recommendations(int)
        :returns: list of tuples(l)"""
        cursor.execute(f'''SELECT orders.productproduct_id, COUNT(orders.productproduct_id)
                           FROM orders
                           JOIN product on orders.productproduct_id = product.product_id
                           WHERE recommendable = True AND {cat_type} = '{cat_name}'
                           GROUP BY orders.productproduct_id
                           ORDER BY count DESC
                           LIMIT {3*count};
                            ''')
        popular_items = cursor.fetchall()
        # Return the list with the requested amount of productid's or less if there are not enough items
        return sample(popular_items, min(count, len(popular_items)))

    def similar(self, cursor, prod_id, count):
        """This function will return a requested amount of product_id's that are similar
        to the provided product_id.
        :vars: self, database cursor, product_id(s), amount of recommendations(int)
        :returns: list of tuples(l)"""
        # This query gives the brand, sub_category and product_name of the given product_id
        cursor.execute(f'''SELECT brand, sub_category, product_name
                           FROM product
                           WHERE product_id = '{prod_id}'
                           LIMIT 1;
        ''')
        columns_raw = cursor.fetchall()[0]
        # These are [brand, sub_category, product_name] respectively
        columns = [columns_raw[0], columns_raw[1], columns_raw[2]]
        # This query gives products that have similar properties to the given product_id
        cursor.execute(f'''SELECT product_id
                           FROM product
                           WHERE recommendable = true
                             AND sub_category = '{columns[1]}'
                             AND (brand = '{columns[0]}' 
                             OR COALESCE(brand, LEFT(product_name, 3)) = LEFT('{columns[2]}', 3))
                             AND NOT product_id = '{prod_id}'
                           ORDER BY RANDOM()
                           LIMIT {count};
        ''')
        return cursor.fetchall()

    def combination(self, cursor, productid, count):
        """This function will return a list of product-id's that are frequently bought together
        with the given product-id.
        :vars: class self, database cursor, product id(s), amount of desired recommendations(int)
        :returns: list of tuples(l)"""
        # This query returns the products that are most frequently bought together with the given product_id
        cursor.execute(f'''SELECT orders.productproduct_id, COUNT(orders.productproduct_id)
                           FROM orders
                           JOIN product AS prod ON orders.productproduct_id = prod.product_id
                           WHERE prod.recommendable = True AND NOT orders.productproduct_id = '{productid}' 
                                AND sessionssession_id IN 
                                    (SELECT orders.sessionssession_id
                                     FROM orders
                                     JOIN sessions AS ses ON ses.session_id = orders.sessionssession_id
                                     WHERE orders.productproduct_id = '{productid}'
                                     ORDER BY ses.end_session DESC
                                     LIMIT 100000
                                     )
                           GROUP BY orders.productproduct_id
                           ORDER BY count DESC
                           LIMIT {3*count};
        
        ''')
        combination_products = cursor.fetchall()
        # Return the list with the requested amount of productid's or less if there are not enough items
        return sample(combination_products, min(count, len(combination_products)))

    def behaviour(self, cursor, profile_id, count):
        """This function will return a requested amount of product_id's that have been viewed
        by the user with the given profile_id before.
        :vars: self, database cursor, profile_id(s), amount of recommendations(int)
        :returns: list of tuples(l)"""
        # This query returns products with which the user with a given profile_id has interacted before
        cursor.execute(f'''SELECT ev.event_product
                           FROM profile prof
                           JOIN identifier AS iden ON prof.profile_id = iden.profileprofile_id
                           JOIN sessions AS ses ON iden.bu_id = ses.bu_id
                           JOIN event AS ev ON ses.session_id = ev.sessionssession_id
                           JOIN product AS prod ON ev.event_product = prod.product_id
                           WHERE prod.recommendable = True AND prof.profile_id = '{profile_id}'
                           ORDER BY RANDOM()
                           LIMIT {count};
        ''')
        return cursor.fetchall()

    def personal(self, cursor, profile_id, count):
        """This function will return a list of product-id's that are based on the profile-id's preferences,
        and are currently on sale.
        :vars: class self, database cursor, profile id(s), amount of desired recommendations(int)
        :returns: list of tuples(l)"""
        # This query returns the products that are most consistent with the preferences of the user based
        # on their profile_id
        cursor.execute(f'''SELECT prod.product_id, COUNT(prod.product_id)
                           FROM profile prof
                           JOIN identifier AS iden ON prof.profile_id = iden.profileprofile_id
                           JOIN sessions AS ses ON iden.bu_id = ses.bu_id
                           JOIN preferences AS pref ON ses.session_id = pref.Sessionssession_id
                           JOIN product AS prod ON (pref.preference_name = prod.category
                                                OR pref.preference_name = prod.sub_category
                                                OR pref.preference_name = prod.sub_sub_category
                                                OR pref.preference_name = prod.brand
                                                OR pref.preference_name = prod.gender
                                                OR pref.preference_name = prod.product_type)
                           WHERE prof.profile_id = '{profile_id}' AND recommendable = True 
                                AND (folder_active = 'Enabled' OR discount IS NOT Null)
                           GROUP BY prod.product_id
                           ORDER BY COUNT DESC
                           LIMIT {3*count};
        ''')
        personal_products = cursor.fetchall()
        return sample(personal_products, count)

    def get(self, profileid, categories, rtype, count):
        """This function represents the handler for GET requests coming in
        through the API. If no filters are given it will return random product_id's.
        Otherwise, it will return product_id's based on the filters and the requested
        business rule.
        :vars: self, profile_id(s), categories(s), recommendationtype(s), count(int)
        :returns: list of product"""
        cursor = conn.cursor()
        # If no filters have been given, recommend random products
        if categories == 'None':
            cursor.execute(f'''SELECT product_id 
                               FROM product 
                               WHERE recommendable = True 
                               ORDER BY RANDOM()
                               LIMIT {count};
            ''')
            prod_ids = cursor.fetchall()
        else:
            # Replace '~' with '/' in the category names (the reverse was done in huw.py)
            if '~' in categories:
                s = list(categories)
                s[categories.index('~')] = "/"
                categories = "".join(s)
            category_name_enc, category_number = categories.split('@')
            # If the given category_number is 0, then a product_id was given
            if category_number == '0':
                category_type = 'product_id'
                category_name_dec = category_name_enc
            # Else a (sub_(sub_))category was given, based on category_number
            else:
                # Add 'sub_' category_number-1 times before adding category
                category_type = '' + ('sub_' * (int(category_number)-1)) + 'category'
                # Request the decoder dictionary
                decoder = self.decode_dict(cursor, category_type)
                # Make the category_name_dec useable for SQL queries
                category_name_dec = decoder[category_name_enc]
            # Request product_id's based on the requested business rule
            match rtype:
                # Anderen kochten ook
                case 'popular':
                    prod_ids = self.popular(cursor, category_type, category_name_dec, count)
                # Soortgelijke producten
                case 'similar':
                    prod_ids = self.similar(cursor, category_name_dec, count)
                # Combineert goed met
                case 'combination':
                    prod_ids = self.combination(cursor, category_name_enc, count)
                # Eerder bekeken
                case 'behaviour':
                    prod_ids = self.behaviour(cursor, profileid, count)
                # Persoonlijk aanbevolen
                case 'personal':
                    prod_ids = self.personal(cursor, profileid, count)

        # prod_ids from [(id0,),(id1,),(id2,),(id3,)] to [id0,id1,id2,id3]
        prodids = [prod_tuple[0] for prod_tuple in prod_ids]

        # If not enough recommendations have been given, request popular products in the same category
        if len(prodids) < 4:
            # If the given filter was a product_id
            if 'category' not in category_type:
                cursor.execute(f"""SELECT category 
                                   FROM product
                                   WHERE product_id = '{category_name_dec}' 
                """)
                category = cursor.fetchall()
                prod_ids2 = self.popular(cursor, 'category', category[0][0], 4 - len(prodids))
            else:
                prod_ids2 = self.popular(cursor, category_type, category_name_dec, 4 - len(prodids))
            # Add the remaining product_id's to the prodid list
            for prod_id in prod_ids2:
                prodids.append(prod_id[0])
        cursor.close()
        return prodids, 200


# This method binds the Recom class to the REST API, to parse specifically
# requests in the format described below.
api.add_resource(Recom, "/<string:profileid>/<string:categories>/<string:rtype>/<int:count>")
