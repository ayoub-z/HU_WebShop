import psycopg2
from more_itertools import powerset #import die gebruikt wordt om een powerset te maken
import random

#connection to PostgreSQL
con = psycopg2.connect('host=localhost dbname=hu_webshop user=postgres password=123')

#Cursor
cur = con.cursor()

#Example product id for testing purposes
productid = 16202

def available_product_columns(productid):
    '''This function will return a List filled with strings of available columns in the given ProductID.
    The columns that will get checked are: Category, brand, sub_category, sub_sub_category,
    If the list contains less than two entries, return None.'''

    #Bunch of variables, only used for indexing
    id, naam, brand, category,description,fast_mover,herhaalaankopen,selling_price,doelgroep,sub_category,sub_sub_category,sub_sub_sub_category=0,1,2,3,4,5,6,7,8,9,10,11

    prod_prop_list=[]

    #Fetch product from PostgreSQL
    productid = str(productid)
    cur.execute("SELECT * FROM product WHERE product._id = %s", (productid,))
    product_id_data = cur.fetchone()

    if product_id_data[category] != None:
        prod_prop_list.append("category")
    if product_id_data[sub_category] != None:
        prod_prop_list.append("sub_category")
    if product_id_data[doelgroep] != None:
        prod_prop_list.append("doelgroep")
    if product_id_data[brand] != None:
        prod_prop_list.append("brand")
    if product_id_data[sub_sub_category] != None:
        prod_prop_list.append("sub_sub_category")
    if len(prod_prop_list) < 2:     #If length of list > 2, I won't use it
        return None
    return prod_prop_list

def tablemaker():
    '''Function that creates PostgreSQL tables, based on sets that contain atleast 2 objects, for products in PostgreSQL'''

    all_filter_column= ['category', 'brand', 'sub_category', 'sub_sub_category']                  # List containing all columns in products, used for making recommendations
    product_sets = [set for set in list(map(list, powerset(all_filter_column))) if len(set) >= 2] # list containing  list based on all_filter_column, where the list contains 2 or more items.


    #loop to make product filter tables
    for set in product_sets:
        SQL_skeerie = "Create table "
        for filter in set:
            SQL_skeerie += filter
        SQL_skeerie += " (productid varchar(255) NOT NULL PRIMARY KEY, reco1 varchar(255), reco2 varchar(255), reco3 varchar(255), reco4 varchar(255) );"
        cur.execute(SQL_skeerie)
        print(SQL_skeerie)
    con.commit()

def table_elimination():
    '''function to cascade drop delete eliminate destroy tables made in function tablemaker'''
    cur.execute("DROP TABLE IF EXISTS brandsub_category ; DROP TABLE IF EXISTS brandsub_categorysub_sub_category CASCADE;  DROP TABLE IF EXISTS categorybrand CASCADE;  DROP TABLE IF EXISTS categorybrandsub_category CASCADE; DROP TABLE IF EXISTS categorybrandsub_categorysub_sub_category CASCADE; DROP TABLE IF EXISTS categorybrandsub_sub_category CASCADE;DROP TABLE IF EXISTS categorysub_category CASCADE; DROP TABLE IF EXISTS categorysub_categorysub_sub_category CASCADE; DROP TABLE IF EXISTS categorysub_sub_category CASCADE; DROP TABLE IF EXISTS sub_categorysub_sub_category CASCADE;  DROP TABLE IF EXISTS brandsub_sub_category CASCADE;")
    con.commit()

def producttable_filler():
    '''Function that loops though all products and fills the tables according to what details are known about the product.'''
    # Bunch of variables, only used for indexing
    id, naam, brand, category, description, fast_mover, herhaalaankopen, selling_price, doelgroep, sub_category, sub_sub_category, sub_sub_sub_category = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11

    cur.execute("SELECT * FROM product")
    producten = cur.fetchall()
    for product in producten:
            #brandsub_catergory
            #try to select all _id with similiar brand and sub_category
            try:
                filtertuple =  (product[brand],product[sub_category],product[id])
                cur.execute("SELECT _id FROM product WHERE brand = %s AND sub_category = %s AND NOT _id = %s", (filtertuple))
                similiar_prod_list = cur.fetchall()
            except psycopg2.errors.InFailedSqlTransaction:
                con.rollback()
                print(f' SQL error @ {product[id]}')
            # This try & except picks 4 random entries from the similiar_prod_list, and inserts them into the corresponding table. If there's an error, continue to the next product
            try:
                four_prod = random.sample(similiar_prod_list, 4)
            except:
                continue
            inserttuple=[product[id],]
            for prod in four_prod:
                inserttuple.append(prod[0])
            inserttuple= tuple(inserttuple)
            try:
                cur.execute('INSERT INTO "brandsub_category" (productid, reco1, reco2, reco3, reco4) VALUES (%s, %s, %s, %s, %s)', inserttuple)
                con.commit()
                print("succes mattie")
            except:
                uniquevio=0


def get_similar_product(productid,prod_prop_list):
    '''
    Function that takes a product_ID & prod_prop_list

    '''
    id = 0
    naam = 1
    brand = 2
    category = 3
    description = 4
    fast_mover = 5
    herhaalaankopen = 6
    selling_price = 7
    doelgroep = 8
    sub_category = 9
    sub_sub_category = 10
    sub_sub_sub_category = 11

    productid = str(productid)
    cur.execute("SELECT * FROM product WHERE product._id = %s",(productid,))
    product_id_data = cur.fetchone()

    sqlquery = "SELECT * FROM product where category = %s"
    filtertuple = [product_id_data[category]]

    if product_id_data[sub_category] != None:
        sqlquery += " and sub_category = %s "
        filtertuple.append(product_id_data[sub_category])
    if product_id_data[doelgroep] != None:
        sqlquery += "and doelgroep = %s "
        filtertuple.append(product_id_data[doelgroep])
    if product_id_data[brand] != None:
        sqlquery += "and brand = %s "
        filtertuple.append(product_id_data[brand])
    if product_id_data[sub_sub_category] != None:
        sqlquery += "and sub_sub_category = %s "
        filtertuple.append(product_id_data[sub_sub_category])

    filtertuple = tuple(filtertuple)

    cur.execute(sqlquery,filtertuple)
    similar_products_data = cur.fetchall()
    for row in similar_products_data:
        print(f'id:{row[id]} | name:{row[naam]} | fm:{row[fast_mover]} | dg: {row[doelgroep]} | cg: {row[category]} | sub: {row[sub_category]} | subsub: {row[sub_sub_category]} ')

producttable_filler()
con.commit()

