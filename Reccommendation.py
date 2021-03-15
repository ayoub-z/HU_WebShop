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

def prod_tablemaker():
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

    # Also makes another table I couldn't be bothered to make anywhere else
    # Create table for profile based reco's
    cur.execute("Create Table VB_reco (profile varchar(255) NOT NULL PRIMARY KEY, reco1 varchar(255), reco2 varchar(255), reco3 varchar(255), reco4 varchar(255) );")
    con.commit()

def table_elimination():
    '''function to cascade drop delete eliminate destroy tables made in function tablemaker'''
    cur.execute("DROP TABLE IF EXISTS brandsub_category ; DROP TABLE IF EXISTS brandsub_categorysub_sub_category CASCADE;  DROP TABLE IF EXISTS categorybrand CASCADE;  DROP TABLE IF EXISTS categorybrandsub_category CASCADE; DROP TABLE IF EXISTS categorybrandsub_categorysub_sub_category CASCADE; DROP TABLE IF EXISTS categorybrandsub_sub_category CASCADE;DROP TABLE IF EXISTS categorysub_category CASCADE; DROP TABLE IF EXISTS categorysub_categorysub_sub_category CASCADE; DROP TABLE IF EXISTS categorysub_sub_category CASCADE; DROP TABLE IF EXISTS sub_categorysub_sub_category CASCADE;  DROP TABLE IF EXISTS brandsub_sub_category CASCADE;")
    con.commit()

def brandsub_filler():
    '''Function that loops though all products and fills the tables according to what details are known about the product.'''
    # Bunch of variables, only used for indexing
    id, naam, brand, category, description, fast_mover, herhaalaankopen, selling_price, doelgroep, sub_category, sub_sub_category, sub_sub_sub_category = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11

    #select all products, and start looping through the products
    cur.execute("SELECT * FROM product")
    producten = cur.fetchall()
    for product in producten:
            #brandsub_catergory
            #try to select all _id with similiar brand and sub_category. If there's a SQL transaction error, roll back cursor and continue to next product.
            try:
                filtertuple =  (product[brand],product[sub_category],product[id])
                cur.execute("SELECT _id FROM product WHERE brand = %s AND sub_category = %s AND NOT _id = %s", (filtertuple))
                similiar_prod_list = cur.fetchall()
            except psycopg2.errors.InFailedSqlTransaction:
                con.rollback()
                print(f' SQL error @ {product[id]}')
                continue
            # This try & except picks 4 random entries from the similiar_prod_list, and inserts them into the corresponding table. If there's an error, continue to the next product
            try:
                four_prod = random.sample(similiar_prod_list, 4)
            except:
                continue
            #The remainder of this function is made to insert the propper data into postgreSQL
            inserttuple=[product[id],]
            for prod in four_prod:
                inserttuple.append(prod[0])
            inserttuple= tuple(inserttuple)
            try:
                cur.execute('INSERT INTO "brandsub_category" (productid, reco1, reco2, reco3, reco4) VALUES (%s, %s, %s, %s, %s)', inserttuple)
                con.commit()
                print(f' succes met {product[id]}')
            except Exception as e:
                print(e)

def categorybrandsub_categorysub_sub_category_filler():
    '''Function that loops though all products and fills the tables according to what details are known about the product.'''
    # Bunch of variables, only used for indexing
    id, naam, brand, category, description, fast_mover, herhaalaankopen, selling_price, doelgroep, sub_category, sub_sub_category, sub_sub_sub_category = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11

    #select all products, and start looping through the products
    cur.execute("SELECT * FROM product")
    producten = cur.fetchall()
    for product in producten:
            #categorybrandsub_categorysub_sub_category
            #try to select all _id with similiar category, brand, sub_category, sub_sub_category. If there's a SQL transaction error, roll back cursor and continue to next product.
            try:
                filtertuple =  (product[brand],product[sub_category],product[sub_sub_category], product[category],product[id])
                cur.execute("SELECT _id FROM product WHERE brand = %s AND sub_category = %s AND sub_sub_category = %s AND category = %s AND NOT _id = %s", (filtertuple))
                similiar_prod_list = cur.fetchall()
            except psycopg2.errors.InFailedSqlTransaction:
                con.rollback()
                print(f' SQL error @ {product[id]}')
                continue
            # This try & except picks 4 random entries from the similiar_prod_list, and inserts them into the corresponding table. If there's an error, continue to the next product
            try:
                four_prod = random.sample(similiar_prod_list, 4)
            except Exception as e:
                print(f'Error {e} , when trying to select 4 random products from similiar_prod_list.')
                continue
            #The remainder of this function is made to insert the propper data into postgreSQL
            inserttuple=[product[id],]
            for prod in four_prod:
                inserttuple.append(prod[0])
            inserttuple= tuple(inserttuple)
            try:
                cur.execute('INSERT INTO "categorybrandsub_categorysub_sub_category" (productid, reco1, reco2, reco3, reco4) VALUES (%s, %s, %s, %s, %s)', inserttuple)
                con.commit()
                print(f' succes met {product[id]}')
            except Exception as e:
                con.rollback()
                print(e)

def VB_reco_filler():
    '''Function that finds all profiles that have viewed the same product as the given profile. ( limited to a single product from the original profile )
    From that list of profiles, all products that those profiles have also viewed before will be made, with a counter for each product.
    Function returns the 4 most viewed products found, excluding the original product.
    VB is an abreviation I used for  "Viewed Before "         '''

    profileid=('5a393eceed295900010386a8',)
    #Try & except to try and retrieve a singular product from the given profile.
    try:
        cur.execute("SELECT product_id FROM viewed_before WHERE profile_id = %s",profileid)
        VBproduct = cur.fetchone()
    except:
        print("Error in het ophalen van product_id uit het originele profile.")
        con.rollback()

    #Try & except to try and retrieve all profile ID's that have viewed VBproduct before.
    try:
        cur.execute("SELECT profile_id FROM viewed_before where product_id = %s", VBproduct)
        VB_prof_list = cur.fetchall()
    except exception as e:
        print(f'Error {e} , when trying to select profile IDs.')

    #dictionairy product counter
    also_VB_prod_count = {}

    for profile in VB_prof_list:

        #Try & except to try and retrieve all product ID's in profile.
        try:
            cur.execute("SELECT product_id FROM viewed_before where profile_id = %s", profile)
            VB = cur.fetchall()
        except Exception as e:
            print(f'Error {e} , when trying to select product IDs contained in profile.')

        #for-loop, adding all products in profile to also_VB_prod_count
        for prod in VB:

            #if product is original product, skip
            if prod == VBproduct:
                continue

            #if product already in dict, add 1 to its value
            if prod[0] in also_VB_prod_count:
                also_VB_prod_count[prod[0]] += 1
                print(" +1 life")

            #if product not yet in dict, add key to dict and 1 to value
            elif prod[0] not in also_VB_prod_count:
                also_VB_prod_count[prod[0]] = 1
                print(" new life")
    #Reverse sort dict to be able to take the top 4 values.
    sorted(also_VB_prod_count.items(), key=lambda x: x[1], reverse=True)

    #create tuple that's
    inserttuple=[profile[0]]
    for prod in also_VB_prod_count:
        if len(inserttuple)<5:
            inserttuple.append(prod)
        else:
            break
    inserttuple = tuple(inserttuple)
    print(inserttuple)

    #insert data into VB_RECO
    try:
        cur.execute('INSERT INTO "vb_reco" (profile, reco1, reco2, reco3, reco4) VALUES (%s, %s, %s, %s, %s)', inserttuple)
        con.commit()
    except Exception as e:
        print(f'Error {e} , when trying to insert data into VB_reco.')











VB_reco_filler()