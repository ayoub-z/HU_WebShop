import psycopg2

#connection to PostgreSQL
con = psycopg2.connect('host=localhost dbname=hu_webshop user=postgres password=123')

#Cursor
cur = con.cursor()

#Example product id for testing purposes
productid = 16202

def available_product_columns(productid):
    '''This function will return a List filled with strings of available columns in the given ProductID.
    The columns that will get checked are: Category, brand, sub_category, sub_sub_category, sub_sub_sub_category
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

def table_check():


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


print(available_product_columns(productid))