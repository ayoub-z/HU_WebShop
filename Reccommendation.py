import psycopg2

#connection to PostgreSQL
con = psycopg2.connect('host=localhost dbname=hu_webshop user=postgres password=123')

#Cursor
cur = con.cursor()

#Example product id for testing purposes
productid = 16202

def available_columns(productid):
    '''This function will return a List filled with strings of available columns in the given ProductID.
    If the list contains less than two entries, return None.'''

    available_list=[]

    productid = str(productid)
    cur.execute("SELECT * FROM product WHERE product._id = %s", (productid,))
    product_id_data = cur.fetchone()

    if product_id_data[category] != None:
        available_list.append("category")
    if product_id_data[sub_category] != None:
        available_list.append("sub_category")
    if product_id_data[doelgroep] != None:
        available_list.append("doelgroep")
    if product_id_data[brand] != None:
        available_list.append("brand")
    if product_id_data[sub_sub_category] != None:
        available_list.append("sub_sub_category")
    if len(available_list) < 2:
        return None
    return available_list


def get_similar_product(productid,available_list):
    '''
    Function that takes a product_ID & available_list

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

get_similar_product(productid)