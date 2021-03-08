import psycopg2

#connect to the db
con = psycopg2.connect('host=localhost dbname=hu_webshop user=postgres password=123')

#cursor
cur = con.cursor()

productid = 16202

def get_similar_product(productid):
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
    print(product_id_data)
    sqlquery = "SELECT * FROM product where category = %s"
    filtertuple = [product_id_data[category]]
    for entity in product_id_data:
        print(entity)

    if product_id_data[sub_category] != None:
        sqlquery += " and sub_category = %s "
        filtertuple.append(product_id_data[sub_category])
    if product_id_data[doelgroep] != None:
        sqlquery += "and doelgroep = %s "
        filtertuple.append(product_id_data[doelgroep])

    filtertuple = tuple(filtertuple)
    #print(f' sql statement: {sqlquery, filtertuple}')
    cur.execute(sqlquery,filtertuple)
    similar_products_data = cur.fetchall()
    #for row in similar_products_data:
        #print(f'id:{row[id]} | fm:{row[fast_mover]} | dg: {row[doelgroep]} | cg: {row[category]} | sub: {row[sub_category]} | subsub: {row[sub_sub_category]} ')

get_similar_product(productid)