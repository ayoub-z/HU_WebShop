import MongodbDAO
import psycopg2

#connect to the db
con = psycopg2.connect('host=localhost dbname=huwebshop user=postgres password=12345')

# informatie tonen over wat data
db = MongodbDAO.getMongoDB()
collectionsNames = db.list_collection_names()
for collectionName in collectionsNames:
	collection = db.get_collection(collectionName)


#cursor
cur = con.cursor()

#zoeken
products = MongodbDAO.getDocuments("products")
profiles = MongodbDAO.getDocuments("profiles")
#products is een Cursor
def profile_converter():
	'''This function converts a mongoDB profile entry into an SQL Profile table entry
		it checks which information is available and inserts it correspondingly
		it also prints a teringbende'''
	for profile in profiles:
		print(profile)
		id = str(profile["_id"])
		if "order" in profile.keys():
			if "count" in profile["order"].keys():
				if "recommendations" in profile.keys():
					try:
						cur.execute("INSERT INTO profile (_id, ordercount, segment) VALUES (%s, %s, %s)", (id, profile["order"]["count"], profile["recommendations"]["segment"]))
					except Exception as e:
						print(id, e)
				else:
					try:
						cur.execute("INSERT INTO profile (_id, ordercount) VALUES (%s, %s)", (id, profile["order"]["count"]))
					except Exception as e:
						print(id, e)
			else:
				if "recommendations" in profile.keys():
					try:
						cur.execute("INSERT INTO profile (_id, segment) VALUES (%s, %s)",
									(id, profile["recommendations"]["segment"]))
					except Exception as e:
						print(id, e)
				else:
					continue
		else:
			if "recommendations" in profile.keys():
				try:
					cur.execute("INSERT INTO profile (_id, segment) VALUES (%s, %s)",
								(id, profile["recommendations"]["segment"]))
				except Exception as e:
					print(id, e)
			else:
				continue


con.commit()

cur.close()
con.close()





























# for product in products:
# 	if counter < 3:
# 		print('Category:',product['category'])
# 		print('Gender:',product['gender'])
# 		print('Fast Mover:',product['fast_mover'])
# 		print('Selling Price:',product['price']['selling_price'])
# 		print('Doelgroep:',product['properties']['doelgroep'])
# 		counter += 1
# 		print('\n')
# 	else:
# 		print('done!')
# 		break



# prijzen = 0
# counter = 0

# for product in products:
# 	try:
# 		if type(product['price']['selling_price']) == int and product['price']['selling_price'] > 0:
# 			prijzen += product['price']['selling_price']
# 			counter += 1
# 	except KeyError:
# 		continue


