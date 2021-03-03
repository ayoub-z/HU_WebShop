import MongodbDAO
import psycopg2

#connect to the db
con = psycopg2.connect('host=localhost dbname=huwebshop user=postgres password=Levidov123')

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

counter = 0
for profile in profiles:
	counter+=1
	if counter < 100:
		id = str(profile["_id"])
		# id = str(profileID)
		if "order" in profile.keys():
			if "recommendations" in profile.keys():
				try:
					cur.execute("INSERT INTO profile (_id, ordercount, segment) VALUES (%s, %s, %s)", (id, profile["order"]["count"],profile["recommendations"]["segment"]))
				except Exception as e:
					# print(counter, id)
					print(counter, e)
			else:
				try:
					cur.execute("INSERT INTO profile (_id, ordercount) VALUES (%s, %s)", (id, profile["order"]["count"]))
				except Exception as e:
					print(counter, e)	

		else:
			if "recommendations" in profile.keys():
				try:
					cur.execute("INSERT INTO profile (_id, segment) VALUES (%s, %s)", (id,profile["recommendations"]["segment"]))
				except Exception as e:
					print(counter, e)
			else:
				try:
					cur.execute("INSERT INTO profile (_id) VALUES (%s)", (id))
				except Exception as e:
					print(counter, e, id)
	else:
		print("Done!")
		cur.execute("SELECT * FROM profile")
		cur.fetchall()
		break
cur.close()





























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


