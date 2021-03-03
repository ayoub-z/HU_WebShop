import MongodbDAO
import psycopg2

#groet Ayoub
#connect to the db
con = psycopg2.connect('host=localhost dbname=huwebshop user=postgres password=test123')

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


all_counter = 0
order_counter = 0
seg_counter = 0
for profile in profiles:
	id = str(profile["_id"])
	# id = str(profileID)
	if "order" in profile.keys():
		order_counter += 1
		if "recommendations" in profile.keys():
			seg_counter += 1
			try:
				cur.execute("INSERT INTO profile (_id, ordercount, segment) VALUES (%s, %s, %s)", (id, profile["order"]["count"],profile["recommendations"]["segment"]))
			except Exception as e:
				# print(counter, id)
				print(e)
		else:
			try:
				cur.execute("INSERT INTO profile (_id, ordercount) VALUES (%s, %s)", (id, profile["order"]["count"]))
			except Exception as e:
				print(e)	
	else:
		if "recommendations" in profile.keys():
			seg_counter += 1
			try:
				cur.execute("INSERT INTO profile (_id, segment) VALUES (%s, %s)", (id,profile["recommendations"]["segment"]))
			except Exception as e:
				print(e)
		else:
			try:
				cur.execute("INSERT INTO profile (_id) VALUES (%s)", (id))
			except Exception as e:
				print(e)
cur.close()



