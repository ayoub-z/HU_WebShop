import MongodbDAO
import psycopg2

#groet Ayoub
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



