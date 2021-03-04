import MongodbDAO
import psycopg2

#connect to the db
con = psycopg2.connect('host=localhost dbname=hu_webshop user=postgres password=123')

# informatie tonen over wat data
db = MongodbDAO.getMongoDB()
collectionsNames = db.list_collection_names(no_cursor_timeout=True)
for collectionName in collectionsNames:
	collection = db.get_collection(collectionName)


#cursor
cur = con.cursor()

#zoeken
products = MongodbDAO.getDocuments("products")
profiles = MongodbDAO.getDocuments("profiles").batch_size(5)

sessions = MongodbDAO.getDocuments("sessions")


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

def buidtablebuilder():
	'''This function converts a mongoDB profile entry into an SQL Profile table entry
		it checks which information is available and inserts it correspondingly'''
	cur.execute("select _id from profile")
	data = cur.fetchall()
	usable__profile_id_list = []
	for entry in data:
		usable__profile_id_list.append(entry[0])
	for profile in profiles:
		id = str(profile["_id"])
		if id in usable__profile_id_list:
			if "buids" in profile.keys():
				for buids in profile["buids"]:
					try:
						cur.execute("INSERT INTO buid (_buid, profile_id) VALUES (%s, %s)",(buids, id))
					except:
						print(f'dr is iets kapot {buids}, {id}')

buidtablebuilder()
con.commit()
cur.close()
con.close()