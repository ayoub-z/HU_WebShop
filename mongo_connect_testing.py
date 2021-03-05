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
sessions = MongodbDAO.getDocuments("sessions")



def profile_converter():
	'''This function converts a mongoDB profile entry into an SQL Profile table entry
		it checks which information is available and inserts it correspondingly
		it also prints a teringbende'''
	for profile in profiles:
		profile_id = str(profile["_id"])
		if "order" in profile.keys():
			if "count" in profile["order"].keys():
				if "recommendations" in profile.keys():
					try:
						print("1")
						cur.execute("INSERT INTO profile (_id, ordercount, segment) VALUES (%s, %s, %s)", (profile_id, profile["order"]["count"], profile["recommendations"]["segment"]))
					except Exception as e:
						print('1 ', e)
				else:
					try:
						cur.execute("INSERT INTO profile (_id, ordercount) VALUES (%s, %s)", (profile_id, profile["order"]["count"]))
					except Exception as e:
						print('2 ', e)
			else:
				if "recommendations" in profile.keys():
					try:
						cur.execute("INSERT INTO profile (_id, segment) VALUES (%s, %s)",
									(profile_id, profile["recommendations"]["segment"]))
					except Exception as e:
						print('3 ', e)
				else:
					continue
		else:
			if "recommendations" in profile.keys():
				try:
					cur.execute("INSERT INTO profile (_id, segment) VALUES (%s, %s)",
								(profile_id, profile["recommendations"]["segment"]))
				except Exception as e:
					print('4 ', e)
			else:
				continue



def session_insert():
	cur.execute("SELECT buid FROM buid")
	buid_buid = cur.fetchall()
	buid_list = []
	for session in sessions:
		count += 1
		session_id = str(session["_id"])
		session_buid = session["buid"]
		sale = session["has_sale"]
		for entry in buid_buid:
			buid_list.append(entry[0])
			if session["buid"] in buid_list:
				if sale == True:
					try:
						cur.execute("INSERT INTO session (_id, buid_buid, has_sale) VALUES (%s, %s, %s)", (session_id, session_buid, sale))
					except Exception as e:
						print(e)
				else:
					continue
			else:
				continue



def order_insert():
	'''This function converts the mongoDB session entry 'order' into the SQL 'order' table 
		it checks which information is available and inserts it correspondingly'''
	count = 0
	for session in sessions:
		session_id = str(session["_id"])
		if session["has_sale"] == True:
			if "order" in session.keys() and session["order"] != None:
				count+=1					
				try:
					cur.execute('INSERT INTO "order"(orderid, session_id) VALUES \
												(%s, %s, %s)', (count, session_id))
				except Exception as e:
					print(e)
			else:
				continue
		else:
			continue



def product_order_insert():
	'''This function converts the mongoDB session entry 'products' into the SQL 'product_order' table
		it checks which information is available and inserts it correspondingly'''
	count = 0
	for session in sessions:
		if session["has_sale"] == True:
			if "order" in session.keys():
				if session["order"] !=None and "products" in session["order"].keys():
					product_orders = session["order"]["products"]
					count += 1
					for product in product_orders:					
						try:
							cur.execute("INSERT INTO product_order(product_order_id, product_id, orderorderid) \
																VALUES (%s, %s, %s,)", (count, product, count))
						except Exception as e:
							print(e)
				else:
					continue
			else:
				continue
		else:
			continue

# con.commit()
# cur.close()
# con.close()

