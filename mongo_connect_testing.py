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
product_orders = MongodbDAO.getDocuments("sessions",{'orders': 'products'})


#products is een Cursor
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
	count = 0
	cur.execute("SELECT buid FROM buid")
	buid_buid = cur.fetchall()
	buid_list = []
	for session in sessions:
		if count < 100:
			count += 1
			session_id = str(session["_id"])
			session_buid = session["buid"]
			sale = session["has_sale"]
			for entry in buid_buid:
				buid_list.append(entry[0])
			if session["buid"] in buid_list:
				if "has_sale" in session.keys():
					if sale == True:
						try:
							cur.execute("INSERT INTO session(_id, buid_buid, has_sale) VALUES (%s, %s, %s)", (session_id, session_buid, sale))
						except Exception as e:
							print(e)
					else:
						continue
				else:
					continue
			else:
				continue
		else:
			break



# #Similar code, inteded for testing until buid table is setup

# count = 0
# for session in sessions:
# 	if count < 100:
# 		count += 1
# 		session_id = str(session["_id"])
# 		session_buid = str(session["buid"])
# 		sale = session["has_sale"]
# 		if "has_sale" in session.keys():
# 			if sale == True:
# 				try:
# 					print('yes!')
# 					cur.execute("INSERT INTO session(_id, buid_buid, has_sale) VALUES (%s, %s, %s)", (session_id, session_buid, sale))
# 				except Exception as e:
# 					print('no', e)
# 			else:
# 				continue
# 		else:
# 			continue
# 	else:
# 		break


def order_insert():
	count = 0
	for session in sessions:
		session_id = str(session["_id"])
		if "has_sale" in session.keys():
			if session["has_sale"] == True:
				for product in product_orders:
					if "Products" in product.keys():
						count+=1
						for product in product_orders:
							try:
								print(count, product)
								cur.execute("INSERT INTO order(orderid, Product_id, session_id) VALUES (%s, %s, %s)", (count, product, session_id))
							except Exception as e:
								print(e)