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
sessions = MongodbDAO.getDocuments("sessions")

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


def productconverter():
	'''this converter converts the MongoDB Product document into a table
	in postgreSQL, '''
	skipcounter =0
	for product in products:
		### all the non-nullable variables: ###

		#_id
		product_id = str(product['_id'])

		#name
		if 'name' in product.keys():
			name = product['name']
		else:
			print('product without name, skipping')
			skipcounter+=1
			continue

		# fast mover
		if 'fast_mover' in product.keys():
			fast_mover = product['fast_mover']
		else:
			print('product with incomplete info (missing fast mover), skipping')
			skipcounter += 1
			continue

		# herhaalaankopen
		if 'herhaalaankopen' in product.keys():
			herhaalaankopen = product['herhaalaankopen']
		else:
			skipcounter += 1
			print('Herhaalaankopen niet aanwezig, skipping')
			continue

		# price
		if 'price' in product.keys():
			if 'selling_price' in product['price']:
				if isinstance(product['price']['selling_price'], int):
					if product['price']['selling_price'] < 5:
						print('price too low, invalid, skipping')
						continue
					else:
						selling_price = product['price']['selling_price']
				else:
					print('price is incorrectly formatted, skipping')
					continue
			else:
				print('no selling price available, skipping')
				continue
		else:
			print('no price available, skipping')
			continue

		###nullable variables###
		try:
			brand = product['brand']
		except KeyError:
			brand = None
		try:
			category = product['category']
		except KeyError:
			category = None
		try:
			description = product['description']
		except KeyError:
			description = None
		try:
			doelgroep = product['properties']['doelgroep']
		except KeyError:
			doelgroep = None
		try:
			sub_category = product['sub_category']
		except KeyError:
			sub_category = None
		try:
			sub_sub_category = product['sub_sub_category']
		except KeyError:
			sub_sub_category = None
		try:
			sub_sub_sub_category = product['sub_sub_sub_category']
		except KeyError:
			sub_sub_sub_category = None

		cur.execute(
			"INSERT INTO product (_id, name, brand, category, description, fast_mover, herhaalaankopen, selling_price, doelgroep, sub_category, sub_sub_category, sub_sub_sub_category) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
		(product_id, name, brand, category, description, fast_mover, herhaalaankopen, selling_price, doelgroep, sub_category,
		 sub_sub_category, sub_sub_sub_category))

con.commit()
cur.close()
con.close()

