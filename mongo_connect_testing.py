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
	in postgreSQL'''

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
		#we try to set these and if they're not available in mongodb, we set them to None
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

def previously_recommended_filler():
	'''This function fills the previous_recommended table in SQL with values from mongoDB '''

	# get documents, filtered by id and previously_recommended
	filterprev = {"_id":1, "previously_recommended":1}
	profiles_with_prev = MongodbDAO.getCollection("profiles").find({}, filterprev, no_cursor_timeout=True)

	#counters
	skipcounter=0
	insertcounter=0
	private_key_counter=1
	sqlerrorcounter = 0

	#loop through all the profiles
	for profile in profiles_with_prev:
		id = str(profile["_id"]) #convert id to string
		if "previously_recommended" not in profile.keys(): #if no prev, skip entry
			continue
		else:
			for recommendation in profile["previously_recommended"]: #for every product id in prev
				try:												 #try to insert it in SQL database
					print(f'inserting profile_id: {profile["_id"]}, reccomendation: {recommendation}')
					cur.execute("INSERT INTO previously_recommended (previously_recommended_id, profile_id, product_id) VALUES (%s, %s, %s)",
							(private_key_counter, id, recommendation))
					con.commit()
					insertcounter += 1 			#count amount of inserts
					private_key_counter += 1	#simple private key counter
				except psycopg2.errors.ForeignKeyViolation:
					print(f'product id:{recommendation} niet nuttig, skip') #if we find a product thats not in SQL, skip
					skipcounter+=1
					continue
				except psycopg2.errors.InFailedSqlTransaction:				#sometimes SQL transaction fails, except it
					print(f'hier komen we een sql transactie error tegen, skip')
					sqlerrorcounter+=1										#keep count of how often this happens
					con.rollback()										    #look at this if it's significant amount
	con.commit()															#(right now its only ~500 out of 1.4m inserts)
	cur.close()
	con.close()
	print(f'done! skipped:{skipcounter}, inserted: {insertcounter}, sql errors: {sqlerrorcounter}')


def viewed_before_filler():
	'''This function fills the viewed_before table in SQL with values from mongoDB'''

	#get documents, filtered by id and recommendations
	filterprev = {"_id": 1, "recommendations":1}
	profiles_with_recommendations = MongodbDAO.getCollection("profiles").find({}, filterprev, no_cursor_timeout=True)

	#a few counters
	skipcounter = 0
	insertcounter = 0
	private_key_counter = 1
	sqlerrorcounter = 0

	#loop through all the profiles
	for profile in profiles_with_recommendations:
		id = str(profile["_id"]) 					#convert profile to string instead of objectid
		if "recommendations" not in profile.keys(): #if theres no recommendations, skip
			continue
		if "viewed_before" not in profile["recommendations"]: #if we do have recommendations but no viewed before, skip
			continue
		else:
			for viewedproduct in profile["recommendations"]["viewed_before"]:	#for every product thats in viewed before
				try:														    #try to insert it
					print(f'inserting profile_id: {profile["_id"]}, reccomendation: {viewedproduct}')
					cur.execute(
						"INSERT INTO viewed_before (viewed_before_id, profile_id, product_id) VALUES (%s, %s, %s)",
						(private_key_counter, id, viewedproduct))
					con.commit()
					insertcounter += 1		#keep track of succesful inserts
					private_key_counter += 1#we use this to generate a simple private key
				except psycopg2.errors.ForeignKeyViolation: #if we find a product id thats not in our product table, skip
					print(f'product id:{viewedproduct} niet nuttig, skip')
					skipcounter += 1
					continue
				except psycopg2.errors.InFailedSqlTransaction: #sometimes SQL transactions fail, count them
					print(f'hier komen we een sql transactie error tegen, skip')
					sqlerrorcounter += 1
					con.rollback() #this rolls back the transaction and makes sure on the next commit we
								   #wont try to commit the faulty transaction again.
	con.commit()
	cur.close()
	con.close()
	print(f'done! skipped:{skipcounter}, inserted: {insertcounter}, sql errors: {sqlerrorcounter}')
