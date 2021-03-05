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
						cur.execute("INSERT INTO profile (_id, ordercount, segment) VALUES (%s, %s, %s)",
							(profile_id, profile["order"]["count"], profile["recommendations"]["segment"]))
					except Exception as e:
						print(e)
				else:
					try:
						cur.execute("INSERT INTO profile (_id, ordercount) VALUES (%s, %s)", 
														(profile_id, profile["order"]["count"]))
					except Exception as e:
						print(e)
			else:
				if "recommendations" in profile.keys():
					try:
						cur.execute("INSERT INTO profile (_id, segment) VALUES (%s, %s)",
									(profile_id, profile["recommendations"]["segment"]))
					except Exception as e:
						print(e)
				else:
					continue
		else:
			if "recommendations" in profile.keys():
				try:
					cur.execute("INSERT INTO profile (_id, segment) VALUES (%s, %s)",
								(profile_id, profile["recommendations"]["segment"]))
				except Exception as e:
					print(e)
			else:
				continue



def session_filler():
	cur.execute("SELECT _buid FROM buid")
	buid_buid = cur.fetchall() #alle buids in de database
	buid_list = [] #list waarin alle buids komen die in de database zijn
	counter = 0
	unique_exceptionError = 0
	ForeignKey_exceptionError = 0
	buid_anomalie = 0
	overige_error = 0

	for entry in buid_buid:
		buid_list.append(entry[0]) #buids uit database op een bruikbare manier in list brengen.

	print(str(buid_list[0]))
	for session in sessions:
		session_id = str(session["_id"])
		session_buid = str(session["buid"])
		sale = session["has_sale"]
		try:
			cur.execute('INSERT INTO "session" (_id, buid_buid, has_sale) VALUES (%s, %s, %s)', 
															(session_id, session_buid, sale))
			counter += 1
			print(counter)
			con.commit()
		except psycopg2.errors.UniqueViolation:	
			unique_exceptionError += 1
			print(f'{unique_exceptionError} Exception used on {session_id}, {session_buid}')
			con.rollback()
		except psycopg2.errors.ForeignKeyViolation:
			ForeignKey_exceptionError += 1
			print('Buid does not exist. skipping', ForeignKey_exceptionError)
			con.rollback()
		except psycopg2.errors.StringDataRightTruncation:
			buid_anomalie += 1
			con.rollback()
			print(f'Kapot')
		except psycopg2.errors.InFailedSqlTransaction:
			overige_error += 1
			con.rollback()
			print('kapot: overig')
	print(f"Foreignkey Violations: {ForeignKey_exceptionError} \nUnique Violations {unique_exceptionError} \nTe veel buids: {buid_anomalie} \nOverige Errors: {overige_error}")

			
		# if session_buid in buid_list: #checkt of buidid uit session al in de database zit
		# 	print('1')
		# 	if sale == True: 
		# 		print('sale true')
		# 		try: #voegt in de database de session_id, buid_id en dat er een sale is gemaakt



def order_filler():
	'''This function converts the mongoDB session entry 'order' into the SQL 'order' table 
		it checks which information is available and inserts it correspondingly'''
	cur.execute('SELECT _id FROM "session"') 
	session_ids = cur.fetchall() #alle session_ids in de database
	session_ids_list = [] #list waarin alle session_ids komen die in de database zijn 
	
	count = 0 #houdt bij het aantal orders dat niet leeg is
			  #wordt ook meegegeven als orderid

	for entry in session_ids:
		session_ids_list.append(entry[0]) #session_ids uit database op een bruikbare manier in list brengen.

	for session in sessions:
		session_id = session["_id"]
		if session_id in session_ids_list: #checkt of session_id al in de database zit
			if session["has_sale"] == True:
				if "order" in session.keys() and session["order"] != None:
					count+=1					
					try: #voegt in de database de orderid en session_id
						cur.execute('INSERT INTO "order"(orderid, session_id) VALUES (%s, %s)', 
																			(count, session_id))
					except Exception as e:
						print(e)
				else:
					continue
			else:
				continue
		else:
			continue


def product_order_filler():
	'''This function converts the mongoDB session entry 'products' into the SQL 'product_order' table
		it checks which information is available and inserts it correspondingly'''
	cur.execute('SELECT session_id FROM "order"')
	session_ids = cur.fetchall() #alle session_ids in de database
	session_ids_list = [] #list waarin alle session_ids komen die in de database zijn 
	
	count = 0 #houdt bij het aantal orders dat gekochten producten bevat
			  #wordt ook meegegeven als product_ordeer_id en orderoerderid
	for entry in session_ids:
		session_ids_list.append(entry[0]) #session_ids uit database op een bruikbare manier in list brengen.

	for session in sessions:
		if session["has_sale"] == True:
			if session in session_ids_list: #checkt of session_id al in de database zit
				if "order" in session.keys():
					if session["order"] !=None and "products" in session["order"].keys(): #checkt of er ook werkelijk producten zitten in order
						product_orders = session["order"]["products"] #alle producten in een order
						count += 1
						for product in product_orders:					
							try: #voegt in de database de product_order_id, product_id en orderid
								cur.execute("INSERT INTO product_order(product_order_id, product_id, orderorderid) \
																	VALUES (%s, %s, %s,)", (count, product, count)) 
																	# note sure if it will be linked to exact same order
							except Exception as e:
								print(e)
					else:
						continue
				else:
					continue
			else:
				continue
		else:
			continue


def buidtablebuilder():
    '''This function converts a mongoDB profile entry into an SQL Profile table entry
		it checks which information is available and inserts it correspondingly'''

    # Hier wordt een filter toegepast op de mongoDB. De enige nuttige informatie voor deze functie is '_id' en 'buids'
    filterid = {"_id": 1, "buids":1}
    profileids = MongodbDAO.getCollection("profiles").find({}, filterid, no_cursor_timeout=True)

    # Alle Profiles uit SQL worden ingeladen en in een lijst van strings geplaatst
    cur.execute("select _id from profile")
    data = cur.fetchall()
    usable_profile_id_list = []
    for entry in data:
        usable_profile_id_list.append(entry[0])

    count = 0            # lelijke counter voor het beihouden van aantal succesvolle commits

    #voor elk profiel geladen uit Mongo word gekeken of deze al gebruikt is in de SQL profile table. 
	#Zo ja, check of deze een buid heeft, zo ja insert en commit deze informatie 1 voor 1.
    for profile in profileids:
        id = str(profile["_id"])
        try:
            if "buids" in profile.keys():
                for buid in profile["buids"]:
                    try:
                        cur.execute("INSERT INTO buid (_buid, profile_id) VALUES (%s, %s)", (buid, id))
                        con.commit()
                        count += 1
                        print(count)
                    except psycopg2.errors.UniqueViolation:  # exception die de duplicate Buids omzeilt.
                        print(f'Exception used on {id}, {buid}')
                        con.rollback()
                    except psycopg2.errors.ForeignKeyViolation:
                        print('Profile ID does not exist. skipping')
                        con.rollback()
        except KeyError:
            print('geen BUIDS')
            continue



session_insert()