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

    insert_counter = 0
    unique_exceptionError_counter = 0
    ForeignKey_exceptionError_counter = 0
    buid_Exception_counter = 0
    failed_sql_counter= 0



    for session in sessions:

        try:
            session_id = str(session["_id"])
        except:
            print('geen id')

        try:
            session_buid = str(session["buid"])
        except:
            print("Session buid invalid")

        try:
            sale = session["has_sale"]
        except:
            print("Geen sale")

        try:
            order = session["order"]
            if order == None:
                continue
        except:
            print(f'Geen order')

        try:
            cur.execute('INSERT INTO "session" (_id, buid_buid, has_sale) VALUES (%s, %s, %s)',
                        (session_id, session_buid, sale))
            insert_counter += 1
            print(insert_counter)
            con.commit()
        except psycopg2.errors.UniqueViolation:
            unique_exceptionError_counter += 1
            print(f'{unique_exceptionError_counter} Exception used on {session_id}, {session_buid}')
            con.rollback()
        except psycopg2.errors.ForeignKeyViolation:
            ForeignKey_exceptionError_counter += 1
            print('Buid does not exist. skipping', ForeignKey_exceptionError_counter)
            con.rollback()
        except psycopg2.errors.StringDataRightTruncation:
            buid_Exception_counter += 1
            con.rollback()
        except psycopg2.errors.InFailedSqlTransaction:
            failed_sql_counter += 1
            con.rollback()
    print(f'Done! We inserted {insert_counter} documents, got unique exception: {unique_exceptionError_counter} times')
    print(f'Got foreign key error {ForeignKey_exceptionError_counter} times, Too long buid error: {buid_Exception_counter} times, and failed {failed_sql_counter} sql queries.')



def order_filler():

    orderidcounter = 1
    unique_exceptionError_counter = 0
    ForeignKey_exceptionError_counter = 0
    buid_Exception_counter = 0
    failed_sql_counter= 0

    for session in sessions:

        if "order" not in session.keys():
            continue

        try:
            session_id = str(session["_id"])
        except:
            print('geen id')

        try:
            order = session["order"]
            if order == None:
                continue
        except:
            print(f'Geen order')

        try:
            print(f'inserting: {orderidcounter} and session id: {session_id}')
            orderidcounter += 1
            cur.execute('INSERT INTO "order" (orderid, session_id) VALUES (%s, %s)',
                        (orderidcounter, session_id))
            con.commit()
        except psycopg2.errors.UniqueViolation:
            unique_exceptionError_counter += 1
            con.rollback()
        except psycopg2.errors.ForeignKeyViolation:
            ForeignKey_exceptionError_counter += 1
            print('Buid does not exist. skipping', ForeignKey_exceptionError_counter)
            con.rollback()
        except psycopg2.errors.StringDataRightTruncation:
            buid_Exception_counter += 1
            con.rollback()
        except psycopg2.errors.InFailedSqlTransaction:
            failed_sql_counter += 1
            con.rollback()
    print(f'Done! We inserted {orderidcounter} documents, got unique exception: {unique_exceptionError_counter} times')
    print(f'Got foreign key error {ForeignKey_exceptionError_counter} times, Too long buid error: {buid_Exception_counter} times, and failed {failed_sql_counter} sql queries.')


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





def product_order_filler():
	for session in sessions:

		try:
			session_id = str(session["_id"])
		except:
			print('geen id')

		try:
			order = session["order"]
			if order == None:
				continue
		except:
			print(f'Geen order')

		selectquery = 'SELECT orderid FROM "order" where session_id = %s'
		cur.execute(selectquery, (session_id,))
		orderselection = cur.fetchone()
		try:
			for product_id in session["order"]["products"]:
				try:  # try to insert it
					print(f'inserting product_id: {orderselection[0]} {product_id["id"]} and orderid: {id}')
					cur.execute(
						"INSERT INTO product_order (product_order_id, product_id, orderorderid) VALUES (%s, %s, %s)",
						(orderselection[0], product_id["id"]))
					con.commit()
					product_order_id_counter += 1
					insertcounter += 1  # keep track of succesful inserts
				except psycopg2.errors.ForeignKeyViolation:  # if we find a product id thats not in our product table, skip
					print(f'product id:{product_id["id"]} niet nuttig, skip')
					skipcounter += 1
					continue
				except psycopg2.errors.InFailedSqlTransaction:  # sometimes SQL transactions fail, count them
					print(f'hier komen we een sql transactie error tegen, skip')
					sqlerrorcounter += 1
					con.rollback()  # this rolls back the transaction and makes sure on the next commit we
				#wont try to commit the faulty transaction again.
			print(f'Done! we inserted {insertcounter} product/orders, we skipped {skipcounter} items, we got {sqlerrorcounter} sql errors. All good!')
		except KeyError:
			print('Geen order')



order_filler()