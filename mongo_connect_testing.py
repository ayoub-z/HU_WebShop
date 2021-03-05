import MongodbDAO
import psycopg2

# connect to the db
con = psycopg2.connect('host=localhost dbname=hu_webshop user=postgres password=123')

# informatie tonen over wat data
db = MongodbDAO.getMongoDB()
collectionsNames = db.list_collection_names()
for collectionName in collectionsNames:
    collection = db.get_collection(collectionName)

# cursor
cur = con.cursor()

# zoeken
products = MongodbDAO.getDocuments("products")
profiles = MongodbDAO.getDocuments("profiles")

sessions = MongodbDAO.getDocuments("sessions")


# products is een Cursor
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
                        cur.execute("INSERT INTO profile (_id, ordercount, segment) VALUES (%s, %s, %s)",
                                    (id, profile["order"]["count"], profile["recommendations"]["segment"]))
                    except Exception as e:
                        print(id, e)
                else:
                    try:
                        cur.execute("INSERT INTO profile (_id, ordercount) VALUES (%s, %s)",
                                    (id, profile["order"]["count"]))
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

    #voor elk profiel geladen uit Mongo word gekeken of deze al gebruikt is in de SQL profile table. Zo ja, check of deze een buid heeft, zo ja insert en commit deze informatie 1 voor 1.
    for profile in profileids:
        id = str(profile["_id"])
        if id in usable_profile_id_list:
            if "buids" in profile.keys():
                for buid in profile["buids"]:
                    try:
                        cur.execute("INSERT INTO buid (_buid, profile_id) VALUES (%s, %s)", (buid, id))
                        con.commit()
                        count +=1
                        print(count)
                    except psycopg2.errors.UniqueViolation:      #exception die de duplicate Buids omzeilt.
                        print(f'Exception used on {id}, {buid}')
                        con.rollback()


buidtablebuilder()

cur.close()
con.close()
