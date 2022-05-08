# add_airports.py

import sys, csv, psycopg2

if len(sys.argv) < 2:
    print("Usage: %s <input file>"%sys.argv[0],file=sys.stderr)
    sys.exit(1)

input_filename = sys.argv[1]

psql_user = 'jkalmar'
psql_db = 'jkalmar'
psql_password = ''
psql_server = 'studdb1.csc.uvic.ca'
psql_port = 5432

conn = psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_password,host=psql_server,port=psql_port)

cursor = conn.cursor()

error = 0
with open(input_filename) as f:
    for row in csv.reader(f):
        if len(row) == 0:
            continue #Ignore blank rows
        if len(row) != 4:
            print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
            conn.rollback()
            break
			#Maybe abort the active transaction and roll back at this point?
        airport_code,airport_name,country,international = row
        if international.lower() not in ('true','false'):
            print('Error: Fourth value in each line must be either "true" or "false"',file=sys.stderr)
            conn.rollback()
            break
			#Maybe abort the active transaction and roll back at this point?
        international = international.lower() == 'true'
		#Do something with the data here
		#Make sure to catch any exceptions that occur and roll back the transaction if a database error occurs.
        insert_statement = cursor.mogrify("insert into airports values(%s, %s, %s, %s);", (airport_code, airport_name, country, international))
        try:
            cursor.execute(insert_statement)
        except psycopg2.ProgrammingError as err:
            print("Caught a ProgrammingError:", file = sys.stderr)
            print(err, file=sys.stderr)
            error =1
            break
        except psycopg2.IntegrityError as err:
            print("Caught an IntegrityError:", file = sys.stderr)
            print(err, file=sys.stderr)
            error = 1
            break
        except psycopg2.InternalError as err:
            print("Caught an InternalError:", file = sys.stderr)
            print(err, file=sys.stderr)
            error = 1
            break
        except psycopg2.OperationalError as err:
            print("Caught an OperationalError:", file = sys.stderr)
            print(err, file=sys.stderr)
            error = 1
            break
if error == 1:
    conn.rollback()
else:
    conn.commit()
cursor.close()
conn.close()
