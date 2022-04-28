# manage_reservations.py

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
    cursor.execute("set constraints all deferred;")
    for row in csv.reader(f):
        if error == 1:
            conn.rollback()
            break
        if len(row) == 0:
            continue #Ignore blank rows
        if len(row) != 4:
            print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
            conn.rollback()
            break
        action,flight_id,passenger_id,passenger_name = row

        if action.upper() not in ('CREATE','DELETE'):
            print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
            conn.rollback()
            break
        if action.upper() == 'CREATE':
            statement = cursor.mogrify("insert into passengers values(%s, %s) on conflict (passenger_id) do nothing; insert into reservations values(%s,%s,%s);", (passenger_id, passenger_name, flight_id,passenger_id,passenger_name))
        if action.upper() == 'DELETE':
            statement = cursor.mogrify("delete from reservations where flight_id = %s and passenger_id = %s and passenger_name = %s;", (flight_id,passenger_id,passenger_name))
		#Do something with the data here
		#Make sure to catch any exceptions that occur and roll back the transaction if a database error occurs.
        try:
            cursor.execute(statement)
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
    conn.commit()
cursor.close()
conn.close()
