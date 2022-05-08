# manage_flights.py

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
        action = row[0]
        if action.upper() == 'DELETE':
            if len(row) != 2:
                print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
                conn.rollback()
                break
                #Maybe abort the active transaction and roll back at this point?
            flight_id = row[1]
            statement = cursor.mogrify("delete from flights where flight_id = %s;", (flight_id,))
        elif action.upper() in ('CREATE','UPDATE'):
            if len(row) != 8:
                print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
                conn.rollback()
                break
                #Maybe abort the active transaction and roll back at this point?
            flight_id = row[1]
            airline = row[2]
            src,dest = row[3],row[4]
            departure, arrival = row[5],row[6]
            aircraft_id = row[7]
            #Handle the "CREATE" and "UPDATE" actions here
            if action.upper() == 'CREATE':
                statement = cursor.mogrify("insert into flights values(%s,%s,%s,%s,%s,%s,%s);", (flight_id, airline, src, dest, aircraft_id, departure, arrival))
            elif action.upper() == 'UPDATE':
                statement = cursor.mogrify("update flights set airline_name = %s, source_airport = %s, dest_airport = %s, departure_time = %s, arrival_time = %s, aircraft_id = %s where flight_id = %s;", (airline, src, dest, departure, arrival, aircraft_id, flight_id))
        else:
            print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
            #Maybe abort the active transaction and roll back at this point?
            break
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
if error == 1:
    conn.rollback()
else:
    conn.commit()
cursor.close()
conn.close()
