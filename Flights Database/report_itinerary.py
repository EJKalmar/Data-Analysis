# report_itinerary.py

import psycopg2, sys

if len(sys.argv) < 2:
    print('Usage: %s <passenger id>'%sys.argv[0], file=sys.stderr)
    sys.exit(1)

passenger_id = sys.argv[1]

psql_user = 'jkalmar'
psql_db = 'jkalmar'
psql_password = ''
psql_server = 'studdb1.csc.uvic.ca'
psql_port = 5432

conn = psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_password,host=psql_server,port=psql_port)

cursor = conn.cursor()

def print_header(passenger_id, passenger_name):
    print("Itinerary for %s (%s)"%(str(passenger_id), str(passenger_name)) )

def print_entry(flight_id, airline, source_airport_name, dest_airport_name, departure_time, arrival_time, duration_minutes, aircraft_id, aircraft_model):
    print("Flight %-4s (%s):"%(flight_id, airline))
    print("    [%s] - [%s] (%s minutes)"%(departure_time, arrival_time,duration_minutes))
    print("    %s -> %s (%s: %s)"%(source_airport_name, dest_airport_name, aircraft_id, aircraft_model))

cursor.execute("""select passenger_name from passengers where passenger_id = %s;""", (passenger_id,))
row = cursor.fetchone()
if row is None:
    print('Passenger does not exist.')
    sys.exit(1)
passenger_name = row[0]

print_header(passenger_id, passenger_name)

cursor.execute("""select flight_id, airline_name, source_airport, dest_airport, departure_time, arrival_time,
	extract(epoch from arrival_time)/60 - extract(epoch from departure_time)/60 as duration_minutes, aircraft_id, model_name
	from reservations natural join flights natural join aircraft where passenger_id = %s;
			   """, (passenger_id,) )

rows_found = 0
while True:
    row = cursor.fetchone()
    if row is None:
        if rows_found == 0:
            print('No reservations found.')
            sys.exit(1)
        else:
            break
    rows_found += 1
    print_entry(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8])
