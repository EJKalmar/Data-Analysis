# report_all_flights.py

import psycopg2, sys

psql_user = 'jkalmar'
psql_db = 'jkalmar'
psql_password = ''
psql_server = 'studdb1.csc.uvic.ca'
psql_port = 5432

conn = psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_password,host=psql_server,port=psql_port)

cursor = conn.cursor()

def print_entry(flight_id, airline, source_airport_name, dest_airport_name, departure_time, arrival_time, duration_minutes, aircraft_id, aircraft_model, seating_capacity, seats_full):
    print("Flight %s (%s):"%(flight_id,airline))
    print("    [%s] - [%s] (%s minutes)"%(departure_time,arrival_time,duration_minutes))
    print("    %s -> %s"%(source_airport_name,dest_airport_name))
    print("    %s (%s): %s/%s seats booked"%(aircraft_id, aircraft_model,seats_full,seating_capacity))

cursor.execute("""select distinct flight_id, airline_name, source_airport, dest_airport, departure_time, arrival_time, (extract(epoch from arrival_time) - extract(epoch from departure_time))/60 as duration_minutes, aircraft_id, model_name, seating_capacity, count(passenger_id) over(partition by flight_id) from flights natural join aircraft natural left outer join reservations
					  order by departure_time;
			   """ )

rows_found = 0
while True:
    row = cursor.fetchone()
    if row is None:
        break
    rows_found += 1
    flight_id = row[0]
    airline = row[1]
    source_airport_name = row[2]
    dest_airport_name = row[3]
    departure_time = row[4]
    arrival_time = row[5]
    duration_minutes = row[6]
    aircraft_id = row[7]
    aircraft_model = row[8]
    seating_capacity = row[9]
    seats_full = row[10]
    print_entry(flight_id, airline, source_airport_name, dest_airport_name, departure_time, arrival_time, duration_minutes, aircraft_id, aircraft_model, seating_capacity, seats_full)
