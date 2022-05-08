# report_aircraft.py

import psycopg2, sys

psql_user = 'jkalmar'
psql_db = 'jkalmar'
psql_password = ''
psql_server = 'studdb1.csc.uvic.ca'
psql_port = 5432

conn = psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_password,host=psql_server,port=psql_port)

cursor = conn.cursor()

def print_entry(aircraft_id, airline, model_name, num_flights, flight_hours, avg_seats_full, seating_capacity):
    print("%-5s (%s): %s"%(aircraft_id, model_name, airline))
    print("    Number of flights : %d"%num_flights)
    print("    Total flight hours: %d"%flight_hours)
    print("    Average passengers: (%.2f/%d)"%(avg_seats_full,seating_capacity))

cursor.execute("""with passenger_count as(
select distinct aircraft_id, count(passenger_id) over(partition by aircraft_id) as total_passengers
from aircraft natural join flights natural left outer join reservations)
select distinct aircraft_id, airline_name, model_name, count(flight_id) over(partition by aircraft_id) as num_flights,
	sum((extract(epoch from arrival_time) - extract(epoch from departure_time))/3600) over(partition by aircraft_id) as flight_hours,
	1.0 * total_passengers / count(flight_id) over(partition by aircraft_id) as avg_seats_full, seating_capacity
	from aircraft natural join flights natural join passenger_count
					  order by aircraft_id;
			   """ )

rows_found = 0
while True:
    row = cursor.fetchone()
    if row is None:
        break
    rows_found += 1
    aircraft_id = row[0]
    airline = row[1]
    model_name = row[2]
    num_flights = row[3]
    flight_hours = row[4]
    avg_seats_full = row[5]
    seating_capacity = row[6]
    print_entry(aircraft_id, airline, model_name, num_flights, flight_hours, avg_seats_full, seating_capacity)
