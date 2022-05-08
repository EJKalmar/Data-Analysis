--create_schema.sql

drop table if exists reservations;
drop table if exists passengers;
drop table if exists flights;
drop table if exists aircraft;
drop table if exists airports;

drop function if exists international_airports_trigger();
drop function if exists airline_name_trigger();
drop function if exists flight_capacity_trigger();
drop function if exists reservation_capacity_trigger();
drop function if exists aircraft_consistency_trigger();
drop function if exists aircraft_consistency_trigger2();
drop function if exists passenger_name_trigger();


create table airports(
	airport_code varchar(3) unique,
	airport_name varchar(255) not null,
	airport_country varchar(255) not null,
	international bool not null,
	primary key(airport_code),
	check(length(airport_code) = 3),
	check(length(airport_name) > 0),
	check(length(airport_country) > 0),
	check(airport_code = upper(airport_code))
	);

create table aircraft(
	aircraft_id varchar(64) unique,
	airline_name varchar(255) not null,
	model_name varchar(255) not null,
	seating_capacity integer not null,
	primary key (aircraft_id),
	check(length(aircraft_id) > 0),
	check(length(airline_name) > 0),
	check(length(model_name) > 0),
	check(seating_capacity >= 0)
	);

create table flights(
	flight_id integer unique,
	airline_name varchar(255) not null,
	source_airport varchar(3) not null,
	dest_airport varchar(3) not null,
	aircraft_id varchar(64) not null,
	departure_time timestamp not null,
	arrival_time timestamp not null,
	primary key (flight_id),
	foreign key (source_airport) references airports(airport_code)
		on delete restrict
		on update cascade,
	foreign key (dest_airport) references airports(airport_code)
		on delete restrict
		on update cascade,
	foreign key (aircraft_id) references aircraft(aircraft_id)
		on delete restrict
		on update cascade,
	check(source_airport <> dest_airport),
	check(departure_time < arrival_time)
	);

create table passengers(
	passenger_id varchar(1000),
	passenger_name varchar(255) not null,
	primary key (passenger_id),
	check(length(passenger_name) > 0)
	);

create table reservations(
	flight_id integer,
	passenger_id varchar(1000),
	passenger_name varchar(255) not null,
	foreign key (flight_id) references flights(flight_id)
		on delete restrict
		on update cascade,
	foreign key (passenger_id) references passengers(passenger_id)
		on delete restrict
		on update cascade,
	primary key (flight_id, passenger_id)
	);

create function international_airports_trigger()
returns trigger as
$BODY$
begin
if (select airport_country from airports where airport_code = new.source_airport)
	<> (select airport_country from airports where airport_code = new.dest_airport)
and ((select international from airports where airport_code = new.source_airport) != 'true'
		or (select international from airports where airport_code = new.dest_airport) != 'true')
then
	raise exception 'Source and destination airports are in different countries but are not both international airports.';
end if;
return new;
end
$BODY$
language plpgsql;

create trigger international_airports_constraint
	after insert or update on flights
	for each row
	execute procedure international_airports_trigger();

create function airline_name_trigger()
returns trigger as
$BODY$
begin
if (select airline_name from aircraft where aircraft_id = new.aircraft_id)
	<> new.airline_name
then
	raise exception 'Airline name of the flight is different from that of the aircraft.';
end if;
return new;
end
$BODY$
language plpgsql;

create trigger airline_name_constraint
	after insert or update on flights
	for each row
	execute procedure airline_name_trigger();

create function flight_capacity_trigger()
returns trigger as
$BODY$
begin
if (select count(*) from reservations where flight_id = new.flight_id) >
	(select seating_capacity from aircraft where aircraft_id = new.aircraft_id)
then
	raise exception 'Seating capacity exceeded.';
end if;
return new;
end
$BODY$
language plpgsql;

create constraint trigger flight_capacity_constraint
	after insert or update on flights
	deferrable initially deferred
	for each row
	execute procedure flight_capacity_trigger();

create function reservation_capacity_trigger()
returns trigger as
$BODY$
begin
if (select count(*) from reservations where flight_id = new.flight_id) >
	(select seating_capacity from aircraft where aircraft_id = (select aircraft_id from flights where flight_id = new.flight_id))
then
	raise exception 'Seating capacity exceeded.';
end if;
return new;
end
$BODY$
language plpgsql;

create constraint trigger reservation_capacity_constraint
	after insert or update on reservations
	deferrable initially deferred
	for each row
	execute procedure reservation_capacity_trigger();

create function aircraft_consistency_trigger()
returns trigger as
$BODY$
begin
if (select previous_arrival from (select flight_id, max(dest_airport) over(order by arrival_time rows between 1 preceding and 1 preceding) as previous_dest,
	min(arrival_time) over(order by arrival_time rows between 1 preceding and 1 preceding) as previous_arrival
	from flights where aircraft_id = new.aircraft_id) as T1 natural join flights where flight_id = new.flight_id) is not null
and
	(extract(epoch from new.departure_time)/60 - (select extract(epoch from previous_arrival) from (select flight_id, max(dest_airport) over(order by arrival_time rows between 1 preceding and 1 preceding) as previous_dest,
	min(arrival_time) over(order by arrival_time rows between 1 preceding and 1 preceding) as previous_arrival
	from flights where aircraft_id = new.aircraft_id) as T1 natural join flights where flight_id = new.flight_id)/60 < 60
or
	(select previous_dest from (select flight_id, max(dest_airport) over(order by arrival_time rows between 1 preceding and 1 preceding) as previous_dest,
	min(arrival_time) over(order by arrival_time rows between 1 preceding and 1 preceding) as previous_arrival
	from flights where aircraft_id = new.aircraft_id) as T1 natural join flights where flight_id = new.flight_id) <> new.source_airport)
then
	raise exception 'Aircraft consistency error.';
end if;
return new;
end
$BODY$
language plpgsql;

create constraint trigger aircraft_consistency_constraint
	after insert or update on flights
	deferrable initially deferred
	for each row
	execute procedure aircraft_consistency_trigger();

create function aircraft_consistency_trigger2()
returns trigger as
$BODY$
begin
if (select next_depart from (select flight_id, max(source_airport) over(order by arrival_time rows between 1 following and 1 following) as next_src,
	min(arrival_time) over(order by arrival_time rows between 1 following and 1 following) as next_depart
	from flights where aircraft_id = new.aircraft_id) as T1 natural join flights where flight_id = new.flight_id) is not null
and
	((select extract(epoch from next_depart) from (select flight_id, max(source_airport) over(order by arrival_time rows between 1 following and 1 following) as next_src,
	min(arrival_time) over(order by arrival_time rows between 1 following and 1 following) as next_depart
	from flights where aircraft_id = new.aircraft_id) as T1 natural join flights where flight_id = new.flight_id)/60 - extract(epoch from new.arrival_time)/60 < 60
or
	(select next_src from (select flight_id, max(source_airport) over(order by arrival_time rows between 1 following and 1 following) as next_src,
	min(arrival_time) over(order by arrival_time rows between 1 following and 1 following) as next_depart
	from flights where aircraft_id = new.aircraft_id) as T1 natural join flights where flight_id = new.flight_id) <> new.dest_airport)
then
	raise exception 'Aircraft consistency error.';
end if;
return new;
end
$BODY$
language plpgsql;

create constraint trigger aircraft_consistency_constraint2
	after insert or update on flights
	deferrable initially deferred
	for each row
	execute procedure aircraft_consistency_trigger2();

create function passenger_name_trigger()
returns trigger as
$BODY$
begin
if (select count (*) from (select distinct passenger_name from reservations where passenger_id = new.passenger_id) as T1) > 1
then
	raise exception 'Passenger name does not match the name on record.';
end if;
return new;
end
$BODY$
language plpgsql;

create constraint trigger passenger_name_constraint
	after insert or update on reservations
	deferrable initially deferred
	for each row
	execute procedure passenger_name_trigger();
