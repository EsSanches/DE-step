-- установка расширения для подключения к другой базе
create extension postgres_fdw;

-- подключение к источнику(к серверу)
create server flight foreign data wrapper postgres_fdw options (
host 'localhost',
dbname 'demo',
port '5432'
);

-- создание пользователя для подключени к базе данных
create user mapping for postgres server flight options (
user 'postgres',
password 'SMR13'
);

-- виртуальная схема
drop schema if exists air_src cascade;
create schema air_src authorization postgres;

-- привязка схемы
import foreign schema bookings from server flight into air_src;

____________________________________________________________________________________________________________________

create schema stg;

--создание таблиц в stg слое

drop table if exists stg.airports_data;
CREATE TABLE stg.airports_data (
	airport_code bpchar(3) NOT NULL,
	airport_name jsonb NOT NULL,
	city jsonb NOT NULL,
	coordinates point NOT NULL,
	timezone text NOT null,
	last_update timestamp NOT NULL,
	deleted timestamp null,
	airport_id int not null,
	hubairportshashkey varchar(32) not null,
	loadDate timestamp not null,									-- дата загрузки записи в систему
	recordSource varchar(50) not null									-- код источника из которой загружена запись	
	);

drop table if exists stg.flights;
CREATE TABLE stg.flights (
	flight_id int NOT NULL,
	airport_id int not null,
	flight_no bpchar(6) NOT NULL,
	scheduled_departure timestamptz NOT NULL,
	scheduled_arrival timestamptz NOT NULL,
	departure_airport bpchar(3) NOT NULL,
	arrival_airport bpchar(3) NOT NULL,
	status varchar(20) NOT NULL,
	aircraft_code bpchar(3) NOT NULL,
	actual_departure timestamptz NULL,
	actual_arrival timestamptz null,
	last_update timestamp NOT NULL,
	deleted timestamp null,
	hubflightshashkey varchar(32) not null,
	hubairportshashkey varchar(32) not null,
	hubaircraftshashkey varchar(32) not null,
	LinkFlightsAirportsHashKey varchar(32) not null,
	LinkFlightsAircraftsHashKey varchar(32) not null,
	flightshashdiff varchar(32) not null,
	flightsDeparthashdiff varchar(32) not null,
	flightsArrivhashdiff varchar(32) not null,
	loadDate timestamp not null,									-- дата загрузки записи в систему
	recordSource varchar(50) not null
	);

drop table if exists stg.aircrafts_data;
CREATE TABLE stg.aircrafts_data (
	aircraft_code bpchar(3) NOT NULL,
	model jsonb NOT NULL,
	"range" int4 NOT NULL,
	last_update timestamp NOT NULL,
	deleted timestamp null,
	hubaircraftshashkey varchar(32) not null,
	loadDate timestamp not null,									-- дата загрузки записи в систему
	recordSource varchar(50) not null
);

drop table if exists stg.bookings;
CREATE TABLE stg.bookings (
	book_ref bpchar(6) NOT NULL,
	book_date timestamptz NOT NULL,
	total_amount numeric(10, 2) NOT NULL,
	last_update timestamp NOT NULL,
	deleted timestamp null,
	hubbookingshashkey varchar(32) not null,
	loadDate timestamp not null,									-- дата загрузки записи в систему
	recordSource varchar(50) not null
);

drop table if exists stg.tickets;
CREATE TABLE stg.tickets (
	ticket_no bpchar(13) NOT NULL,
	book_ref bpchar(6) NOT NULL,
	passenger_id varchar(20) NOT NULL,
	passenger_name text NOT NULL,
	contact_data jsonb NULL,
	last_update timestamp NOT NULL,
	deleted timestamp null,
	hubticketshashkey varchar(32) not null,
	hubbookingshashkey varchar(32) not null,
	linkticketsbookingshashkey varchar(32) not null,
	loadDate timestamp not null,									-- дата загрузки записи в систему
	recordSource varchar(50) not null
);

drop table if exists stg.ticket_flights;
create table stg.ticket_flights(
ticket_no bpchar(13) NOT NULL,
flight_id int4 not null,
fare_conditions varchar(10) not null,
amount numeric(10, 2) not null,
last_update timestamp NOT NULL,
deleted timestamp null,
linkflightsticketshashkey varchar(32) not null,
hubflightshashkey varchar(32) not null,
hubticketshashkey varchar(32) not null,
loadDate timestamp not null,									-- дата загрузки записи в систему
recordSource varchar(50) not null
);

-- таблица для записи обновлений
drop table if exists stg.last_update;
create table stg.last_update(
table_name varchar(50) not null,
update_dt timestamp not null
);

-- заполнение таблицы с датами обновлений

-- добавление записи при загрузке
create or replace procedure stg.set_table_load_time(table_name varchar, current_update_dt timestamp default now())
as $$
begin 
	insert into stg.last_update(table_name, update_dt)							-- добавлем запись последней загрузки
			values 
				(table_name, current_update_dt);
end;
$$ language plpgsql;

-- берем последнюю дату из таблицы для инкрементальной загрузки 
create or replace function stg.get_last_update_t(table_name varchar) returns timestamp
as $$
begin 
	return coalesce(					-- берем запись максимальной загрузки, если нет то вводится число
			(select max(update_dt)
			from stg.last_update lu
			where lu.table_name = get_last_update_t.table_name),
			'2000-01-01'::date
		    );
end;	
$$ language plpgsql;


--заполнение stg слоя

-- заполнение stg.airports_data

create or replace procedure stg.airports_data_load(current_update_dt timestamp)
as $$
declare 
		last_update_dt timestamp;
begin 
	last_update_dt = stg.get_last_update_t('stg.airports_data');
	truncate table stg.airports_data;

	-- данные обновлются с даты удаления, 
	-- данные вставляются с момента обновления
	-- В таблицу stg.last_update загружаем данные о последней загрузке
	
	insert into stg.airports_data(airport_code, airport_name, city, coordinates, timezone, last_update, deleted, airport_id,
	hubairportshashkey, loadDate, recordSource)
	select airport_code, airport_name, city, coordinates, timezone, last_update, deleted, airport_id,
	upper(md5(upper(trim(coalesce(airport_id::text, ''))))) as 	hubairportshashkey,	                      --создание хеш ключа
	current_update_dt, 'flight_plan'
	from air_src.airports_data as ad
	where ad.last_update >= last_update_dt or ad.deleted >= last_update_dt;

	call stg.set_table_load_time('stg.airports_data', current_update_dt);
end;
$$ language plpgsql;

-- заполнение stg.flights
create or replace procedure stg.flights_load(current_update_dt timestamp)
as $$
	declare 
		last_update_dt timestamp;
	begin
		last_update_dt = stg.get_last_update_t('stg.flights');
	truncate table stg.flights;

	insert into stg.flights(flight_id, airport_id, flight_no, scheduled_departure, scheduled_arrival, departure_airport, arrival_airport,
 	status, aircraft_code, actual_departure, actual_arrival, last_update, deleted, hubflightshashkey, hubairportshashkey, hubaircraftshashkey,
 	LinkFlightsAirportsHashKey, LinkFlightsAircraftsHashKey, flightshashdiff, flightsDeparthashdiff, flightsArrivhashdiff, loadDate, recordSource)
	select flight_id, airport_id, flight_no, scheduled_departure, scheduled_arrival, departure_airport, arrival_airport, status, aircraft_code,
	actual_departure, actual_arrival, last_update, deleted,
	upper(md5(upper(trim(coalesce(flight_id::text, ''))))) as hubflightshashkey,
	upper(md5(upper(trim(coalesce(airport_id::text, ''))))) as 	hubairportshashkey,
	upper(md5(upper(trim(coalesce(aircraft_code::text, ''))))) as hubaircraftshashkey,
	upper(md5((upper(concat(trim(coalesce(flight_id::text, '')), ';', trim(coalesce(airport_id::text, ''))))))) as LinkFlightsAirportsHashKey,
	upper(md5((upper(concat(trim(coalesce(flight_id::text, '')), ';', trim(coalesce(aircraft_code::text, ''))))))) as LinkFlightsAircraftsHashKey,
	upper(md5((upper(concat(trim(coalesce(flight_no::text, '')), ';', 
	trim(coalesce(departure_airport::text, '')), ';',
	trim(coalesce(arrival_airport::text, '')), ';',
	trim(coalesce(status::text, '')), ';',
	trim(coalesce(aircraft_code::text, ''))))))) as flightshashdiff,
	upper(md5((upper(concat(trim(coalesce(scheduled_departure::text, '')), ';', 
	trim(coalesce(actual_departure::text, ''))))))) as flightsDeparthashdiff,
	upper(md5((upper(concat(trim(coalesce(scheduled_arrival::text, '')), ';', 
	trim(coalesce(actual_arrival::text, ''))))))) as flightsArrivhashdiff,
	current_update_dt, 'flight_plan'
	from air_src.flights as f
	where f.last_update >= last_update_dt or f.deleted >= last_update_dt;

	call stg.set_table_load_time('stg.flights', current_update_dt);
	end;
$$ language plpgsql;

--заполнение stg.aircrafts_data

--drop procedure stg.aircrafts_data_load;
create or replace procedure stg.aircrafts_data_load(current_update_dt timestamp)
as $$
begin 
	truncate table stg.aircrafts_data;

INSERT INTO stg.aircrafts_data
(aircraft_code, model, "range", last_update, deleted, hubaircraftshashkey, loadDate, recordSource)
select aircraft_code, model, "range", last_update, deleted,
upper(md5(upper(trim(coalesce(aircraft_code::text, ''))))) as hubaircraftshashkey,
current_update_dt, 'flight_plan'
from air_src.aircrafts_data;

call stg.set_table_load_time('stg.aircrafts_data', current_update_dt);
end;
$$ language plpgsql;

--заполнение stg.bookings
create or replace procedure stg.bookings_load(current_update_dt timestamp)
as $$
declare 
	last_update_dt timestamp;
begin
	last_update_dt = stg.get_last_update_t('stg.bookings');
	truncate table stg.bookings;

INSERT INTO stg.bookings
(book_ref, book_date, total_amount, last_update, deleted, hubbookingshashkey, loadDate, recordSource)
select book_ref, book_date, total_amount, last_update, deleted,
upper(md5(upper(trim(coalesce(book_ref::text, ''))))) as hubbookingshashkey,
current_update_dt, 'flight_plan'
from air_src.bookings b
where b.last_update >= last_update_dt or b.deleted >= last_update_dt;

call stg.set_table_load_time('stg.bookings', current_update_dt);
end;
$$ language plpgsql;

--заполнение stg.tickets
create or replace procedure stg.tickets_load(current_update_dt timestamp)
as $$
declare 
	last_update_dt timestamp;
begin
	last_update_dt = stg.get_last_update_t('stg.tickets');
	truncate table stg.tickets;

INSERT INTO stg.tickets
(ticket_no, book_ref, passenger_id, passenger_name, contact_data, last_update, deleted,
hubticketshashkey, hubbookingshashkey, linkticketsbookingshashkey, loadDate, recordSource)
select ticket_no, book_ref, passenger_id, passenger_name, contact_data, last_update, deleted,
upper(md5(upper(trim(coalesce(ticket_no::text, ''))))) as hubticketshashkey,
upper(md5(upper(trim(coalesce(book_ref::text, ''))))) as hubbookingshashkey,
upper(md5((upper(concat(trim(coalesce(ticket_no::text, '')), ';', trim(coalesce(book_ref::text, ''))))))) as linkticketsbookingshashkey,
current_update_dt, 'flight_plan'
from air_src.tickets t
where t.last_update >= last_update_dt or t.deleted >= last_update_dt;

call stg.set_table_load_time('stg.tickets', current_update_dt);
end;
$$ language plpgsql;


create or replace procedure stg.ticket_flights_load(current_update_dt timestamp)
as $$ 
declare
	last_update_dt timestamp;
begin 
	last_update_dt = stg.get_last_update_t('stg.ticket_flights');
	truncate table stg.ticket_flights;
	
INSERT INTO stg.ticket_flights
(ticket_no, flight_id, fare_conditions, amount, last_update, deleted, linkflightsticketshashkey, hubflightshashkey, hubticketshashkey,loadDate, recordSource)
select ticket_no, flight_id, fare_conditions, amount, last_update, deleted,
upper(md5((upper(concat(trim(coalesce(flight_id::text, '')), ';', trim(coalesce(ticket_no::text, ''))))))) as LinkFlightsTicketsHashKey,
upper(md5(upper(trim(coalesce(flight_id::text, ''))))) as hubflightshashkey,
upper(md5(upper(trim(coalesce(ticket_no::text, ''))))) as hubticketshashkey,
current_update_dt, 'flight_plan'
from air_src.ticket_flights tf
where tf.last_update >= last_update_dt or tf.deleted >= last_update_dt;

call stg.set_table_load_time('stg.ticket_flights', current_update_dt);
end;
$$ language plpgsql;


__________________________________________________________________________________________________________________________

--создание слоя datevault
create schema datevault;

drop table if exists datevault.SatFlights;
drop table if exists datevault.SatFlightsDepartureDate;
drop table if exists datevault.SatFlightsArrivalDate;
drop table if exists datevault.SatFlightsAirport;
drop table if exists datevault.SatAirports;
drop table if exists datevault.SatTickets;
drop table if exists datevault.SatTicketsBookings;
drop table if exists datevault.SatBookings;
drop table if exists datevault.SatAircrafts;
drop table if exists datevault.SatFlightsAircrafts;

drop table if exists datevault.LinkFlightsAirport;
drop table if exists datevault.LinkFlightsAircrafts;
drop table if exists datevault.LinkFlightsTickets;
drop table if exists datevault.LinkTicketsBookings;

drop table if exists datevault.HubFlights;
drop table if exists datevault.HubAirports;
drop table if exists datevault.HubAircrafts;
drop table if exists datevault.HubTickets;
drop table if exists datevault.HubBookings;
drop table if exists datevault.SatFlightsTickets;


-- создание hub
create table datevault.HubFlights(
HubFlightsHashKey varchar(32) primary key,
LoadDate timestamp not null,
RecordSource varchar(50) not null,
FlightsID int not null
);

create table datevault.HubAirports(
HubAirportsHashKey varchar(32) primary key,
LoadDate timestamp not null,
RecordSource varchar(50) not null,
AirportID int not null
);

create table datevault.HubTickets(
HubTicketsHashKey varchar(32) primary key,
LoadDate timestamp not null,
RecordSource varchar(50) not null,
Ticket_noID bpchar(13) not null
);

create table datevault.HubAircrafts(
HubAircraftsHashKey varchar(32) primary key,
LoadDate timestamp not null,
RecordSource varchar(50) not null,
Aircraft_codeID bpchar(3)
);

create table datevault.HubBookings(
HubBookingsHashKey varchar(32) primary key,
LoadDate timestamp not null,
RecordSource varchar(50) not null,
Book_refID bpchar(6)
);


-- создание linck

create table datevault.LinkFlightsAirport(
LinkFlightsAirportsHashKey varchar(32) primary key,
LoadDate timestamp not null,
RecordSource varchar(50) not null,
HubFlightsHashKey varchar(32) references datevault.HubFlights(HubFlightsHashKey),
HubAirportsHashKey varchar(32) references datevault.HubAirports(HubAirportsHashKey)
);

create table datevault.LinkFlightsAircrafts(
LinkFlightsAircraftsHashKey varchar(32) primary key,
LoadDate timestamp not null,
RecordSource varchar(50) not null,
HubFlightsHashKey varchar(32) references datevault.HubFlights(HubFlightsHashKey),
HubAircraftsHashKey varchar(32) references datevault.HubAircrafts(HubAircraftsHashKey)
);

create table datevault.LinkFlightsTickets(
LinkFlightsTicketsHashKey varchar(32) primary key,
LoadDate timestamp not null,
RecordSource varchar(50) not null,
HubFlightsHashKey varchar(32),
HubTicketsHashKey varchar(32) references datevault.HubTickets(HubTicketsHashKey)
);

create table datevault.LinkTicketsBookings(
LinkTicketsBookingsHashKey varchar(32) primary key,
LoadDate timestamp not null,
RecordSource varchar(50) not null,
HubTicketsHashKey varchar(32) references datevault.HubTickets(HubTicketsHashKey),
HubBookingsHashKey varchar(32) references datevault.HubBookings(HubBookingsHashKey)
);

--создание sat

create table datevault.SatFlights(
HubFlightsHashKey varchar(32) not null,
LoadDate timestamp not null,
LoadEndDate timestamp null,
RecordSource varchar(50) not null,
HashDiff varchar(32) not null,

flight_no bpchar(6),
departure_airport bpchar(3),
arrival_airport bpchar(3),
status varchar(20),
aircraft_code bpchar(3),

primary key (HubFlightsHashKey, LoadDate)
);

create table datevault.SatFlightsDepartureDate(
HubFlightsHashKey varchar(32) not null,
LoadDate timestamp not null,
LoadEndDate timestamp null,
RecordSource varchar(50) not null,
HashDiff varchar(32) not null,

scheduled_departure timestamptz,
actual_departure timestamptz,

primary key (HubFlightsHashKey, LoadDate)
);

create table datevault.SatFlightsArrivalDate(
HubFlightsHashKey varchar(32) not null,
LoadDate timestamp not null,
LoadEndDate timestamp null,
RecordSource varchar(50) not null,
HashDiff varchar(32) not null,

scheduled_arrival timestamptz,
actual_arrival timestamptz,

primary key (HubFlightsHashKey, LoadDate)
);

create table datevault.SatFlightsAirport(
LinkFlightsAirportsHashKey varchar(32) not null,
LoadDate timestamp not null,
LoadEndDate timestamp null,
RecordSource varchar(50) not null,

primary key (LinkFlightsAirportsHashKey, LoadDate)
);

create table datevault.SatAirports(
HubAirportsHashKey varchar(32) not null,
LoadDate timestamp not null,
LoadEndDate timestamp null,
RecordSource varchar(50) not null,
HashDiff varchar(32) not null,

airport_code bpchar(3),
airport_name jsonb,
city jsonb,
coordinates point,

primary key (HubAirportsHashKey, LoadDate)
);

create table datevault.SatTickets(
HubTicketsHashKey varchar(32) not null,
LoadDate timestamp not null,
LoadEndDate timestamp null,
RecordSource varchar(50) not null,
HashDiff varchar(32) not null,

passenger_id varchar(20),
passenger_name text,
contact_data jsonb,

primary key (HubTicketsHashKey, LoadDate)
);

create table datevault.SatTicketsBookings(
LinkTicketsBookingsHashKey varchar(32) not null,
LoadDate timestamp not null,
LoadEndDate timestamp null,
RecordSource varchar(50) not null,

primary key (LinkTicketsBookingsHashKey, LoadDate)
);

create table datevault.SatBookings(
HubBookingsHashKey varchar(32) not null,
LoadDate timestamp not null,
LoadEndDate timestamp null,
RecordSource varchar(50) not null,
HashDiff varchar(32) not null,

book_date timestamptz,
total_amount numeric(10, 2),

primary key (HubBookingsHashKey, LoadDate)
);

create table datevault.SatAircrafts(
HubAircraftsHashKey varchar(32) not null,
LoadDate timestamp not null,
LoadEndDate timestamp null,
RecordSource varchar(50) not null,
HashDiff varchar(32) not null,

aircraft_code bpchar(3),
model jsonb,
"range" int,

primary key (HubAircraftsHashKey, LoadDate)
);

create table datevault.SatFlightsAircrafts(
LinkFlightsAircraftsHashKey varchar(32) not null,
LoadDate timestamp not null,
LoadEndDate timestamp null,
RecordSource varchar(50) not null,

primary key (LinkFlightsAircraftsHashKey, LoadDate)
);

create table datevault.SatFlightsTickets(
LinkFlightsTicketsHashKey varchar(32) not null,
LoadDate timestamp not null,
LoadEndDate timestamp null,
RecordSource varchar(50) not null,
HashDiff varchar(32) not null,

fare_conditions varchar(10),
amount numeric(10, 2),

primary key (LinkFlightsTicketsHashKey, LoadDate)
);


-- создание загрузки datevault слоя

-- создание процедур загрузки хабов

--загрузка hubflight
create or replace procedure datevault.hubaircrafts_load()
as $$
begin
	--загрузка из stg слоя строк которых нет в datevault слое
	INSERT INTO datevault.hubaircrafts
	(hubaircraftshashkey, loaddate, recordsource, Aircraft_codeID)
	select ad.hubaircraftshashkey, ad.loaddate, ad.recordsource, ad.aircraft_code
	from stg.aircrafts_data ad
	where ad.aircraft_code not in (
		select ha.Aircraft_codeID from datevault.hubaircrafts ha);
end;
$$ language plpgsql;

-- hub hubairports
create or replace procedure datevault.hubairports_load()
as $$
begin
	--загрузка из stg слоя строк которых нет в datevault слое
	INSERT INTO datevault.hubairports
	(hubairportshashkey, loaddate, recordsource, airportid)
	select ad.hubairportshashkey, ad.loaddate, ad.recordsource, ad.airport_id
	from stg.airports_data ad
	where ad.airport_id not in (
		select ha.airportid from datevault.hubairports ha);
end;
$$ language plpgsql;

-- hub datevault.hubbookings
create or replace procedure datevault.hubbookings_load()
as $$
begin
	--загрузка из stg слоя строк которых нет в datevault слое
	INSERT INTO datevault.hubbookings
	(hubbookingshashkey, loaddate, recordsource, book_refid)
	select b.hubbookingshashkey, b.loaddate, b.recordsource, b.book_ref
	from stg.bookings b
	where b.book_ref not in (
		select hb.book_refid from datevault.hubbookings hb);
end;
$$ language plpgsql;

-- hub datevault.hubflights
create or replace procedure datevault.hubflights_load()
as $$
begin
	--загрузка из stg слоя строк которых нет в datevault слое
	INSERT INTO datevault.hubflights
	(hubflightshashkey, loaddate, recordsource, flightsid)
	select f.hubflightshashkey, f.loaddate, f.recordsource, f.flight_id
	from stg.flights f
	where f.flight_id not in (
		select hf.flightsid from datevault.hubflights hf);
end;
$$ language plpgsql;

-- hub datevault.hubtickets
create or replace procedure datevault.hubtickets_load()
as $$
begin
	--загрузка из stg слоя строк которых нет в datevault слое
	INSERT INTO datevault.hubtickets
	(hubticketshashkey, loaddate, recordsource, ticket_noid)
	select t.hubticketshashkey, t.loaddate, t.recordsource, t.ticket_no
	from stg.tickets t
	where t.ticket_no not in (
		select ht.ticket_noid from datevault.hubtickets ht);
end;
$$ language plpgsql;

-- link datevault.linkflightsaircrafts
create or replace procedure datevault.linkflightsaircrafts_load()
as $$
begin
	--проверем какие линки есть в stg и нет в datevault слое и их вставлем
	insert into datevault.linkflightsaircrafts 
	(linkflightsaircraftshashkey, loaddate, recordsource, hubflightshashkey, hubaircraftshashkey)
	select sf.linkflightsaircraftshashkey, sf.loaddate, sf.recordsource, sf.hubflightshashkey, sf.hubaircraftshashkey
	from stg.flights sf
	where not exists (
		select 1
		from datevault.linkflightsaircrafts lfa
		where sf.linkflightsaircraftshashkey = lfa.linkflightsaircraftshashkey);
end;
$$ language plpgsql;


-- link datevault.linkflightsairport
create or replace procedure datevault.linkflightsairport_load()
as $$
begin
	--проверем какие линки есть в stg и нет в datevault слое и их вставлем
	insert into datevault.linkflightsairport 
	(LinkFlightsAirportsHashKey, loaddate, recordsource, hubflightshashkey, hubairportshashkey)
	select sf.LinkFlightsAirportsHashKey, sf.loaddate, sf.recordsource, sf.hubflightshashkey, sf.hubairportshashkey
	from stg.flights sf
	where not exists (
		select 1
		from datevault.linkflightsairport lfar
		where sf.LinkFlightsAirportsHashKey = lfar.LinkFlightsAirportsHashKey);
end;
$$ language plpgsql;


-- link datevault.linkflightstickets
create or replace procedure datevault.linkflightstickets_load()
as $$
begin
	--проверем какие линки есть в stg и нет в datevault слое и их вставлем
	insert into datevault.linkflightstickets 
	(linkflightsticketshashkey, loaddate, recordsource, hubflightshashkey, HubTicketsHashKey)
	select stf.linkflightsticketshashkey, stf.loaddate, stf.recordsource, stf.hubflightshashkey, stf.HubTicketsHashKey
	from stg.ticket_flights stf
	where not exists (
		select 1
		from datevault.linkflightstickets lft
		where stf.linkflightsticketshashkey = lft.linkflightsticketshashkey);
end;
$$ language plpgsql;


-- link datevault.linkticketsbookings
create or replace procedure datevault.linkticketsbookings_load()
as $$
begin
	--проверем какие линки есть в stg и нет в datevault слое и их вставлем
	insert into datevault.linkticketsbookings
	(linkticketsbookingshashkey, loaddate, recordsource, HubTicketsHashKey, hubbookingshashkey)
	select st.linkticketsbookingshashkey, st.loaddate, st.recordsource, st.hubbookingshashkey, st.HubTicketsHashKey
	from stg.tickets st
	where not exists (
		select 1
		from datevault.linkticketsbookings ltb
		where st.linkticketsbookingshashkey = ltb.linkticketsbookingshashkey);
end;
$$ language plpgsql;


-- создаем процедуры для саттелитов
-- sat datevault.satflights
create or replace procedure datevault.satflights_load(current_update_dt timestamp)
as $$
begin
	insert into datevault.satflights(hubflightshashkey, loaddate, loadenddate, recordsource, hashdiff, flight_no, 
	departure_airport, arrival_airport, status, aircraft_code)
	select sf.hubflightshashkey, sf.loaddate, null as loadenddate, sf.recordsource, sf.flightshashdiff, flight_no, 
	sf.departure_airport, sf.arrival_airport, sf.status, sf.aircraft_code
	from stg.flights sf left join datevault.satflights satf on sf.hubflightshashkey = satf.hubflightshashkey
		and sat.loadenddate is null
	where satf.hubflightshashkey is null
		or satf.hashdiff != sf.flightshashdiff;

	with update_sat as (
		select f.hubflightshashkey, f.loaddate, cf.loaddate as loadenddate
		from datevault.satflights f join datevault.satflights cf on f.hubflightshashkey = cf.hubflightshashkey
			and cf.loaddate > f.loaddate
		)
	update datevault.satflights as satf
	set loadenddate = s.loadenddate
	from update_sat s
	where satf.hubflightshashkey = s.hubflightshashkey
		and satf.loaddate = s.loaddate;

	with deleted_sat as (
		select f.hubflightshashkey, f.loaddate, cf.loaddate as loadenddate
		from datevault.satflights df left join stg.flights sf on df.hubflightshashkey = sf.hubflightshashkey
		where df.hubflightshashkey is null
			and sf.loadenddate is null
		)
	update datevault.satflights as satfl
	set loadenddate = ds.loadenddate
	from deleted_sat ds
	where satfl.hubflightshashkey = ds.hubflightshashkey
		and satfl.loaddate = ds.loaddate;
		
end;
$$ language plpgsql;

_________________________________________________________________________________________________________________________________


create or replace procedure stg.full_load()
as $$
declare 
	current_update_dt timestamp = now();
begin
	--stg слой
	call stg.airports_data_load(current_update_dt);
	call stg.flights_load(current_update_dt);
	call stg.aircrafts_data_load(current_update_dt);
	call stg.bookings_load(current_update_dt);
	call stg.tickets_load(current_update_dt);
	call stg.ticket_flights_load(current_update_dt);

	--datevault слой
			
	--хабы
	/*
	call datevault.hubaircrafts_load();
	call datevault.hubairports_load();
	call datevault.hubbookings_load();
	call datevault.hubflights_load();
	call datevault.hubtickets_load();

	--линки
	call datevault.linkflightsaircrafts_load();
	call datevault.linkflightsairport_load();
	call datevault.linkflightstickets_load();
	call datevault.linkticketsbookings_load(); */

end;
$$ language plpgsql;

call stg.full_load();


select * from stg.flights;

select * from stg.aircrafts_data;

select * from stg.ticket_flights;

select * from stg.last_update;

select * from stg.bookings;

select  * from datevault.hubtickets;

select * from air_src.aircrafts_data;

select * from stg.airports_data;

select  * from stg.tickets;

select * from datevault.hubflights;

select * from datevault.hubairports;

select * from datevault.hubaircrafts;

select * from datevault.hubbookings;

select * from datevault.linkflightsaircrafts;

select * from datevault.linkflightsaircrafts;

select * from datevault.linkflightstickets;

select * from datevault.linkticketsbookings;


-- переделать таблицу ticket_flights в БД demo. Создать новый линк для соединения 

-- загрузить новую таблицу про самолеты которая полегче
