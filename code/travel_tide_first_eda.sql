select *
from flights
limit 5;

select *
from hotels
limit 5;

select *
from users
limit 5;

select *
from sessions
limit 5;
-- 5408063 sessions

-- user anmeldungen nach monat
select 
    date_trunc('month', sign_up_date) as sign_up_date,
    count(*) as users
from users
group by date_trunc('month', sign_up_date) 
order by date_trunc('month', sign_up_date) ASC
;

-- identify number of columns per table
SELECT table_name, COUNT(*) AS column_count
FROM information_schema.columns
WHERE table_schema = 'public'
GROUP BY table_name
ORDER BY column_count DESC;

-- null werte
SELECT
    count(*) as total_rows,
    count(origin_airport),
    count(destination) as destination_cnt,
    count(seats) as seats_cnt,
    count(return_flight_booked) as return_flights_cnt,
    count(departure_time) as dep_time_cnt
FROM flights
limit 5;


-- finde spalten mit null werten
SELECT count(trip_id)
FROM flights
WHERE trip_id is null;


-- UNTERSUCHE TABELLE USERS
SELECT '1. rows cnt' as "analysis", count(*) as "result"
from users 
UNION
select '2. user_id cnt', count(user_id)
from users 
UNION
select '3. user_id dist cnt', count(distinct user_id)
FROM users
union 
select '4. user_id is null', count(user_id)
from users
where user_id is null
order by analysis asc
;


-- UNTERSUCHE TABELLE FLIGHTS
SELECT '1. rows cnt' as "analysis", count(*) as "result"
from flights
UNION
select '2. trip_id cnt', count(trip_id)
from flights 
UNION
select '3. trip_id dist cnt', count(distinct trip_id)
FROM flights
union 
select '4. trip_id is null', count(trip_id)
from flights
where trip_id is null
order by analysis asc
;


-- UNTERSUCHE TABELLE SESSIONS
SELECT '1. rows cnt' as "analysis", count(*) as "result"
from sessions
UNION
select '2. user_id cnt', count(user_id)
from sessions 
UNION
select '3. user_id dist cnt', count(distinct user_id)
FROM sessions
union 
select '4. user_id is null', count(user_id)
from sessions
where user_id is null
order by analysis asc
;

-- wie sind die relationships 1:1, 1:n, m:n zwischen den tablelen?
SELECT s.session_id, s.trip_id as trip_id_in_session, h.trip_id trip_id_in_hotel, f.trip_id as trip_id_in_flights
FROM sessions as s
FULL JOIN hotels as h using(trip_id)
FULL JOIN flights as f using(trip_id)
ORDER BY random()
limit 100;
-- eine session umfasst keine oder eine hotelbuchung oder mehr?
-- eine hotelbuchung entspricht einer session
-- kann in einer session mehrere hotels gebucht sein?

SELECT session_id, count(trip_id)
FROM sessions
GROUP BY session_id
order by count(trip_id) DESC
limit 5;
-- ERGEBNIS: zu einer session id gibt es keine oder exakt eine trip id. Eine hotel_buchung gehört zu einer (oder zwei) session_ids (im falle von cancel)
-- FAZIT: session <> hotel: 1(2) zu 0(1)????


SELECT s.trip_id as session_trip_id, count(h.trip_id) as trip_count_in_hotels
FROM hotels as h
INNER JOIN sessions as s using(trip_id)
GROUP BY session_trip_id
ORDER BY trip_count_in_hotels asc
LIMIT 5;
-- ERGEBNIS: eine trip_id in hotels kann bis zu zweimal in sessions sein

--- UNTERSUCHE RELATIONSHIP BETWEEN SESSIONS UND USERS

WITH prep as(
    SELECT user_id, count(user_id) as user_active_sessions
    FROM sessions as s 
    GROUP BY user_id
)
SELECT
    user_active_sessions, count(user_active_sessions)
FROM prep
group by user_active_sessions
order by user_active_sessions asc
;
-- ein user hat n sessions
-- eine session hat ? user?

with prep as(
    SELECT
        distinct session_id, count(user_id) as user_cnt
    FROM sessions
    GROUP BY session_id
)
select *
from prep
order by user_cnt desc
limit 5
;
-- ERGEBNIS: 1 session hat genau einen user


-- UNTERSUCHE TABELLE SESSIONS
SELECT '1. rows cnt' as "analysis", count(*) as "result"
from hotels
UNION
select '2. trip_id cnt', count(trip_id)
from hotels 
UNION
select '3. trip_id dist cnt', count(distinct trip_id)
FROM hotels
union 
select '4. trip_id is null', count(trip_id)
from hotels
where trip_id is null
order by analysis asc
;


-- Wie oft kommt jeder trip_id in Flights vor?
SELECT 
  trip_id, COUNT(*) 
FROM Flights 
GROUP BY trip_id 
ORDER BY COUNT(*) DESC;
-- ERGEBNIS: eine trip_id kommt in nur einmal vor

-- wie oft kommt die trip_id in sessions vor?
SELECT
    trip_id, count(trip_id)
FROM sessions
group BY trip_id
ORDER BY count(trip_id) DESC
;
-- ERGEBNIS: eine trip_id kommt in sessions einmal oder zweimal vor

-- wann kommt eine trip_id einmal vor? wann zweimal?

-- zeige 5 beispiele, wenn eine trip_id zweimal vorkommt
WITH prep as (
    SELECT
        trip_id, count(trip_id)
    FROM sessions
    group BY trip_id
    having count(trip_id) = 2
    ORDER BY count(trip_id) DESC
)    
select *
from prep as p 
left join sessions as s on s.trip_id = p.trip_id    
order by p.trip_id ASC, session_start ASC
limit 5
;
-- 114677-adabdb0467a141f4b53d0ea7504fb141
-- 115101-f07207255be240ba81427f192352f7ff

-- HYPOTHESE: alle doppelten trip_ids in session stammen von einer buchung und einer darauf folgenden Cancellation
-- Methode: zähle alle doppelten und gruppiere nach cancellation (true, false)

WITH prep as (
    SELECT
        trip_id, count(trip_id)
    FROM sessions
    group BY trip_id
    having count(trip_id) = 2
    ORDER BY count(trip_id) DESC
)    
select s.cancellation, count(p.trip_id)
from prep as p 
left join sessions as s on s.trip_id = p.trip_id    
group by s.cancellation
;
-- ERGEBNIS: wir haben exakt so viele entries of trip_id mit false (90,670) wie mit cancellation = true (90670)
-- FAZIT: die doppelten einträge resultieren zu hoher wahrscheinlichkeit ausschließlich durch cancellations
-- FAZIT 2: eine Trip_id wird nicht wie ein Warenkorb aufgehoben und bei folgenden Sessions erneut verwendet bis jemand bucht

-- FRAGE: wie sehen die sessions aus, bis der nutzer bucht. werden daten gespeichert, die auf sein Interesse Hotel/Flug/Destination hinweisen?
-- STEP1: identifiziere einen nutzer, der mehrere sessions hatte UND gebucht hat. 
SELECT
    user_id, count(user_id)
FROM sessions
group by user_id
order by count(user_id) DESC
;
-- ein user war maximal 17 mal aktiv
-- FRAGE: wie ist die Häufigkeitsverteilung and sessions | über alle und später pro jahr
-- FRAGE: wie häufig pro Jahr haben sich die user im Durchschnitt angemeldet?

WITH prep as (
    SELECT
        user_id as user_id, count(user_id) as sessions_cnt
    FROM sessions
    group by user_id
    order by count(user_id) DESC
)
SELECT 
    sessions_cnt, 
    sum(sessions_cnt) as count, 
    round(sum(sessions_cnt)::dec / (select sum(sessions_cnt) from prep),2) as share_of_total,
    sum(round(sum(sessions_cnt)::dec / (select sum(sessions_cnt) from prep),2)) over(order by sessions_cnt) as share_run_sum
FROM prep
group BY sessions_cnt
order by sessions_cnt asc
;

-- ERGEBNIS: 21% haben bis zu 4 sessions. 50% der Nutzer haben bis zu 6 sessions. 80% bis zu 8 sessions. 94% bis zu 10 sessions. 
--           1 sessions haben nur 1%, 2 sessions nur 3%, 
--           5,6,7,8 sessions kommen am häufigsten vor (13-16% jeweils)



-- wieviele sessions gab es, in denen nichts oder etwas gebucht wurde
select flight_booked, hotel_booked, count(session_id)
from sessions 
group by flight_booked, hotel_booked
;
-- in 56% der sessions wurde nichts gebucht
-- in 30% der sesions wurde beides gebucht
-- in je 6% der sesisons wurde entweder ein hotel oder ein flug gebucht


-- geben die felder hotel_booked, flight_booked korrekt an, dass sich dann eine buchung in hotels and flights wieder finde?
SELECT count(*)
FROM sessions as s
LEFT JOIN hotels as h using(trip_id)
left join flights as f using(trip_id)
WHERE h.trip_id is null
limit 5;

-- ERGEBNIS: 3398776 entries with hotel_booked = false
--           3398776 entries with hotel_booked = false AND hotel.trip_id = NULL
--           3445143 entries without a corresponding hotel entry
-- FAZIT: das feld hotel_booked gibt korrekt wieder dass in table hotels eine buchung vorliegt

-- FRAGE: gibt es buchungen in hotels, wo keine session dazu vorliegt?
SELECT count(*)
FROM hotels as h 
left join sessions as s using(trip_id)
where s.trip_id is null 
;
-- ERGEBNIS 0.
-- FAZIT: zu jedem eintrag in hotels gibt es einen passenden eintrag in sessions

-- FRAGE: wieviele hotels wurden gebucht und wieder abgesagt. was ist die stornoquote von hotels
SELECT cancellation::char as hotel_cancelled, count(trip_id)
FROM sessions
where hotel_booked = True
group by cancellation
UNION
SELECT 'total_hotels_booked' as cancellation, count(trip_id)
from sessions
where hotel_booked = True
;

-- FRAGE: welche produkte (hotels oder flüge) werden wie häufig gecancelled
select hotel_booked, flight_booked, count(trip_id)
from sessions
where cancellation is true
group by hotel_booked, flight_booked
;
-- ergebnis: 90670 cancellations sind immer cancel of hotel und flug gemeinsam
--           hotels und flüge werden immer gemeinsam gecanceld
--           es gibt keine cancellations wo nur ein hotel oder ein flug getrennt gebucht waren oder gecancelled wurden
--           es ist anzunehmen, dass sie beide gemeinsam gecancelled wurden

-- frage: werden flights und hotels aus den buchungen gelöscht wenn sie gecancelled werden?
-- weg: identifiziere trip_ids mit cancellation und schaue, ob die trip_id in hotels und flights ist
SELECT count(trip_id)
FROM sessions as s
INNER JOIN flights using(trip_id)
where s.cancellation is true
;
-- für 90k cancellations gibt es 89344 passende flüge in flights tabelle
-- 90670 - 89344 = 

select 90670 - 89344; -- das ergibt 1326

SELECT count(trip_id)
FROM sessions as s
INNER JOIN hotels using(trip_id)
where s.cancellation is true
;
-- für 90k cancellations gibt es 44303 hotels in hotels tabelle
-- ERGEBNIS: cancellations liegen nur vor, wenn kunde sowohl flug als auch hotel gebucht hat
--           zu cancellations gibt es jedoch noch 89k flüge und 44k hotels buchungen
-- FAZIT: einträge in sessions zeigen nicht, welches product gecanceled wurde
--        die ursprünglichen buchungen in hotels und flights werden durch ein cancel offensichtlich gelöscht 



-- wieviel einzigartige nutzer sind in tabelle sessions und in tabelle users
select 'user_in_sessions' as type, count(distinct user_id)
from sessions
UNION
select 'user_in_users' as type, count(distinct user_id)
from users
;


select count(distinct trip_id), count(*), count(trip_id)
from flights
;
-- 1.9 Mio unique trip_ids, keine null werte
-- Rows in flights ONLY exist with details being filled 
-- >> Question: do we have the same trip_id also if it is not booked, but stored in the basket? 
-- >> How can we see that? If same trip_id is multiple times in the sessions

-- zähle die trip id and session id in sessions and in hotels and flights

select 'hotels: trip_id', count(distinct trip_id) as cnt_distinct, count(trip_id) as cnt_trip_id, count(*) as "count_*"
from hotels
UNION
select 'flights: trip_id', count(distinct trip_id), count(trip_id), count(*)
from flights
UNION
select 'sessions: trip_id', count(distinct trip_id), count(trip_id), count(*)
from sessions
;
-- in hotels and flights gibt es nur erfolgte Buchungen, mit jeweils nur unique trip_ids
-- in sessions gibt es 5,4 Mio einträge, davon 2.2 MIo unique Trip IDs aber 2.3 Mio Einträge mit Trip_id

-- Hypothese: Es gibt viele Sessions, wo nicht gebucht wurde (keine Trip_id?, keine erfolgte buchungen, doch eine Trip_ID?)
SELECT hotel_booked, flight_booked, trip_id, count(trip_id)
FROM sessions 
where 
    trip_id is null and
    (hotel_booked is true or
    flight_booked is true) 
group by hotel_booked, flight_booked, trip_id
;
-- >> Für jede Session MIT einer Trip_ID ist der Flag hotel_booked oder flight_booked TRUE gesetzt
-- >> es gibt keine Session mit einem der beiden Flats auf True, ohne eine Trip_ID

-- wieviele sessions gibt es mit ein und dergleichen trip_nummer?
with prep as (
    select count(session_id) 
    from sessions
    where trip_id is not null
    group by trip_id
    having count(session_id) > 1
    order by count(session_id) DESC
)
select count(*)
from prep
;
-- wieviele Cancellation gibt es?
select sum(case when cancellation = true then 1 else 0 end) as sum_cancellation
from sessions 
limit 5
;

select * 
from sessions
where trip_id = '213933-75d22ef1251b4682808bccc1aed2b2d6'
;
-- INSIGHT: In der Session Tabelle kann es verschiedene Sessions mit der gleichen Trip ID geben
--          Wenn ein Trip gebucht wird UND dann nochmal, wenn er wieder storniert wird



-- Hypotehse: In manchen Fällen werden Trip_IDs angelegt, aber keine buchung realisiert. Gibt es das?
-- Hypothese: Wird das Hotel-Flug dann später gebucht wird diese Trip_ID später verwendet

-- Hypothese: Gibt es Flights-Hotels-Einträge, die zu Sessions führen, wo keine buchungen erfolgt sind?
SELECT 
    count(trip_id) as cnt_tripid, 
    count(distinct trip_id) as cnt_dist_tripid
FROM hotels as h
LEFT JOIN sessions as s using(trip_id)      -- TODO: Unterschied left/right/inner verstehen
WHERE s.trip_id is not null
;


-- Hypothese: Es gibt Sessions mit und ohne Trip_ID
SELECT
    sum(case when trip_id is null then 1 else 0 end) as trip_id_is_null,
    sum(case when trip_id is not null then 1 else 0 end) as trip_id_not_null,
    flight_booked,
    hotel_booked
from sessions
group by flight_booked, hotel_booked
;
-- 3.07 Mio Sessions mit Trip_id_is_null 
-- 2.33 Mio Sessions mit Trip_id_not_null, davon
--    344 nur hotel
--    326 nur flug
--  1.665 flug und hotel

-- wieviele user id gibt es ohne session?
select count(distinct u.user_id)
from users as u
left join sessions as s 
using(user_id)
where s.user_id is null
;
-- ERGEBNIS: jeder user hat mindestens eine session gehabt

-- selektiere users, die noch keine session haben
select count(*)
from sessions as s 
left join users as u
on u.user_id = s.user_id
--where s.user_id is null;
;

-- wieviel nutzer haben noch nichts gebucht
select count(*)
from users as u 
full join sessions as s
on u.user_id = s.user_id
where s.hotel_booked is null; --or hotel_booked is null;
-- 0?


-- wie häufig haben einzelne user gebucht
WITH user_bookings AS (
    SELECT user_id, 
           COUNT(user_id) AS total_bookings
    FROM sessions
    WHERE flight_booked = true OR hotel_booked = true
    GROUP BY user_id
)
SELECT total_bookings, 
       COUNT(*) AS number_of_users
FROM user_bookings
GROUP BY total_bookings
ORDER BY total_bookings;



--Frage: Query the users table to get a breakdown of users by gender,
--marital status, and whether they have children.
SELECT
    gender,
    married,
    has_children,
    COUNT(user_id) AS anzahl_nutzer,
    round(count(user_id)::DEC / (select count(*) from users),2) as share_of_total
FROM
    users
GROUP BY
    gender,
    married,
    has_children
ORDER BY
    anzahl_nutzer DESC;

-- our userbase has 1M registered users, males dominating slightly with 55% share
-- 450k (~40%) of our users are not married without children (roughly half male, half female)
-- 24% are married with no children
-- 31% have children and either married or not (317k users) 
-- only a very small number of users (<10k) do not supply gender information


-- define age buckets and identify count of users and their share of all users
with users_age as (
    SELECT
        EXTRACT(YEAR FROM AGE(birthdate)) AS alter
    FROM users
    ),
age_count as (
    select
        CASE
            when alter <= 30 then '30-'
            when alter between 31 and 40 then '31-40'
            when alter between 41 and 50 then '41-50'
            when alter between 51 and 60 then '51-60'
            when alter > 60 then '60+'
        END as age_group
    from users_age
)
select 
    age_group, 
    count(age_group),
    round(count(age_group)::DEC / (select count(*) from age_count),2) as share_of_total 
from age_count
group by age_group
order by age_group
;
-- INSIGHT: the age distribution of our users are not representative to society
-- below 30s have a share of 20%. 31 to 50 represent 56%, 50-60 only 17% and 60+ only 7%
-- Obviously our service attract less silver serfers


-- show average seats booked by social demographics
SELECT
    ROUND(AVG(seats), 2) AS average_seats_booked,
    gender,
    married,
    has_children,
    home_country
FROM flights as f
inner join sessions as s on s.trip_id = f.trip_id
inner join users as u on u.user_id = s.user_id
group by gender,
    married, 
    has_children,
    home_country
;
-- INSIGHT: average seats booked vary between 1.2 and 1.8. non-married no children book least seats. married and has_children most seats. country or sex does not make a big differnce.
select count(user_id)::dec / (select count(user_id) from users) as avg_session_per_user
from sessions;

-- show amount of users by registration_month and calculate running total as well as % of total
with prep as (
    select 
        date_trunc('year', sign_up_date) as signed_up_month,
        count(user_id)
    from users
    group by signed_up_month
    order by signed_up_month ASC
),
prep2 as (
    select 
        signed_up_month,
        count as total_users,
        sum(count) over(order by count) as running_total,
        sum(count) over(order by count):: DEC / (select sum(count) from prep) as share_of_total
    from prep
)
select *, share_of_total - lag(share_of_total,1) over(order by signed_up_month) as growth_mom
from prep2
;



-- wann melden sich die meisten user an
select 
    date_part('month', sign_up_date) as signed_up_month,
    round(count(user_id)::DEC / (SELECT count(*) FROM users),2) as share_of_total
from users
group by date_part('month', sign_up_date)
;
-- monate märz bis juni deutlich überdurchschnittlich --> buchung sommerferien
-- monate august bis oktober deutlich unterdurchschnittlich


--What is the average "customer age" of TravelTide user?
--(defined as months since the user signed up).
SELECT
    round(AVG(EXTRACT(YEAR FROM AGE(sign_up_date)) * 12 + EXTRACT(MONTH FROM 
AGE(sign_up_date))),1) AS durchschnitt_kundenalter_in_monaten
FROM users;

-- note AGE Function returns difference to current_date
select sign_up_date, age(sign_up_date)
from users
limit 5
;



-- What are the 10 most popular hotels?
-- TOP TEN BELIEBTESTE
SELECT
    hotel_name,
    COUNT(trip_id) AS anzahl_buchungen,
    AVG(nights) AS avg_aufenthaltsdauer,
    AVG(hotel_per_room_usd) AS avg_preis_pro_nacht
FROM
    hotels
GROUP BY
    hotel_name
ORDER BY
    anzahl_buchungen DESC
LIMIT 10;

-- TOP TEN TEUERSTE
SELECT
    hotel_name,
    AVG(hotel_per_room_usd) AS avg_preis_pro_nacht
FROM
    hotels
GROUP BY
    hotel_name
ORDER BY
    avg_preis_pro_nacht DESC
LIMIT 10;


-- What is the most used airline in the last 6 months of recorded data?
SELECT
    trip_airline,
    COUNT(*) AS anzahl_fluege
FROM
    flights
WHERE
    departure_time >= (SELECT MAX(departure_time) FROM flights) - INTERVAL '6' 
MONTH
GROUP BY
    trip_airline
ORDER BY
    anzahl_fluege DESC
LIMIT 5;

-- What is the average time between registration and booking of the first trip
with prep as (
    SELECT
        u.user_id,
        u.sign_up_date,
        s.flight_booked,
        s.session_start as end_of_previous_sessions,
        lead(s.session_start, 1) over(partition by u.user_id order by s.session_start asc) as start_of_next_sessions
    FROM users as u
    INNER JOIN sessions as s using(user_id)
    WHERE cancellation = FALSE
    ORDER BY u.user_id, s.session_start
)
SELECT *
FROM prep
WHERE flight_booked = TRUE 
ORDER BY user_id asc, start_of_next_sessions ASC
limit 10
;


SELECT cancellation, nights, count(*)
FROM hotels
inner join sessions using (trip_id)
where ((check_out_time < check_in_time) or nights <0) --and cancellation is true
group by cancellation, nights
limit 100
;


-- untersuche preis
SELECT max(hotel_per_room_usd), min(hotel_per_room_usd), avg(hotel_per_room_usd), STDDEV_POP(hotel_per_room_usd)
from hotels
order by random();

-- wie kann ich am einfachsten anomalien erkennen?
-- z.B. Alter > 90
-- z.B. Nights > 50?
-- z.B. room_price 8 EUR?
-- am einfachsten über Box-Plots und Histogramme
-- NULL-Werte, negative Werte
-- am einfachsten über Python (oder in Tableau)


-- identifiziere den ersten eintrag bis flight_booked = true


--What is the average number of seats booked on flights via TravelTide?
SELECT
    AVG(seats) AS durchschnittlich_gebuchte_sitze
FROM
    flights;
 
 
   -- What is the variability of the price for the same flight routes over different seasons?
   --Hierfür definieren wir "Variabilität" als die Standardabweichung (STDDEV) und "Saison" über eine CASE-Anweisung.
SELECT
    origin_airport,
    destination_airport,
    CASE
        WHEN EXTRACT(MONTH FROM departure_time) IN (3, 4, 5) THEN 'Fruehling'
        WHEN EXTRACT(MONTH FROM departure_time) IN (6, 7, 8) THEN 'Sommer'
        WHEN EXTRACT(MONTH FROM departure_time) IN (9, 10, 11) THEN 'Herbst'
        ELSE 'Winter'
    END AS saison,
    COUNT(*) AS anzahl_fluege,
    AVG(base_fare_usd) AS avg_preis,
    STDDEV(base_fare_usd) AS preis_variabilitaet
FROM
    flights
GROUP BY
    origin_airport,
    destination_airport,
    saison
ORDER BY
    origin_airport,
    destination_airport,
    avg_preis DESC;


-- Frage: werden gecancelte Flüge und Hotels in Tabellen Hotels und Flüge wieder gelöscht?
with cancelled_trips as (
    select trip_id
    from sessions
    where cancellation = True and hotel_booked = True and flight_booked = True)
SELECT s.trip_id, s.cancellation, s.flight_booked, s.hotel_booked, f.trip_id as flight_trip_id, h.trip_id as hotel_trip_id, h.check_in_time, h.check_out_time
FROM sessions as s  
left join hotels as h using(trip_id)
left join flights as f using(trip_id)
inner join cancelled_trips as c using(trip_id)
where f.trip_id is null and s.flight_booked = True and s.cancellation = False   -- gibt es gecancelte Flugbuchungen wo keine Flüge in Flugtabelle exsiteiren (gelöscht wurden) -> Ergebnis 0
-- where h.trip_id is null and s.hotel_booked = True and s.cancellation = False   -- gibt es gecancelte Flugbuchungen wo die Flüge in Flugtabelle auch gelöscht wurden -> Ergebnis 0
order by s.trip_id, s.cancellation
limit 100
;

-- Insight: Offenbar werden die gebuchten Hotels und Flüge NICHT bei einem Cancel aus den Tabellen gelöscht
-- es scheint aber so, dass bei check_out_time in hotels ein dummywert erscheint (11:00:00)
-- es könnte sein, dass alle cancelations stets hotel-booked und flight_booked = True haben

-- frage: sind cacelations stets mit flags flight_booked und hotel_booked = True unabhängig von der ursprünglichen buchung?
with cancelled_trips as (
    select trip_id
    from sessions
    where cancellation = True and hotel_booked = True and flight_booked = True)
SELECT s.trip_id, s.cancellation, s.flight_booked, s.hotel_booked, f.trip_id as flight_trip_id, h.trip_id as hotel_trip_id, h.check_in_time, h.check_out_time
FROM sessions as s  
left join hotels as h using(trip_id)
left join flights as f using(trip_id)
inner join cancelled_trips as c using(trip_id)
where s.cancellation = True and (s.flight_booked = False or s.hotel_booked = False)
order by s.trip_id, s.cancellation
limit 100
-- Insight: Alle Cancel Sessions haben immer Hotel_booked = True und Flight_booked = True unabhängig davon ob es gebucht wurde oder nicht
; 

-- FRAGE: besteht ein zusammenbhang zwischen gecancelten hotels und dem dummyeintrag bei checked_out auf 11:00? -> Antwort: Nein
-- FRAGE: Gibt es andere "auffällige" check_out Uhrzeiten außer 11 Uhr? -> Antwort: Nein
select 
    count(h.trip_id),
    date_part('hour', h.check_out_time) as check_out_hour,
    date_part('minute', h.check_out_time) as check_out_minute,
    date_part('second', h.check_out_time) as check_out_seconds
from hotels as h
where date_part('hour', h.check_out_time) <> 11 -- gibt es andere check_out_time als 11 Uhr?
group by check_out_hour, check_out_minute, check_out_seconds
limit 100
-- offenbar gibt es NUR Check_out Time um 11 Uhr. Also auch kein Zusammenhang mit Cancel. 
-- Die Check_Out_Zeit entspricht NICHT der tatsächlichen Check_out_zeit
;


with sessions_2023 as (
    select *
        from sessions
    where session_start >= '2023-01-05'
),
over_seven_sessions as (
    select user_id, 
     count(*) session_count
    from sessions
    group by user_id
    having count(*) > 7
),
prep_sessions_basiert as (
  select 
    s.session_id, 
  	s.user_id, 
  	s.trip_id, 
  	s.session_start, 
  	s.session_end, 
  	s.page_clicks,
 	s.flight_discount, 
  	s.flight_discount_amount, 
  	s.hotel_discount, 
  	s.hotel_discount_amount, 
  	s.flight_booked, 
  	s.hotel_booked, 
  	s.cancellation,
	u.birthdate, 
  	u.gender, 
  	u.married, 
  	u.has_children, 
  	u.home_country, 
  	u.home_city, 
  	u.home_airport, 
  	u.home_airport_lat, 
  	u.home_airport_lon,
  	u.sign_up_date,
 	f.origin_airport, 
  	f.destination, 
  	f.destination_airport, 
  	f.seats, 
  	f.return_flight_booked, 
  	f.departure_time, 
  	f.return_time, 
  	f.checked_bags, 
  	f.trip_airline, 
  	f.destination_airport_lat, 
  	f.destination_airport_lon,
  	f.base_fare_usd,
 	h.hotel_name, 
  	h.nights, 
  	h.rooms, 
  	h.check_in_time, 
  	h.check_out_time, 
  	h.hotel_per_room_usd hotel_price_per_room_night_usd
  from sessions_2023 s
  left join users u
  on s.user_id = u.user_id
  left join flights f
  on s.trip_id = f.trip_id
  left join hotels h
  on s.trip_id = h.trip_id
  where s.user_id in (select user_id from over_seven_sessions)
  )
select 
	count(*)
from prep_sessions_basiert;



WITH sessions_2023 as (
    select 
        user_id, 
        session_id, 
        session_start,
        count(*) over(partition by user_id order by session_start) as number_of_sessions -- total sessions of this user
    from sessions
    where session_start >= '2023-01-05') -- keep users with sessions after 2023-01-05
select 
    *
from sessions_2023
where number_of_sessions > 7 -- keep users with sessions above 7
;



with sessions_2023 as (
    select 
        *, 
		count(*) over(partition by user_id) sessions
    from sessions
    where session_start >= '2023-01-05'
)
select
    count(sessions)
from sessions_2023 
where sessions >7
;





-- some more statistics
WITH sessions_2023 as (
    select user_id, 
    session_id, 
    session_start,
    session_end,
    round(EXTRACT(EPOCH FROM (session_end - session_start))::DEC / 60,2) as session_duration_min,
    flight_discount,
    hotel_discount,
    cancellation,
    hotel_booked,
    flight_booked,
    count(*) over(partition by user_id) as number_of_sessions
    from sessions
    where session_start >= '2023-01-05')
select 
    min(session_start) as min,
    max(session_start) as max,
    count(session_id) as sessions_cnt,
    round(avg(number_of_sessions),1) as avg_sessions_p_user,
    round(avg(session_duration_min),2) as avg_session_duration_min,
    round(AVG(case when flight_discount = TRUE AND cancellation = False THEN 1 ELSE 0 end),2) as flight_discount_offer_rate,
    round(AVG(case when hotel_discount = TRUE AND cancellation = False THEN 1 ELSE 0 end),2) as hotel_discount_offer_rate,
    min(number_of_sessions) as sessions_min,
    max(number_of_sessions) as sessions_max,
    count(distinct user_id) as users_cnt,
    sum(case when cancellation = False and (hotel_booked = True or flight_booked = True) then 1 else 0 end) as bookings_cnt,
    sum(case when cancellation = True then 1 else 0 end) as cancellation_cnt,
    sum(case when cancellation = FALSE then case when hotel_booked = TRUE THEN 1 ELSE 0 END ELSE 0 END) as hotels_booked,
    sum(case when cancellation = FALSE then case when flight_booked = TRUE THEN 1 ELSE 0 END ELSE 0 END) as flights_booked

from sessions_2023
where number_of_sessions > 7            -- ergibt 31024 mit 7-12 sessions per user
;






WITH sessions_2023 as (
    select session_id, user_id
    from sessions
    where session_start >= '2023-01-05'
),
over_seven_sessions as (
	select 
		distinct user_id
	from sessions_2023
	group by user_id
	having count(*) > 7
),
prep_table as (
    select 
        *,
        extract('year' from age(u.birthdate)) as user_age,
        case when flight_discount = True then 1 else 0 end as flight_discount_cleaned, 
        case when hotel_discount = True then 1 else 0 end as hotel_discount_cleaned, 
        case when flight_booked = True then 1 else 0 end as flight_booked_cleaned, 
        case when hotel_booked = True then 1 else 0 end as hotel_booked_cleaned, 
        case when cancellation = True then 1 else 0 end as cancellation_temp, 
        case when return_flight_booked = True then 1 else 0 end as return_flight_booked_cleaned,
        case when gender = 'M' then 1 else 0 end as gender_cleaned, -- M=1 | F=0 
        case when married = True then 1 else 0 end as married_cleaned, 
        case when has_children = True then 1 else 0 end as has_children_cleaned,
        case when lower(home_country) = 'usa' then 1 else 0 end as home_country_cleaned, -- usa = 1, canada = 0,
        case when seats is not null then seats else 0 end as seats_cleaned,
        case 
            when hotel_per_room_usd IS NOT NULL and hotel_booked = True and cancellation = False
            THEN (hotel_per_room_usd - (hotel_per_room_usd * COALESCE(hotel_discount_amount, 0)))
            ELSE null
        END as hotel_p_room_p_night_after_discount_temp,
        CASE 
            WHEN base_fare_usd IS NOT NULL AND flight_booked = TRUE AND cancellation = False
            THEN (base_fare_usd * (1 - COALESCE(flight_discount_amount,0)))/ COALESCE(seats,1)
            ELSE null -- evtl. do -9999 for machine learning???
        END as flight_price_p_seat_after_discount_cleaned,
        case 
            when (hotel_booked = True or flight_booked = True) 
                then case 
                    when flight_booked = True 
                    then (EXTRACT(EPOCH FROM (departure_time - session_start)) / 86400) :: INT
                    else (EXTRACT(EPOCH FROM (check_in_time - session_start)) / 86400) :: INT
                end
            else null -- if nothing is booked
        end as days_between_booking_and_trip_cleaned, 
        MIN(EXTRACT(EPOCH FROM session_start)) OVER () AS session_start_min_epoch_temp,  -- temp_value for recency calculation
        MAX(EXTRACT(EPOCH FROM session_start)) OVER () AS session_start_max_epoch_temp   -- temp_value for recency calculation
    from over_seven_sessions as ov
    left join sessions_2023 as se on se.user_id = ov.user_id
    left join sessions as s on  -- ich komme am ende auf 48069 zeilen. ok?
    left join hotels as h using(trip_id)
    left join flights as f using(trip_id)
    left join users as u using(user_id)
),
metrics as (    -- some basic metrics for evtl. later use
    select 
        cancellation,
        count(distinct user_id) as user_cnt, -- 5782 users, and 596 with cancellations (how many are similar?)
        count(session_id) as sessions_cnt, -- 48069 sessions (thereof 31905 without any booking)
        count(distinct trip_id) as trips_booked_cnt, -- 16164 trips booked, therof 614 canceled
        sum(case when hotel_booked = True then 1 else 0 end) as hotel_booked_cnt, -- booked: 14374
        sum(case when flight_booked = TRUE then 1 else 0 end) as flights_booked_cnt -- booked: 13767
    from prep_table
    group by cancellation  
),
nights_wout_return_flight as (   -- average nights in hotel for imputation in case of no return_flight_booked in CTE clean_nights
    SELECT
        avg(nights) as avg_nights
    FROM prep_table
    where hotel_booked = True and flight_booked = True and return_flight_booked = False
),
clean_nights as(    -- clean nights table 
    SELECT
        session_id,
        CASE 
            WHEN nights <= 0 and return_flight_booked = False THEN (SELECT avg_nights FROM nights_wout_return_flight)
            WHEN nights <= 0 and return_flight_booked = True THEN (EXTRACT ('day' from (return_time - check_in_time)) +1)
            ELSE nights 
        END as nights_cleaned
    FROM prep_table
) -- needs to be joined in later. Info: returns 2.74 for nights IF impute with average is required
select 
    *,
    MAX(cancellation_temp) OVER(PARTITION BY trip_id) as cancellation_cleaned, -- also flag the booking session of trips which got canceled
    coalesce((hotel_p_room_p_night_after_discount_temp * nights_cleaned * rooms),0) + 
                coalesce((base_fare_usd * (1 - COALESCE(flight_discount_amount,0))),0) 
                as monetary_value_per_trip_USD_cleaned,
    ROUND(
        (EXTRACT(EPOCH FROM session_start) - session_start_min_epoch_temp) 
        / NULLIF(session_start_max_epoch_temp - session_start_min_epoch_temp, 0), 4
    ) AS recency_norm_cleaned,
    min(session_start) over(),
    max(session_start) over()
    -- following select variables can be deleted finally
    --
from prep_table as t 
inner join clean_nights as c using (session_id)
where session_start < '2023-01-05' -- shows 1247 rows
; -- count rows: 48683 without filter minus 1247 rows of data < 2023-01-05 = 47436 rows !! which is exactly like @ beginning - WHAT HAPPENED?

-- PROBLEM: wieso sind session_starts VOR dem 05.01.2023 drin? und wieso sind es 48683 rows
-- next: create recency, frequency (to check)
-- Stornoquote (geht nur auf user_ebene)
-- Preissensitivität
-- OK ------------------------------------------
-- days_between_booking_and_trip OK
-- hotel after discount
-- flight per seat after discount
-- total monetary value

cancellations as (
    select trip_id as cancelled_trip_id
    from basis_table
    where cancellation = True
),


-- AVG Number of Bookings per User | WAS IST HIER FALSCH?




avg_metrics as (
    SELECT 
        s.user_id,
        count(s.session_id) as sessions_cnt,
        count(s.trip_id) as trips_cnt,
        sum(case when s.hotel_booked = TRUE THEN 1 else 0 end) as hotels_booked_cnt,
        sum(case when s.flight_booked = True then 1 else 0 end) as flights_booked_cnt
    FROM sessions as s
--    WHERE s.cancellation = False and s.user_id in (SELECT user_id from users where sign_up_date >= '04.01.2023')
    WHERE s.user_id in xxxx
    GROUP BY s.user_id
)
SELECT
    round(avg(sessions_cnt),1) as avg_sessions,
    round(avg(trips_cnt),1) as avg_booked_trips,
    round(avg(hotels_booked_cnt),1) as avg_booked_hotels,
    round(avg(flights_booked_cnt),1) as avg_flights_booked,
    count(distinct user_id)
FROM avg_metrics as m 
;
-- ERGEBNIS: 2.9 Sessions per user (2.8 ohne cancels) | 1.2 booked trips | 1.0 booked hotels | 1.0 booked flights | 310907 users
-- FRAGE: wie können die avg trips größer sein als hotels und flights

SELECT
    s.trip_id,
    s.hotel_booked,
    s.flight_booked
FROM sessions as s
LEFT JOIN hotels as h
--where h.trip_id is null and s.hotel_booked = TRUE
;


-- erstelle flag für canceled (auch für die ursprüngliche buchung)
with prep as (
    SELECT
        trip_id,
        session_id,
        session_start,
        CASE 
            WHEN cancellation = True THEN 1  
            ELSE 0 
        END as cancellation
    FROM sessions as s
    order by trip_id, session_id
)
SELECT
    *,
    MAX(cancellation) OVER(PARTITION BY trip_id)
FROM prep
order by trip_id, session_start
limit 1000
;
