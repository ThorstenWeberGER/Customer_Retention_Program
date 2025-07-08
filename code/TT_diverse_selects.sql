-- Feature-Engineering: Create flag on session level indicating trip has been canceled (includes booking session and cancelation session)
-- Tables used: only sessions
with flag_canceled_trips as (
	select
		s.trip_id,
	  	s.CANCELLATION as cancellation_session,
		max(case when s.cancellation = true then 1 else 0 end) over(partition by s.trip_id) as trip_cancelled,
	    s.HOTEL_DISCOUNT_AMOUNT, 
	    s.FLIGHT_DISCOUNT_AMOUNT
	from sessions as s
	where s.trip_id is not null
	)
select
	trip_cancelled,
	avg(hotel_discount_amount) as avg_hotel_discount,
	avg(flight_discount_amount) as avg_flight_discount
from flag_canceled_trips as f
group by trip_cancelled
;
-- discounts are same for canceled or not canceled trips
-- DONE TRANSFERED


-- INSIGHT QUERY: what is the impact of discount yes/no and discount_rate on booking-rate?
select
	s.FLIGHT_DISCOUNT and s.HOTEL_DISCOUNT as discount_flag,
	trip_id is not null as booking_flag,
	count(session_id),
	count(session_id)::dec / sum(count(session_id)) over(partition by s.FLIGHT_DISCOUNT and s.HOTEL_DISCOUNT) as share_of_discount_flag
from sessions as s
where cancellation = false
group by discount_flag, booking_flag
;
-- Insight: offered discounts do not make a difference. flights are being booked 42% of all sessions. 
-- STATUS: transfered, done

-- INSIGHT QUERY: does discount height make a difference?
-- do discounts impact differently depending on age group?
with prep as (
	select
		s.session_id,
		floor(extract('year' from age(u.birthdate)) / 10) * 10 as age_bucket,
		s.FLIGHT_DISCOUNT and s.HOTEL_DISCOUNT as discount_flag,
		trip_id is not null as booking_flag
	from sessions as s
	inner join users as u using(user_id)
	where cancellation = false
),
prep2 as(
	select
		age_bucket,
		discount_flag,
		booking_flag,
		count(session_id)
	from prep
	group by age_bucket, discount_flag, booking_flag
)
select 
	age_bucket,
	discount_flag,
	booking_flag,
	count:: dec / sum(count) over(partition by age_bucket, discount_flag) as booking_rate
from prep2
order by age_bucket, discount_flag, booking_flag
;

-- INSIGHT: the impact of discount strongly depends on age group. Below 30s show a 5% higher conversion (+ 25%) with discounts than without.
--          age between 30 and 60 discount has no ipact on booking
--          for senior citizens we can again see a push of around 4% by offered discounts

-- Status: not transfered



-- Feature engineering: Flag all trips which were booked 14 days ahead of travel start (fligh departure)
-- Tables used: sessions, flights
-- Type: single routine
-- Helps for: last_minute_booker
SELECT
  CASE
    WHEN 
      extract('day' from (f.DEPARTURE_TIME - s.SESSION_END)) <= 14 
    THEN 1 ELSE 0
  end as type_last_minute
FROM sessions as s
inner join flights as f using(trip_id)
WHERE cancellation = False AND trip_id is not null
;   -- 1.6 Mio !!!
-- status: transfered, done

-- Insights Query: Count young users and their share of total users
-- Status: not transfered - OPEN
-- how many young users are there and what is their share from total users
SELECT 
	has_children,
	count(user_id) as young_users_cnt,
	count(user_id)::DEC / (select count(user_id) from users) as youngsters_share_of_all
FROM users
where extract('year' from age(birthdate)) < 20
group by has_children
; -- 5% (roughly 47k) young users, !!! 25% have children !!

-- Insight Query: Stats for below 20s users
-- Status: not transfered - OPEN
-- how many bookings have they booked (including cancellation)
SELECT 
	s.CANCELLATION,	
	count(s.trip_id) as below_20s_trips_cnt,
	count(s.trip_id)::dec / sum(count(distinct trip_id)) over() as share_of_all_trips2, 
	sum(case when s.HOTEL_BOOKED = true then 1 else 0 end) as hotels_booked_cnt,
	sum(case when s.FLIGHT_BOOKED = true then 1 else 0 end) as flight_booked_cnt,
	sum(case when h.trip_id is not null then 1 else 0 end) as hotels_booked_h_cnt,
	sum(case when f.trip_id is not null then 1 else 0 end) as flights_booked_f_cnt,
	avg(s.FLIGHT_DISCOUNT_AMOUNT) as flight_discount_avg,
	avg(s.HOTEL_DISCOUNT_AMOUNT) as hotel_discount_avg,
	avg(f.CHECKED_BAGS) as avg_checked_bags,
	avg(h.rooms) as avg_rooms,
	avg(f.seats) as avg_seats
FROM users as u 
inner join sessions as s using(user_id)
left join hotels as h using(trip_id)
left join flights as f using(trip_id)
where extract('year' from age(u.birthdate)) < 20
group by s.cancellation
;
-- seltsam wir haben 7032 cancellations. von denen gibt es noch 3160 gebuchte hotels und 6953 gebuchte flights
-- evtl liegt das daran, dass die dazugehörigen buchungen deutlich seltener hotels gebucht wurden?
-- hypothese: die hotels und flights bleiben in der datenbank
-- youngster haben 14% discount auf flights und 11% discount auf hotels

-- Insight Query: Check booking behaviour (booked rooms, checked bags, seats) for different age groops
-- Status: not transfered, OPEN
-- wie verhalten sich gebuchte zimmer, checked_bags und avg_seats abhängig von altersgruppen
select
	FLOOR(extract('year' from age(u.birthdate)) / 10) * 10 AS age_group, -- TIPP: Nice way to cluster age groups
	count(distinct u.user_id) as user_cnt,
	count(session_id) as sessions_cnt,
	count(s.trip_id) as trip_cnt,
	count(s.trip_id)::dec / count(session_id) as trips_per_session,
	sum(s.page_clicks)::dec / count(session_id) as clicks_per_session,
	stddev(s.page_clicks) as std_page_clicks,
	sum(s.page_clicks)::dec / count(s.trip_id) as clicks_per_trip,
	sum(case when h.trip_id is not null then 1 else 0 end) as hotels_booked_cnt,
	sum(case when f.trip_id is not null then 1 else 0 end) as flights_booked_cnt,
	avg(s.FLIGHT_DISCOUNT_AMOUNT) as flight_discount_avg,
	avg(s.HOTEL_DISCOUNT_AMOUNT) as hotel_discount_avg,
	avg(f.CHECKED_BAGS) as avg_checked_bags,
	avg(h.rooms) as avg_rooms,
	avg(f.seats) as avg_seats	
from USERS as u
inner join sessions as s using(user_id)
left join hotels as h using(trip_id)
left join flights as f using(trip_id)
where 
	cancellation = false
group by age_group
order by age_group asc
;
-- booking behaviour is quite similar among age groups for many attributes
-- avg_flight_discount around 14%, avg_hotel_discount around 11%, avg_rooms 1.2
-- avg_seats between 1.22 and 1.3 for age_groups 18+ until 60
-- avg_seats increase 60+ to 1.60 seats per flight
-- avg_checked bags also vary stronger in age_groups with 0.60 on average for 18+ til 60 years
-- avg_checked_bags increasing for elderly travelers around 0.78 on average
-- on average hotels are booked slightly more across all age groups than flights deviating by age group a little
-- avg clicks per sessions rnge around 16 on average with a standard deviation of 14. 30-60s year olds tend to have higher clicks per sessions
-- avg clicks per trip ranges around 55 for 18+ til 30s, 37 for 30+ til 60 increasing to around 65 for 70+
-- avg_trips_per_sessions is on contrast significantly higher for 30+ til 60. for other age groups drops down to 0.3 and even 0.2
-- > younger and older people need more sessions and clicks for a booking
-- > higher clicks per session for 30+ till 60 could be due to more clicks for choosing family kids, ...
-- > people who work and earn have higher booking per session rate (i.e. less time for looking, less reason for saving?)


-- Feature Engineering: calculate session_duration
-- Uses tables: sessions
-- Requires other feature: session_type
-- Status: transfered, OK
select
	(floor(extract('year' from age(u.birthdate)) / 10) * 10) as age_group,
	s.cancellation,
	(s.hotel_booked and s.flight_booked) as booking_yes_no,
	avg(extract(EPOCH from s.session_end) - extract(epoch from s.session_start)) as avg_duration_sec,
	stddev(extract(EPOCH from s.session_end) - extract(epoch from s.session_start)) as std_duration_sec,
	min(extract(EPOCH from s.session_end) - extract(epoch from s.session_start)) as min_duration_sec,
	max(extract(EPOCH from s.session_end) - extract(epoch from s.session_start)) as max_duration_sec
from sessions as s
inner join users as u using(user_id)
group by age_group, s.cancellation, booking_yes_no
order by cancellation, booking_yes_no, age_group
;

-- significant difference in duration of session depending on
--      booking: 200 seconds (90 seconds std)
--      cancellation: 5400 seconds (2400 seconds std)
--      browsing: 100 seconds (95 seconds std, min of 1, max of 6500)
-- no difference in age groups
-- RELEVANT FOR: how to identify "zögerer" und "window-shopper"
-- IMPORTANT: needs to be re-run for for filtered sessions (2023 and only user with +7 sessions)


-- window shopper: upper percentile of clicks per session but no bookings (only compare with pure browsing no booking sessions?)
-- step 1: flag_sessions according to booking, cancellation, browsing
with session_type as (
	select 
		session_id,
		case
			when (s.trip_id is not null and s.cancellation = false) then 'booking'
			when (s.trip_id is not null and s.cancellation = true) then 'cancellation'
			when (s.trip_id is null and s.cancellation = false) then 'browsing'			
		end as session_type,
		extract('epoch' from (s.session_end - s.session_start)) as session_duration_sec,
		s.page_clicks as page_clicks
	from sessions as s
),
-- step 2: calculate 0.85 for each booking type as a threshold for high-clicker per session_type
session_click_p85 as(
	select
		session_type,
		avg(page_clicks) as avg_page_clicks,
		percentile_disc(0.85) within group (order by page_clicks) as percentile85_page_clicks
	from session_type as s
	group by session_type
)
-- step 2: flag sessions according to very clicks/session - ratio above 0.85 percentile
select *
from session_click_p85
;




-- step 3: select only users without any booking so far and calculate metrics
select
	session_type,
	count(session_type),
	avg(s.page_clicks) as avg_clicks_per_session,
	percentile_disc(0.85) WITHIN GROUP (ORDER BY s.page_clicks) AS percentile85_clicks_per_session
from session_type as st
inner join sessions as s using(session_id)
where s.user_id in (select distinct user_id from sessions group by user_id having count(trip_id) = 0) -- mark users without booking
group by session_type
;	


-- Feature Engineering: cluster travel distance in short-medium-long-ultra-long hauls using haversine_distance
-- Tables required: sessions, users
-- Type: two steps, first feature: distance, second feature distance category
-- Status: transfered, DONE
-- STEP 1: flight_distance
with flight_distance as (
	select
		floor(extract('year' from age(u.birthdate) / 10)) * 10 as age_group,
		avg(6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(f.destination_airport_lat - u.home_airport_lat) / 2), 2) +
			COS(RADIANS(u.home_airport_lat)) * COS(RADIANS(f.destination_airport_lat)) *
			POWER(SIN(RADIANS(f.destination_airport_lon - u.home_airport_lon) /2),2)
		))) AS avg_total_distance_km
	FROM flights f
	LEFT JOIN sessions s ON s.trip_id = f.trip_id
	LEFT JOIN users u ON s.user_id = u.user_id
	--WHERE u.home_airport = 'LGA' AND f.destination_airport = 'MDW'
	GROUP BY age_group, u.home_airport, f.destination_airport, f.destination_airport_lat, u.home_airport_lat,
	f.destination_airport_lon, u.home_airport_lon
),
-- STEP 2: flight_categories
flight_distance_categories as(
	select 
		*,
		case
			when avg_total_distance_km < 1500 then 'short-haul'
			when avg_total_distance_km < 3500 then 'medium-haul'
			when avg_total_distance_km < 6500 then 'long-haul'
			else 'ultra-long-haul'
		end as distance_category
	from flight_distance
),
prep as(
select
	distance_category,
	age_group,
	count(distance_category)
from flight_distance_categories
group by distance_category, age_group
order by distance_category, age_group
)
select
	*,
	count::dec / sum(count) over(partition by distance_category) as share_of_distance_category
from prep
order by distance_category, age_group
;
-- age_groups show in flight_distance_category with similar shares
-- thus: +18-20 book 10% of the short-medium-long-ultra long-flights
-- same is true for other age groups
-- interestingly there is few difference in average and stdev flight_distance across age_groups
-- on average around 5400km flight distance with stddev of 4600km also similar across age groups



-- Insight Query: when do bookings happen: day of week, time during day
-- Status: not transfered, OPEN
SELECT
    EXTRACT('DOW' FROM s.session_start) AS session_dow,
    COUNT(s.trip_id) AS trip_cnt,
    COUNT(s.trip_id)::DEC / SUM(COUNT(s.trip_id)) OVER() AS share_of_total    -- << NOTE: without sum(count...) over() does not work!
FROM
    SESSIONS AS s
WHERE
    s.CANCELLATION = FALSE
    AND s.TRIP_ID IS NOT NULL
GROUP by
	session_dow
ORDER BY
    session_dow;

-- bookings around 320k every day (share 14%). Day of week makes no difference.
-- more bookings happen during afternoon 1600 til 2100 hours;



-- Feature Engineering: airline_category
-- Use for: identify  bookers as bookers with price per seat in 0.75 quintile or price per room in 0.75 quintile
-- start with flights categorizing the airlines into cheap, premium, average created with help of chatGPT
-- Status: not transfered, done
with prep as (
select
	distinct f.trip_airline,
  CASE
    -- Cheap / Low-cost Airlines
    WHEN f.trip_airline IN (
      'Ryanair', 'easyJet', 'Wizz Air', 'AirAsia', 'AirAsia X', 'Jet2.com',
      'Spirit Airlines', 'Allegiant Air', 'Flybe', 'Volaris', 'Tiger Airways',
      'Tiger Airways Australia', 'Pegasus Airlines', 'Norwegian Air Shuttle',
      'Frontier Airlines', 'Cebu Pacific', 'IndiGo Airlines', 'Jetstar Airways',
      'Jetstar Asia Airways', 'Jetstar Pacific', 'Mango', 'Air Arabia',
      'Transavia Holland', 'SunExpress', 'Blue Panorama Airlines', 'AirTran Airways',
      'bmibaby', 'TUIfly', 'Volotea Costa Rica', 'FlyNordic', 'Valuair',
      'Go Air', 'Nok Air', 'Nas Air', 'Nasair', 'Flybaboo'
    ) THEN 'cheap'
    -- Premium / Full-service International Airlines
    WHEN f.trip_airline IN (
      'Singapore Airlines', 'Emirates', 'Qatar Airways', 'Etihad Airways',
      'Cathay Pacific', 'Japan Airlines', 'All Nippon Airways', 'Swiss International Air Lines',
      'Virgin Atlantic Airways', 'British Airways', 'Qantas', 'EVA Air',
      'Korean Air', 'Lufthansa', 'KLM Royal Dutch Airlines', 'Air France',
      'United Airlines', 'Delta Air Lines', 'American Airlines'
    ) THEN 'premium'
    -- All others default to Average (Full-service or Hybrid)
    ELSE 'average'
  END AS airline_category
from sessions as s
inner join flights as f using(trip_id)
where s.FLIGHT_BOOKED = true and s.trip_id is not null
order by f.TRIP_AIRLINE asc
)
select *
from prep
order by trip_airline
;




-- flag trips with premium hotel_bookings
-- done re-written with perentile_rank
WITH hotel_discounted_prices AS (
  SELECT
    s.trip_id,
    case when s.cancellation = false and s.trip_id is not null then (h.hotel_per_room_usd * (1 - COALESCE(s.hotel_discount_amount, 0))) end AS price_after_discount
  FROM hotels h
  LEFT JOIN sessions s USING(trip_id)

),
percentile_val AS (
  SELECT
    percentile_disc(0.85) WITHIN GROUP (ORDER BY price_after_discount) AS p85
  FROM hotel_discounted_prices
)
SELECT
  hdp.trip_id,
  CASE 
    WHEN hdp.price_after_discount >= p85.p85 THEN 1
    ELSE 0
  END AS is_premium_hotel_rate
FROM hotel_discounted_prices as hdp, percentile_val as p85 -- cross join, d.h. ohne explizite verknüpfung
;

-- INSIGHT: Query
--- what flight discounts were give and how often
with flight_discounts as (
	select
		distinct s.flight_discount_amount,
		count(s.trip_id) as trips_cnt,
		count(s.trip_id)::dec / sum(count(s.trip_id)) over() as share_of_total
	from flights as f
	inner join sessions as s using(trip_id)
	where s.cancellation = false and s.trip_id is not null and s.FLIGHT_DISCOUNT_AMOUNT is not null
	group by s.FLIGHT_DISCOUNT_AMOUNT
)
select
	*,
	sum(share_of_total) over(order by flight_discount_amount) as running_total
from flight_discounts
;
-- 75% of discounts are below up to 0.15 precent
-- 92% are up to and including 0.25 percent


----- INSIGHT: query
----- calculate averages for flight discount and ratio of flights booked with a discount
select
	avg(f.base_fare_usd) as avg_base_fare,
	avg(f.seats) as avg_seats,
	avg(s.flight_discount_amount) as avg_flight_discount_amount, 
	max(AVG_FLIGHT_DISCOUNT),
	avg(case when s.flight_discount = true then 1 else 0 end) as given_discount_ratio,
	avg(f.BASE_FARE_USD * (1 - s.flight_discount_amount)::dec / (case when seats = 0 then 1 else seats end)) as avg_airfare_per_seat_after_discount,
from flights as f
inner join sessions as s using(trip_id)
where s.cancellation = false and s.trip_id is not null
;
-- out of all flights, 15% were booked with a discount
-- (weighted) average flight discount is around 14%
-- average airfare is around 394 USD after discount per seat

--- INSIGHT QUERY
----- show distribution of air_fare_per_seat_after_discount (bin width = 50s)
with flight_distance as (
	select
		f.trip_id,
		6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(f.destination_airport_lat - u.home_airport_lat) / 2), 2) +
			COS(RADIANS(u.home_airport_lat)) * COS(RADIANS(f.destination_airport_lat)) *
			POWER(SIN(RADIANS(f.destination_airport_lon - u.home_airport_lon) /2),2))) AS total_distance_km
	FROM flights f
	LEFT JOIN sessions s ON s.trip_id = f.trip_id
	LEFT JOIN users u ON s.user_id = u.user_id
	GROUP BY f.trip_id, u.home_airport, f.destination_airport, f.destination_airport_lat, u.home_airport_lat,
	f.destination_airport_lon, u.home_airport_lon
),
flight_distance_categories as(
	select 
		*,
		case
			when total_distance_km < 1500 then 'short-haul'
			when total_distance_km < 3500 then 'medium-haul'
			when total_distance_km < 6500 then 'long-haul'
			else 'ultra-long-haul'
		end as distance_category
	from flight_distance
),
fare_seat_after_discount as (
	select
		f.trip_id,
		f.BASE_FARE_USD,
		s.FLIGHT_DISCOUNT,
		s.FLIGHT_DISCOUNT_AMOUNT,
		f.SEATS,
		-- note: coalesce with flight_discount_amount, because 1 - NULL = NULL !!
		f.BASE_FARE_USD * (1 - coalesce(s.flight_discount_amount,0))::dec / (case when seats = 0 then 1 else seats end) as airfare_per_seat_after_discount,
		d.TOTAL_DISTANCE_KM,
		d.DISTANCE_CATEGORY as distance_category
	from FLIGHTS as f
	inner join sessions as s using(trip_id)
	inner join flight_distance_categories as d using(trip_id)
	where s.CANCELLATION = false and s.TRIP_ID is not null and extract('year' from s.SESSION_START) >= 2023 -- später rausnehmen
)
select 
	DISTANCE_CATEGORY,
	min(airfare_per_seat_after_discount) as min_airfare_per_eat_after_discount,
	avg(airfare_per_seat_after_discount) as avg_airfare_per_eat_after_discount,
	percentile_disc(0.15) WITHIN GROUP (ORDER BY airfare_per_seat_after_discount) AS percentile15_airfare_per_eat_after_discount,
	percentile_disc(0.5) WITHIN GROUP (ORDER BY airfare_per_seat_after_discount) AS median_airfare_per_eat_after_discount,
	percentile_disc(0.85) WITHIN GROUP (ORDER BY airfare_per_seat_after_discount) AS percentile85_airfare_per_eat_after_discount,
	max(airfare_per_seat_after_discount) as max_airfare_per_eat_after_discount,
	stddev(airfare_per_seat_after_discount) AS std_airfare_per_eat_after_discount,
	count(trip_id)
from fare_seat_after_discount
group by DISTANCE_CATEGORY
order by percentile_disc(0.5) WITHIN GROUP (ORDER BY airfare_per_seat_after_discount)
;
-- insight: airfaire_per_km_after_discount around 0.17$. not sufficient difference between short and long flights
-- 390k short-haul-flights, 490k medium, 156k long and 68k ultra-long
-- lower 0.15 and upper 0.85 percentile can be used solid for differentiation of price_category. 
-- std and spread are large enough for clear differentiation
-- 0.15 and 0.85 are chosen for easy of use instead of the outlier definition or median +- 1.5 IQR


-- FEATURE ENGINEERING
--- ALREADY DONE
-- calculate flight distance based on haverstine distance
with flight_distance as (
	select
		trip_id,
		6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(f.destination_airport_lat - u.home_airport_lat) / 2), 2) +
			COS(RADIANS(u.home_airport_lat)) * COS(RADIANS(f.destination_airport_lat)) *
			POWER(SIN(RADIANS(f.destination_airport_lon - u.home_airport_lon) /2),2))) AS total_distance_km
	FROM flights f
	inner join sessions s using(trip_id)
	inner join users u using(user_id)
),
flight_distance_categories as(
	select 
		*,
		case
			when total_distance_km < 1500 then 'short-haul'
			when total_distance_km < 3500 then 'medium-haul'
			when total_distance_km < 6500 then 'long-haul'
			else 'ultra-long-haul'
		end as distance_category
	from flight_distance
)
select *
from flight_distance_categories
-- where total_distance_km <= 0 or total_distance_km is null -- check routine if calculation has filled all fields
limit 100;


-- INSIHGT QUERY
-- how many booked flights?
select
	count(trip_id)
from sessions 
where CANCELLATION = false and TRIP_ID is not null and extract('year' from SESSION_START) >= 2023
;-- 1.3 Mio booked flights total

-- INSIGHTS QUERY
-- show distribution of booked seats
SELECT 
	distinct seats,
	count(trip_id) as seats_cnt,
	count(trip_id)::dec / sum(count(trip_id)) over() as share_of_total
from flights
where extract('year' from departure_time) >= 2023
group by SEATS 
order by seats
;
-- Large majority of flights (80%) are with only 1 booked seats. 2 seats are booked 14% of all bookings. 3 and 4 seats are booked rarely. Rest can be considered as outliers up to max of 12 seats booked one time only.
----- compare both categorizations and select better one


-- CLEANING QUERY: clean negative and 0 nights
-- tables required:
-- created feature required:
-- status: Done
-- A: if flight_booked: take difference in days between departure and return flight timestamp
-- B: if no flight_booked: impute with median of nights booked for only hotel_booked
with stats_nights as(
	select
		percentile_disc(0.5) WITHIN GROUP (ORDER BY nights) AS median_nights_no_return_flight
	from sessions as s
	left join hotels as h using(trip_id)
	left join flights as f using(trip_id)
	where 
		h.nights > 0 
	 	and s.trip_id is not null
	 	and s.hotel_booked = true
	 	and f.return_time is null
		and s.cancellation = False
)
select
	s.flight_booked,
	f.return_flight_booked,
	h.nights,
	f.departure_time,
	f.return_time,
	extract('day' from f.return_time - f.departure_time) as travel_days,
-- case flight booked and return booked
	case 
		when f.return_flight_booked = true then
			extract('day' from f.return_time - f.departure_time)
-- case no flight booked impute with median stays for no flights booked or no return booked
		when f.return_flight_booked is not true then
			median_nights_no_return_flight
-- any others? for error checking
		else -1
	end as nights_cleaned
from sessions as s
left join hotels as h using(trip_id)
left join flights as f using(trip_id)
cross join stats_nights
where 
	h.nights <= 0 
 	and s.trip_id is not null 
	and s.cancellation = False
order by random()
limit 100
;
-- komisch der impute für no return flight ist 6. das ist hoch. yes that is correct





-- Feature engineering: RECENCY
-- needs tables: sessions
-- needs additional feature: no, but two steps 
-- level: sessions and later users
-- order: step 1 into creation of the flat table, when joining everything together
--        step 2 into aggregation on user level
--		  step 3 calculate recency stats (requires step 2)
--		  step 4 do scaling after all recency values on user level have been calculated (required step 3)
-- status: VALIDATED, TRANSFERED, DONE
-- QUESTION: Can i write case when into the extract statement? does not work! i want to exclude sessions which do not include a booking
-- QUESTION: This way I will have a NULL value if no booking has happened. should I impute with a negative -9999?

-- step 1
with days_since_last_booking as(
	select 
		s.user_id, 
		extract('day' from max(s.session_start) over() - max(s.session_start) over(partition by s.user_id)) as days_since_last_booking 
		-- i take last recorded booking as reference day cause data is older
	from sessions as s 
	where s.cancellation is false and s.trip_id is not null
	group by s.user_id, s.session_start
),
-- step 2: aggregate on user_level
user_summary as (
	select 
		user_id,
		max(lb.days_since_last_booking) as days_since_last_booking
	from sessions as s
	left join days_since_last_booking as lb using(user_id) -- for later: do a left join on days_since_last_booking so that it will yield a null if no booking has happened
	group by user_id
),
-- step 3: calculate recency stats
recency_stats AS (
  SELECT 
    MIN(days_since_last_booking) AS min_days,
    MAX(days_since_last_booking) AS max_days
  FROM user_summary
),
-- step 4: min-max scale to scoring 1 (long time ago) to 5 (very recent)
recency_scaled AS (
  SELECT 
    us.user_id,
    us.days_since_last_booking,
    -- calculate the scaled recency score
    round(1 + ((rs.max_days - us.days_since_last_booking)::numeric
      / NULLIF(rs.max_days - rs.min_days, 0)) * 4,0) AS recency_score
  FROM user_summary us, recency_stats rs -- cross join to get min/max from all users
)
SELECT 
  user_id,
  days_since_last_booking,
  recency_score
FROM recency_scaled
limit 500;
-- TODO: analyse recency -> create a histogram

-- insight: es gibt knapp 100k registrierte nutzer, die noch nie gebucht haben
-- maßnahme: aktivierung -> we have not seen you a while -> voucher: 1 free night at selected hotels




-- FEATURE ENGINEERING: frequency: how many bookings has user done
-- needs tables: sessions
-- needs additional features: no
-- level: sessions, later user
-- order: step by step - can integrate into code (or have multiple seperate ctes)
-- 		STEP 1: do pretty early, when creating the first flat table as it requires only sessions - OR when aggregating on user_level, i.e. calculate sum_bookings, sum_bookings_not_canceled, sum_cancellations
-- 		STEP 2: after that while doing all the metric - calculation
-- 		STEP 3: after that while doing all the metric - calculation
-- status:  NOT VALIDATED

-- step 1: calculate frequency for every user
with calculate_frequency as (
	select 
		s.user_id, s.session_id, s.trip_id, s.cancellation,
		COUNT(trip_id) FILTER (WHERE s.cancellation = false) -- FILTER()-clause prevents false counting
			OVER (PARTITION BY s.user_id) 
			AS bookings_per_user
	from sessions as s
	group by s.user_id, s.session_id, s.TRIP_ID, s.CANCELLATION
),
-- step 2: calculate min and max frequency of all users
frequency_stats as(
select
	max(bookings_per_user) as max_book_frequency,
	min(bookings_per_user) as min_book_frequency
from calculate_frequency
)
-- step 3: create frequency score 1 (low frequency) to 5 (high frequency)
select
	cf.user_id,
	PERCENT_RANK() OVER (ORDER BY cf.bookings_per_user) AS bookings_per_user_percentile /*,
	-- Min-Max scaled to 1–5
  ROUND(
    1 + (
      (cf.bookings_per_user - fs.min_book_frequency)::numeric 
      / NULLIF(fs.max_book_frequency - fs.min_book_frequency, 0)
    ) * 4,
    0
  ) AS booking_freq_scaled_1_to_5 */
from calculate_frequency as cf, frequency_stats as fs
order by user_id
;

-- monetary_value and more: prices after discount, total price after discount and discount in usd for hotel and flights
-- step 1: what was the individual trip worth on session_id_level -> can be done while creating first flat table
-- step 2: aggregating on user_level -> aggregation on user level
-- step 3: identifying max and min values for scaling -> calculating diverse stats, averages, etc
-- step 4: min-max-scaling with score 1 (minimum) and 5 (maximum) -> when doing all the scoring (last)
-- tables required: sessions, flights, hotels, later: user
-- previous features required: step 3 and 4 require previously calculated total_trip_price_after_discount

-- input required: cleaned_seats and cleaned_nights
-- PROBLEM: by using min-max-scaling I think, the scoring is unfair, as 5 can only be reached by a very few people
-- QUESTION: is percentile_rank and dividing up into 5 equally sized parts better? fairer?
-- IMPORTANT: Solution should also be applied on frequency and recency!!!
-- STATUS: re-factoring still required, several TODOs

-- NOTE: flight_ / hotel_discount_usd will be shown on dashboards (not for )

with calculate_prices_after_discount as (
select
	s.user_id,
	s.session_id,
	f.base_fare_usd * (1 - coalesce(s.flight_discount_amount, 0)) / case when f.seats = 0 then 1 else f.seats end -- put into cleaning procedures
		as flight_price_per_seat_after_discount,    -- checked: OK
	h.HOTEL_PER_ROOM_USD * (1 - coalesce(s.hotel_discount_amount,0)) 
		as hotel_price_per_room_after_discount, -- checked: OK
	coalesce(f.base_fare_usd * (1 - coalesce(s.flight_discount_amount, 0)),0) + 
		coalesce(h.HOTEL_PER_ROOM_USD * (1 - coalesce(s.hotel_discount_amount,0)) * h.nights * h.rooms,0) 
			as trip_price_after_discount,  											-- TODO: h.nights need to be cleaned nights | same for rooms?
	f.base_fare_usd * coalesce(s.flight_discount_amount, 0) 
		as flight_discount_usd,
	coalesce(h.HOTEL_PER_ROOM_USD * coalesce(s.hotel_discount_amount,0) * h.nights * h.rooms,0) 
		as hotel_discount_usd--,
	-- select trip_was caneled -- differentiate late with this
	-- TODO: need to differentiate between trips which happened and which not happened
from
	sessions as s
	left join hotels as h using(trip_id)
	left join flights as f using(trip_id)
),
-- step 2: accumulate on user_id_level
value_on_user as (
select
	cd.user_id,
	sum(cd.trip_price_after_discount) as value_trips_happened, -- need to include trip_was_canceled = False here
	sum(cd.trip_price_after_discount) as value_trips_canceled  -- need to include trip_was_canceled = true and cancel = true (cound only once)
from calculate_prices_after_discount as cd
group by user_id
),
-- step 3: calculate stats: min / max monetary user value
monetary_stats as(
	select
		min(vu.value_trips_happened) as min_value_trips_happened, -- of all users
		max(vu.value_trips_happened) as max_value_trips_happened  -- of all users
	from value_on_user as vu
),
-- step 4: min(1)-max(5)-scaling of monetary value across all users
scale_monetary_value as (
	select 
		*,
		round(
			1 + (
				(cu.value_trips_happened - ms.min_value_trips_happened)::numeric
				/ nullif(ms.max_value_trips_happened - ms.min_value_trips_happened, 0)
			) * 4
			,0 ) as monetary_value_scaled_1_to_5
	from value_on_user as cu, monetary_stats ms
)
select 
	monetary_value_scaled_1_to_5,
	count(*) as cnt
from scale_monetary_value
group by monetary_value_scaled_1_to_5
;
/*
 * -- Min-Max scaled to 1–5
  ROUND(
    1 + (
      (ub.bookings_per_user - s.min_b)::numeric 
      / NULLIF(s.max_b - s.min_b, 0)
    ) * 4,
    2
  ) AS booking_freq_scaled_1_to_5
 */

-- TODO: sum of discounts given in dashboard anzeigen / ratio of discount given compared to all sessions


-- FEATURE ENGINEERING: Business Journey Score
-- STATUS: open
-- identify business journey (1-3 nights, 1 room, 1 seat, age between 20-60, departure and return during week)
with trip_is_business_journey as (
	SELECT 
	  case -- relevant filter for business customers
	    when 
	       h.nights in (1,2,3) and
	       f.seats = 1 and
	       h.rooms = 1 and
	       EXTRACT(DOW FROM f.departure_time) between 1 and 5  AND
	       EXTRACT(DOW FROM f.return_time) between 1 and 5 and
	       s.hotel_booked = true and
	       s.flight_booked = true and
	       extract('year' from u.birthdate) between 20 and 60
	       ---- add clause for short-medium haul
	    then 1
	    else 0
	  end as trip_is_business_journey
	FROM sessions as s
	inner join flights as f using(trip_id)
	left join hotels as h using(trip_id)
	left join users as u using(user_id)
)
select count(*)
from trip_is_business_journey
;

select count( distinct trip_id)
from sessions;

-- roughly 328k business journeys


-- FEATURE ENGINEERING: Senior Flag
-- identify senior citizens 60+ by age only
-- STATUS: transfered, done
SELECT
  CASE 
    WHEN extract('year' from age(birthdate)) >= 60
    THEN 1
    ELSE 0
  END as user_is_senior_citizen
FROM users
;

-- show binned age distribution
SELECT
	floor(extract('year' from age(birthdate)) / 10) * 10 as age_bin,
	count(birthdate)
FROM users
group by age_bin
;




-- flag possible-family-vacation-trip on sessions level
select
	count(s.session_id),
  CASE
    WHEN
       u.HAS_CHILDREN = True -- # 579k trips by people with children
       and f.seats in (3, 4) -- #  22k trips thereof with 3 or 4 seats
       and h.rooms in (1, 2) -- #   8k trips thereof with 1 or 2 rooms 
       and h.nights >5 		 -- #   8k (a little less) trips with longer stay
       and s.HOTEL_BOOKED = True
    then 1
    ELSE 0 
  END AS is_family_trip
from sessions as s
left join flights as f using(trip_id)
left join hotels as h using(trip_id)
left join users as u using(user_id)
WHERE cancellation = false and trip_id is not null 
group by is_family_trip
;



-- flag possible-group-trip on sessions level - only 4.100
-- status: transfered
select
	count(s.session_id),
  CASE
    WHEN
       f.seats > 4 -- #  22k trips thereof with 3 or 4 seats
       and h.rooms > 2 -- #   8k trips thereof with 1 or 2 rooms 
    then 1
    ELSE 0 
  END AS is_group_trip,
  avg(f.CHECKED_BAGS)
from sessions as s
left join flights as f using(trip_id)
left join hotels as h using(trip_id)
left join users as u using(user_id)
WHERE cancellation = false and trip_id is not null 
group by is_group_trip
;


-- select: distribution of checked_bags for all flights
select count(*), CHECKED_BAGS 
from FLIGHTS
group by CHECKED_BAGS 
-- insight: 1.8M booked flights (incl cancelled) have 0 or 1 checked_bags (both around 900k times). Rest of 110k flights focus on 2-3 bags. That is it.



-- select: weekend tripper young couple (traveling without kids), departure friday/saturday and return saturday/sunday, booked hotel and flight, 2 seats, 1 room (could be young couple OR older couple same treat: free candle light dinner)
-- mark in session table
-- status: transfered
select
	count(trip_id),
	case 
		when
			extract('year' from age(u.BIRTHDATE)) between 25 and 55
			and h.rooms = 1
			and f.seats = 2
			and extract('dow' from f.departure_time) in (6,7) -- departure friday or saturday
			--and h.nights in (1,2) -- !! IMPORTANT: USE CLEANED NIGHTS -- stay for 1-2 nights return sunday
		then 1 
		else 0
	end as is_weekend_tripper
from sessions as s
left join users as u using (user_id)
left join flights as f using(trip_id)
left join hotels as h using(trip_id)
where s.CANCELLATION = false and s.TRIP_ID is not null
group by is_weekend_tripper
;


-- when are typical departure days?
select
	'--Departure dates--' as merkmal, null as count
union
select
	extract('dow' from departure_time)::char as merkmal,
	count(trip_id) as count
from flights
group by merkmal
order by merkmal
;
-- insight: departure dates are absolutely uniform distributed, 270k a day

-- when are typical return days?
select
	'--Return dates--' as merkmal, null as count
union
select
	extract('dow' from return_time)::char as merkmal,
	count(trip_id) as count
from flights
group by merkmal
order by merkmal
;
-- insight: return dates are ----absolutely uniform distributed, 260k a day

-- how many nights do travelers stay till return
select
	extract('day' from (return_time - departure_time)) as nights,
	count(*) as count,
	round(count(*):: dec / (select count(*) from flights) * 100,2) as share_of_total
from FLIGHTS
group by nights;
-- insight: 55% of booked flights up to 4 nights. 5 to 12 nights accumulate additional 31%