-- STATUS: Quality checked till end of user_level_aggregation 

/* TRAVEL TIDE - Project: Customer retention analysis and retention program */

-- select only sessions later than 4th Jan 2023
WITH sessions_2023 as (
    select 
        user_id, 
        count(*) over(partition by user_id order by session_start) as number_of_sessions -- total sessions of this user
    from sessions
    where session_start >= '2023-01-05'
),
-- filter users with at more than 7 sessions
over_seven_sessions as (
    select 
        distinct user_id -- eliminate redundant user_ids
    from sessions_2023
    where number_of_sessions > 7
),
-- create some statistics for median nights for later imputation of negative and empty hotel nights
stats_nights as(
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
),
-- accumulate data from all four tables
-- same time do some data transformation (birthdate into age, booleans in 1/0)
prep_sessions_basiert as (
	select
		s.session_id,
		s.user_id,
		s.trip_id,
		max(case when s.cancellation = true then 1 else 0 end) over(PARTITION by s.trip_id) as trip_is_canceled,
		s.session_start,
		s.session_end,
		s.page_clicks,
		case 
			when s.flight_discount = false then 0 
			when s.flight_discount = true then 1 
			else null 
		end as flight_discount_clean,
		s.flight_discount_amount,
		case 
			when s.hotel_discount = false then 0 
			when s.hotel_discount = true then 1 
			else null 
		end as hotel_discount_clean,
		s.hotel_discount_amount,
		case 
			when s.flight_booked = false then 0 
			when s.flight_booked = true then 1 
			else null 
		end as flight_booked_clean,
		case 
			when s.hotel_booked = false then 0 
			when s.hotel_booked = true then 1 
			else null 
		end as hotel_booked_clean,
		case 
			when s.cancellation = false then 0 
			when s.cancellation = true then 1 
			else null 
		end as cancellation_clean,
		extract('year' from age(u.birthdate)) as age, -- calculates age of user at current daytime 
		u.gender,
		case 
			when u.married = false then 0 
			when u.married = true then 1 
			else null 
		end as married_clean,
		case 
			when u.has_children = false then 0 
			when u.has_children = true then 1 
			else null 
		end as has_children_clean,
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
		case 
			when f.return_flight_booked = false then 0 
			when f.return_flight_booked = true then 1 
			else null 
		end as return_flight_booked_clean,
		f.departure_time,
		f.return_time,
		f.checked_bags,
		f.trip_airline,
		f.destination_airport_lat,
		f.destination_airport_lon,
		f.base_fare_usd as flight_price_before_discount_before_cancel,
		h.hotel_name,
		h.nights,
		-- cleaning negative or zero nights. if flight booked, take days between departure and return. if no return flight, take median booked hotel nights of flights without return
		case 
			when h.nights <= 0 and s.trip_id is not null and s.cancellation = False 
			then
				CASE 
					-- case flight booked and return booked
					when f.return_flight_booked = true 
						then extract('day' from f.return_time - f.departure_time)
					-- case no flight booked impute with median stays for no flights booked or no return booked
					when f.return_flight_booked is not true 
						then sn.median_nights_no_return_flight
					-- impute null for other cases
					else null
				end
			else nights -- use nights if nights positive   
		end as nights_clean,
		h.rooms,
		h.check_in_time,
		h.check_out_time,
		h.hotel_per_room_usd as hotel_price_per_room_night_usd
	from sessions as s
		left join users as u using(user_id)
		left join flights as f using(trip_id)
		left join hotels as h using(trip_id)
		cross join stats_nights as sn -- for imputation of median nights
	-- filter on users /w more than 7 sessions and sessions after 4th Jan 2023
	where s.user_id in (select user_id from over_seven_sessions)
),
-- FEATURE ENGINERING on session level
feature_engineering_session_level as(
	select
		*,
		-- calculate days between booking and departure_time
		case 
			when s.cancellation_clean = 0 and s.trip_id is not NULL
			then extract('day' from (s.DEPARTURE_TIME - s.SESSION_END))
			else null
		end as days_between_booking_and_departure, 
		-- last minute booking flag per a booking session if days between booking and departure(trip_start) is below or equal to 14
		CASE
			WHEN 
			extract('day' from (s.DEPARTURE_TIME - s.SESSION_END)) <= 14 
			and s.cancellation_clean = 0
			and s.trip_id is not null
			THEN 1 ELSE null -- TIPP: null is important here if I want to use any svg or percent_rank, ...
		end as is_last_minute_booking,
		-- calculate duration of session in seconds
		extract(EPOCH from s.session_end) - extract(epoch from s.session_start) as session_duration_sec,
		-- categorize sessions into three types
		case
			when cancellation_clean = 0 and trip_id is not null then 'booking'
			when cancellation_clean = 1 and trip_id is not null then 'canceling'
			when cancellation_clean = 0 and trip_id is null then 'browsing'
			else 'no category' 
		end as session_category,
		-- mark sessions which include a trip which has been canceled later
		max(case when s.trip_id is not null and s.cancellation_clean = 1 then 1 else 0 end) over(partition by s.trip_id) as trip_is_cancelled,
		-- calculate days of travel between departure and return in days. return NULL is any of both is NULL
		extract('day' from s.return_time - s.departure_time) as travel_duration_days,
		-- calculate flight distance in km using haversine distance
		6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(s.destination_airport_lat - s.home_airport_lat) / 2), 2) +
				COS(RADIANS(s.home_airport_lat)) * COS(RADIANS(s.destination_airport_lat)) *
				POWER(SIN(RADIANS(s.destination_airport_lon - s.home_airport_lon) /2),2))) AS flight_distance_km,
		-- calculate total hotel price for all rooms and all nights before discount before canceled
		case 
			when s.cancellation_clean = 0 and s.trip_id is not null and s.hotel_price_per_room_night_usd is not null 
			then s.hotel_price_per_room_night_usd * s.rooms * s.nights_clean
		end AS total_hotel_price_before_discount_before_canceled,
		-- calculate total hotel price for all rooms and all nights after discount before canceled
		case 
			when s.cancellation_clean = 0 and s.trip_id is not null and s.hotel_price_per_room_night_usd is not null 
			then (s.hotel_price_per_room_night_usd * (1 - COALESCE(s.hotel_discount_amount, 0))) * s.rooms * s.nights_clean
		end AS total_hotel_price_after_discount_before_canceled,
		-- calculate total hotel price for all rooms and all nights after discount after canceled
		case 
			when s.trip_is_canceled = 0 and s.trip_id is not null and s.hotel_price_per_room_night_usd is not null 
			then (s.hotel_price_per_room_night_usd * (1 - COALESCE(s.hotel_discount_amount, 0))) * s.rooms * s.nights_clean
		end AS total_hotel_price_after_discount_after_canceled,
		-- calculate hotel price per room for one night after discount after canceled 
		case 
			when s.trip_is_canceled = 0 and s.trip_id is not null and s.hotel_price_per_room_night_usd is not null 
			then (s.hotel_price_per_room_night_usd * (1 - COALESCE(s.hotel_discount_amount, 0)))
		end AS hotel_price_per_room_after_discount_after_canceled,
		-- calculate total flight price for all seats booked before discount before cancelation
		case 
			when s.cancellation_clean = 0 and s.trip_id is not null and s.flight_price_before_discount_before_cancel is not null 
			then s.flight_price_before_discount_before_cancel 
		end AS total_flight_price_before_discount_before_canceled,
		-- calculate total flight price for all seats booked after discount before cancelation
		case 
			when s.cancellation_clean = 0 and s.trip_id is not null and s.flight_price_before_discount_before_cancel is not null 
			then (s.flight_price_before_discount_before_cancel * (1 - COALESCE(s.flight_discount_amount, 0))) 
		end AS total_flight_price_after_discount_before_canceled,
		-- calculate total flight price for all seats booked after discount after cancelation
		case 
			when s.trip_is_canceled = 0 and s.trip_id is not null and s.flight_price_before_discount_before_cancel is not null 
			then (s.flight_price_before_discount_before_cancel * (1 - COALESCE(s.flight_discount_amount, 0))) 
		end AS total_flight_price_after_discount_after_canceled,	
   		-- calculate flight price per seat booked after discount after cancelation
		case 
			when s.trip_is_canceled = 0 and s.trip_id is not null and s.flight_price_before_discount_before_cancel is not null 
			then (s.flight_price_before_discount_before_cancel:: dec / s.seats * (1 - COALESCE(s.flight_discount_amount, 0))) 
		end AS flight_price_per_seat_after_discount_after_canceled,		
		case 
			when s.trip_is_canceled = 0 and s.trip_id is not null and s.flight_price_before_discount_before_cancel is not null 
			then (s.flight_price_before_discount_before_cancel:: dec 
            / 	6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(s.destination_airport_lat - s.home_airport_lat) / 2), 2) +
				COS(RADIANS(s.home_airport_lat)) * COS(RADIANS(s.destination_airport_lat)) *
				POWER(SIN(RADIANS(s.destination_airport_lon - s.home_airport_lon) /2),2))) 
            / s.seats * (1 - COALESCE(s.flight_discount_amount, 0))) 
		end AS flight_price_per_seat_per_km_after_discount_after_canceled,	
		-- flag flights which are typical weekendtrips depart on fri/sat and return after 1 or 2 nights
		case 
			when 
				s.cancellation_clean = 0
				and s.trip_id is not null
				and extract('day' from s.return_time - s.departure_time) in (1,2) -- don't use nights, cause of other accomondations possible like friends, family, hostels
				and extract('dow' from s.departure_time) in (6,7) 
			then 1
			else 0
		end as is_weekend_trip,
		max(session_end) over() as last_session_date, -- used as reference for calculating recency
        -- cnt_bookings_with_any discount offered which were not canceled later 
        case 
            when s.trip_is_canceled = 0 and s.trip_id is not null and s.cancellation_clean = 0 and (s.hotel_discount_clean = 1 or s.flight_discount_clean = 1)
            THEN 1 else 0
        end as is_booking_with_discount_offered_not_canceled,
        -- flag for trip happened during week (departure and return and no overlap with weekend)
        case 
            when 
            EXTRACT(DOW FROM departure_time) BETWEEN 1 AND 5 -- Monday–Friday departure
            and EXTRACT(DOW FROM return_time) BETWEEN 1 AND 5 -- Monday–Friday return
            and NOT EXISTS (                                  -- no overlap with a saturday or sunday
                SELECT 1
                FROM generate_series(
                DATE_TRUNC('day', departure_time),
                DATE_TRUNC('day', return_time),
                INTERVAL '1 day'
                ) AS day
                WHERE EXTRACT(DOW FROM day) IN (0, 6)
            )
            then 1 else 0
        end as is_during_week_trip
	from prep_sessions_basiert as s
),
-------------------- calculate the percentile of flight and hotels prices for later counting of these type of trips
-- a) cheap < 0.25percentile for flights and hotels
-- b) expensive > 0.75percentile for flights and hotels
stats_flight_hotel_prices as ( 
    select
        PERCENTILE_CONT(0.2) within group (order by flight_price_per_seat_per_km_after_discount_after_canceled) as p20_flight_price_per_seat_per_km_after_discount_after_canceled,
        PERCENTILE_CONT(0.8) within group (order by flight_price_per_seat_per_km_after_discount_after_canceled) as p80_flight_price_per_seat_per_km_after_discount_after_canceled,
        PERCENTILE_CONT(0.2) within group (order by hotel_price_per_room_after_discount_after_canceled) as p20_hotel_price_per_room_after_discount_after_canceled,
        PERCENTILE_CONT(0.8) within group (order by hotel_price_per_room_after_discount_after_canceled) as p80_hotel_price_per_room_after_discount_after_canceled
    from feature_engineering_session_level
),
------------------------- aggregate metrics on user level
aggregate_on_user_level as(
	select
	-- user metrics
		user_id,
		max(age) as age,
		max(gender) as gender,
		max(married_clean) as is_married,
		max(has_children_clean) as has_children_clean,
		max(home_country) as home_country,
		max(home_city) as home_city,
		max(home_airport) as home_airport,
		max(sign_up_date) as sign_up_date,
		max(round(EXTRACT(EPOCH from age(last_session_date, sign_up_date)) / 60 / 60 / 24,0)) as signed_up_days, -- use last session date of all users as reference for current_time because data is a year old we simulate current_date
		max(case when trip_is_canceled = 0 then session_start else null end) as last_booking_date,
	-- sessions metrics
		count(session_id) as sessions_cnt,
		sum(case when session_category = 'browsing' then 1 else 0 end) as sessions_browsing_cnt,
		sum(case when session_category = 'canceling' then 1 else 0 end) as sessions_canceling_cnt,
		sum(case when session_category = 'booking' then 1 else 0 end) as sessions_booking_cnt,
		sum(page_clicks) as page_clicks_total,
		sum(case when session_category = 'browsing' then page_clicks else 0 end) as page_clicks_clicks_browsing,
		sum(case when session_category = 'canceling' then page_clicks else 0 end) as page_clicks_canceling,
		sum(case when session_category = 'booking' then page_clicks else 0 end) as page_clicks_booking,
		sum(case when flight_discount_clean = 1 or hotel_discount_clean = 1 then 1 else 0 end) as sessions_with_discount_offer_cnt,
		sum(session_duration_sec) as session_duration_sec,
		sum(days_between_booking_and_departure) as sum_days_between_booking_and_departure,
	-- bookings metrics
		count(distinct trip_id) as trips_booked_all,
		sum(case when trip_id is not null and cancellation_clean = 1 then 1 else 0 end) as trips_booked_canceled,
        sum(case when trip_id is not null and trip_is_canceled = 0 then 1 else 0 end) trips_booked_not_canceled,
		sum(case when trip_is_canceled = 0 then flight_booked_clean else 0 end) as flights_booked_not_canceled,
		sum(case when trip_is_canceled = 0 then coalesce(return_flight_booked_clean,0) else 0 end) as return_flights_booked_not_canceled,
		sum(case when trip_is_canceled = 0 then hotel_booked_clean else 0 end) as hotels_booked_not_canceled,
		sum(case when (flight_discount_clean = 1 or hotel_discount_clean = 1) and trip_id is not null and trip_is_canceled = 0 then 1 else 0 end) 
			as trips_booked_with_discount_not_canceled,
		sum(case when (flight_discount_clean = 0 and hotel_discount_clean = 0) and trip_id is not null and trip_is_canceled = 0 then 1 else 0 end) 
			as trips_booked_without_discount_not_canceled,
		sum(case when trip_is_canceled = 0 then is_last_minute_booking else 0 end) as cnt_last_minute_bookings_not_canceled,
		sum(case when trip_is_canceled = 0 then is_weekend_trip else 0 end) as cnt_weekend_trips_not_canceled,
        sum(case when trip_is_canceled = 0 then is_during_week_trip else 0 end) as cnt_is_during_week_trip,
		sum(case when trip_is_canceled = 0 then travel_duration_days else 0 end) as sum_travel_duration_days_not_canceled,
		sum(case when trip_is_canceled = 0 then days_between_booking_and_departure else 0 end) as days_between_booking_and_departure_not_canceled,
	-- hotel metrics
		sum(case when trip_is_canceled = 0 then nights_clean else 0 end) as sum_nights_not_canceled,
		sum(case when trip_is_canceled = 0 then rooms else 0 end) as sum_rooms_not_canceled,
		sum(total_hotel_price_before_discount_before_canceled) as sum_total_hotel_price_before_discount_before_canceled,
		sum(total_hotel_price_after_discount_before_canceled) as sum_total_hotel_price_after_discount_before_canceled,
		sum(total_hotel_price_after_discount_after_canceled) as sum_total_hotel_price_after_discount_after_canceled,
        sum(case 
            when hotel_price_per_room_after_discount_after_canceled < p20_hotel_price_per_room_after_discount_after_canceled
            then 1 else 0 
        end) as cnt_hotel_booking_cheap,
        sum(case 
            when hotel_price_per_room_after_discount_after_canceled > p80_hotel_price_per_room_after_discount_after_canceled
            then 1 else 0 
        end) as cnt_hotel_booking_premium,
	-- flight metrics
		sum(case when trip_is_canceled = 0 then seats else 0 end) as sum_seats_not_canceled,
		sum(case when trip_is_canceled = 0 then checked_bags else 0 end) as sum_checked_bags_not_canceled,
		sum(case when trip_is_canceled = 0 then flight_distance_km else 0 end) as sum_flight_distance_km_not_canceled,
		sum(total_flight_price_before_discount_before_canceled) as sum_total_flight_price_before_discount_before_canceled,
		sum(total_flight_price_after_discount_before_canceled) as sum_total_flight_price_after_discount_before_canceled,
		sum(total_flight_price_after_discount_after_canceled) as sum_total_flight_price_after_discount_after_canceled,	
        sum(case 
            when flight_price_per_seat_per_km_after_discount_after_canceled < p20_flight_price_per_seat_per_km_after_discount_after_canceled
            then 1 else 0 
        end) as cnt_flight_booking_cheap,
        sum(case 
            when flight_price_per_seat_per_km_after_discount_after_canceled > p80_flight_price_per_seat_per_km_after_discount_after_canceled
            then 1 else 0 
        end) as cnt_flight_booking_premium,
	-- total monetary value metrics
		sum(total_hotel_price_before_discount_before_canceled) + sum(total_flight_price_before_discount_before_canceled) as sum_total_trip_price_before_discount_before_cancel,
		sum(total_hotel_price_after_discount_before_canceled) + sum(total_flight_price_after_discount_before_canceled) as sum_total_trip_price_after_discount_before_cancel,
		sum(total_hotel_price_after_discount_after_canceled) + sum(total_flight_price_after_discount_after_canceled) as sum_total_trip_price_after_discount_after_cancel
	from feature_engineering_session_level, stats_flight_hotel_prices
	group by user_id
),
-- CTE user level feature engineering (check further below)
feature_and_metrics_engineering_user_level as ( 
	select *,
		case when age < 20 then 1 else 0 end as is_age_below20,
		case when age between 20 and 60 then 1 else 0 end as is_age_between_20_and_60,
		case when age > 60 then 1 else 0 end as is_age_above60,
        coalesce(round(trips_booked_canceled::dec / nullif(trips_booked_all,0), 2),0) as cancellation_rate,
		coalesce(round(sessions_cnt::dec / nullif(trips_booked_all,0),2), null) as sessions_per_booking,
        coalesce(round(sum_seats_not_canceled::dec / nullif(flights_booked_not_canceled,0), 2),0) as avg_sum_seats_not_canceled,
		coalesce(round(sum_nights_not_canceled:: dec / nullif(hotels_booked_not_canceled,0), 2),0) as avg_sum_nights_not_canceled,
    	coalesce(round(sum_rooms_not_canceled:: dec / nullif(hotels_booked_not_canceled,0), 2),0) as avg_sum_rooms_not_canceled,
		coalesce(round(sum_flight_distance_km_not_canceled::dec / nullif(flights_booked_not_canceled,0), 2),0) avg_flight_distance,
		coalesce(round(sum_days_between_booking_and_departure::dec / nullif(trips_booked_not_canceled,0), 2),0) as avg_sum_days_between_booking_and_departure,
        coalesce(round(trips_booked_with_discount_not_canceled::dec / nullif(trips_booked_not_canceled,0), 2),0) as ratio_trips_booked_with_discount_to_total,
        coalesce(round(sum_total_trip_price_after_discount_after_cancel:: dec / nullif(sum_nights_not_canceled,0) / nullif(sum_rooms_not_canceled,0) ,2),0) as avg_total_hotel_price_after_discount_after_canceled,
        coalesce(round(sum_total_flight_price_after_discount_after_canceled::dec / nullif(sum_seats_not_canceled,0), 2),0) as avg_sum_total_flight_price_per_seat_after_discount_after_canceled,
        coalesce(round(sum_checked_bags_not_canceled / nullif(flights_booked_not_canceled,0) ,2),0) as avg_sum_checked_bags_not_canceled,
		coalesce(round(hotels_booked_not_canceled::dec / nullif(flights_booked_not_canceled,0), 2),0) as ratio_hotel_to_flight_trips,
        coalesce(round(cnt_is_during_week_trip::dec / nullif(trips_booked_not_canceled,0), 2), 0) as ratio_during_week_trip_to_all,
        coalesce(round(cnt_weekend_trips_not_canceled::dec / nullif(trips_booked_not_canceled,0),2),0) as ratio_weekend_trip_to_all,
        -- ratio cheap booked hotels and flights to all booked hotels and flights
        coalesce(round((cnt_hotel_booking_cheap + cnt_flight_booking_cheap)::dec / nullif(hotels_booked_not_canceled + flights_booked_not_canceled,0), 2), 0) as ratio_cheap_hotels_flights_booked_to_all,
        -- ratio premium booked hotels and flights to all booked hotels and flights
        coalesce(round((cnt_hotel_booking_premium + cnt_flight_booking_premium)::dec / nullif(hotels_booked_not_canceled + flights_booked_not_canceled,0), 2), 0) as ratio_premium_hotels_flights_booked_to_all
    from aggregate_on_user_level
)
select
    *
from feature_and_metrics_engineering_user_level
;

