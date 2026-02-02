# data-engineering-zoomcamp
Workshop Codespaces

SQL Code for homework questions week 1
		/*Total trips with distance less than or equal to one mile.*/
		SELECT
			count(1)
		FROM
			green_taxi_trips
		WHERE
			lpep_pickup_datetime >= '2025-11-01' AND lpep_pickup_datetime < '2025-12-01'
			and trip_distance <= 1

		/*Trip date with the longest single trip.*/
		SELECT
			cast(lpep_pickup_datetime AS DATE) as pickup_day,
			max(trip_distance) as max_distance
		FROM
			green_taxi_trips
		WHERE 
			lpep_pickup_datetime >= '2025-11-01' AND lpep_pickup_datetime < '2025-12-01'
			and trip_distance < 100
		GROUP BY 
			pickup_day
		ORDER BY 
			max_distance desc

		/*Sum total amount and determine which zone was the highest.*/
		SELECT
			sum(total_amount) as zone_total,
			t2."Zone" as pickup_zone
		FROM
			green_taxi_trips as t1 
			join taxi_zone_data as t2 
			on t1."PULocationID" = t2."LocationID"
		WHERE
			cast(lpep_pickup_datetime AS DATE) = '2025-11-18'
		GROUP BY
			pickup_zone
		ORDER BY
			zone_total desc

		/*Drop off zone with largest tip*/
		SELECT
			max(t1.tip_amount) as max_tip,
			doff."Zone" as dropoff_zone
		FROM
			green_taxi_trips as t1 
			join taxi_zone_data as doff 
			on t1."DOLocationID" = doff."LocationID"
			join taxi_zone_data as pup
			on t1."PULocationID" = pup."LocationID"
		WHERE
			lpep_pickup_datetime >= '2025-11-01' AND lpep_pickup_datetime < '2025-12-01'
			and pup."Zone" = 'East Harlem North'
		GROUP BY
			dropoff_zone
		ORDER BY
			max_tip desc

SQL Code for homework questions week 2
		/*Select row count for yellow taxi data for all csv files in 2020.*/
		SELECT 
			count(1) 
		FROM 
			`dtc-de-zoomcamp-486023.dtcdezoomcamp486023_zoomcamp_dataset.yellow_tripdata` 
		WHERE 
			filename like '%2020%'

		/*Select row count for green taxi data for all csv files in 2020.*/
		SELECT 
			count(1) 
		FROM 
			`dtc-de-zoomcamp-486023.dtcdezoomcamp486023_zoomcamp_dataset.green_tripdata` 
		WHERE 
			filename like '%2020%'

		/*Select row count for yellow taxi data for march 2021 csv file.*/
		SELECT 
			count(1) 
		FROM 
			`dtc-de-zoomcamp-486023.dtcdezoomcamp486023_zoomcamp_dataset.yellow_tripdata` 
		WHERE 
			filename like '%2021-03%'