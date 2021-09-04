CREATE TABLE if not exists weather_fact (
	date_time timestamp NOT NULL,
	prcp numeric,
	snow numeric,
	snwd numeric,
	tavg numeric,
	tmax numeric,
	tmin numeric,
	CONSTRAINT weather_fact_pkey PRIMARY KEY (date_time)
);

CREATE TABLE if not exists date_with_weather_type (
	date_time timestamp NOT NULL,
	weather_type_id int4,
	CONSTRAINT date_with_weather_type_pkey PRIMARY KEY (date_time)
);

CREATE TABLE if not exists weather_type (
	weather_type_id int4 NOT NULL,
	description varchar(256),
	CONSTRAINT weather_type_pkey PRIMARY KEY (weather_type_id)
);

CREATE TABLE if not exists dim_station (
	station_id int4,
	name varchar(256),
	longitude numeric,
	latitude numeric,
	CONSTRAINT dim_station_pkey PRIMARY KEY (station_id)
);

CREATE TABLE if not exists trip_fact (
	trip_id numeric,
	duration int4,
	start_time timestamp,
    end_time timestamp,
    start_station_id int4,
    end_station_id int4,
    bikeid int4,
    usertype varchar,
    gender int4,
    birth_year int4,
    CONSTRAINT trip_fact_pkey PRIMARY KEY (trip_id)
);

CREATE TABLE if not exists dim_datetime (
	"DATE" timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	"week" int4,
	"month" int4,
	"weekday" int4,
	"year" int4,
	"quarter" int4,
	CONSTRAINT dim_datetime_pkey PRIMARY KEY (DATE)
);





