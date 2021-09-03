CREATE TABLE if not exists weather_fact (
	date_time timestamp NOT NULL,
	prcp numeric,
	snow numeric,
	snwd numeric,
	tavg numeric,
	tmax numeric,
	tmin numeric
	CONSTRAINT weather_fact_pkey PRIMARY KEY (date_time)
);

CREATE TABLE if not exists date_with_weather_type (
	date_time timestamp NOT NULL,
	weather_type_id int4
	CONSTRAINT date_with_weather_type_pkey PRIMARY KEY (date_time)
);

CREATE TABLE if not exists weather_type (
	weather_type_id int4 NOT NULL,
	description varchar(256),
	CONSTRAINT weather_type_pkey PRIMARY KEY (weather_type_id)
);

CREATE TABLE if not exists staging_events (
	artist varchar(256),
	auth varchar(256),
	firstname varchar(256),
	gender varchar(256),
	iteminsession int4,
	lastname varchar(256),
	length numeric(18,0),
	"level" varchar(256),
	location varchar(256),
	"method" varchar(256),
	page varchar(256),
	registration numeric(18,0),
	sessionid int4,
	song varchar(256),
	status int4,
	ts int8,
	useragent varchar(256),
	userid int4
);

CREATE TABLE if not exists staging_songs (
	num_songs int4,
	artist_id varchar(256),
	artist_name varchar(256),
	artist_latitude numeric,
	artist_longitude numeric,
	artist_location varchar(256),
	song_id varchar(256),
	title varchar(256),
	duration numeric,
	"year" int4
);

CREATE TABLE if not exists "time" (
	start_time timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
);

CREATE TABLE if not exists users (
	userid int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	"level" varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
);





