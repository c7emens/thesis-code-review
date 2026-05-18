-- Auto-generated from live DB. Re-run _test_runner.sh to regenerate.

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS postgis CASCADE;

--
-- PostgreSQL database dump
--


-- Dumped from database version 16.11 (Ubuntu 16.11-1.pgdg22.04+1)
-- Dumped by pg_dump version 16.11 (Ubuntu 16.11-1.pgdg22.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: public; Type: SCHEMA; Schema: -; Owner: -
--



SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: vessel_data_ais; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.vessel_data_ais (
    mms_id text NOT NULL,
    "time" timestamp with time zone NOT NULL,
    latitude double precision,
    longitude double precision,
    location public.geography(Point,4326),
    speed_over_ground real,
    course_over_ground real,
    heading real,
    vessel_name text,
    imo_number text,
    radio_call_sign text,
    vessel_type smallint,
    navigation_status smallint,
    vessel_length real,
    vessel_width real,
    vessel_draft real,
    cargo_type_code smallint,
    ais_transceiver_class text
);


--
-- Name: weather_observations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.weather_observations (
    station_id text NOT NULL,
    "time" timestamp with time zone NOT NULL,
    latitude double precision,
    longitude double precision,
    location public.geography(Point,4326),
    elevation real,
    station_name text,
    imma_version smallint,
    attachment_count smallint,
    time_indicator smallint,
    latlon_indicator smallint,
    ship_course smallint,
    ship_speed smallint,
    national_source_indicator smallint,
    id_indicator smallint,
    country_code text,
    wind_direction_indicator smallint,
    wind_direction smallint,
    wind_speed_indicator smallint,
    wind_speed real,
    visibility_indicator smallint,
    visibility smallint,
    present_weather smallint,
    past_weather smallint,
    sea_level_pressure real,
    pressure_tendency_char smallint,
    pressure_tendency_amount smallint,
    temperature_indicator smallint,
    air_temp real,
    wet_bulb_indicator smallint,
    wet_bulb_temp real,
    dew_point_indicator smallint,
    dew_point_temp real,
    sst_measurement_method smallint,
    sea_surface_temp real,
    total_cloud_amount smallint,
    low_cloud_amount smallint,
    low_cloud_type smallint,
    height_indicator smallint,
    cloud_height smallint,
    mid_cloud_type smallint,
    high_cloud_type smallint,
    wave_period smallint,
    wave_height real,
    swell_direction smallint,
    swell_period smallint,
    swell_height real,
    marsden_square_10 smallint,
    marsden_square_1 smallint,
    deck_id smallint,
    source_id smallint,
    platform_id smallint,
    duplicate_status smallint,
    duplicate_check smallint,
    night_day_flag smallint,
    trim_flag text,
    ncdc_qc_flags text,
    external_flag smallint,
    source_exclusion_flag smallint,
    observation_source smallint,
    observation_platform smallint,
    fm_code_version smallint,
    station_weather_indicator smallint,
    past_weather_2 smallint,
    swell_2_direction smallint,
    swell_2_period smallint,
    swell_2_height real,
    precipitation_indicator smallint,
    qc_indicator smallint,
    qc_indicator_fields smallint,
    mqcs_version text
);


--
-- Name: gshhg_land; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.gshhg_land (
    id text,
    level integer,
    source text,
    parent_id integer,
    sibling_id integer,
    area double precision,
    geometry public.geometry(Polygon,4326)
);


--
-- Name: ne_land; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ne_land (
    featurecla text,
    scalerank double precision,
    min_zoom double precision,
    geometry public.geometry(Geometry,4326)
);


--
-- Name: osm_airports; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.osm_airports (
    osm_id bigint NOT NULL,
    osm_type text,
    latitude double precision NOT NULL,
    longitude double precision NOT NULL,
    name text,
    operator text,
    icao text,
    iata text,
    aeroway text,
    ele_m double precision,
    source text DEFAULT 'openstreetmap'::text NOT NULL,
    ingested_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: osm_land; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.osm_land (
    x integer,
    y integer,
    geometry public.geometry(Polygon,4326)
);


--
-- Name: osm_ports; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.osm_ports (
    osm_id bigint NOT NULL,
    osm_type text,
    latitude double precision NOT NULL,
    longitude double precision NOT NULL,
    name text,
    operator text,
    harbour text,
    seamark_type text,
    amenity text,
    port_type text,
    source text DEFAULT 'openstreetmap'::text NOT NULL,
    ingested_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: osm_wind_turbines; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.osm_wind_turbines (
    osm_id bigint NOT NULL,
    latitude double precision NOT NULL,
    longitude double precision NOT NULL,
    name text,
    ref text,
    operator text,
    manufacturer text,
    model text,
    output_kw double precision,
    hub_height_m double precision,
    rotor_diameter_m double precision,
    start_date text,
    location_tag text,
    is_offshore boolean DEFAULT false NOT NULL,
    source text DEFAULT 'openstreetmap'::text NOT NULL,
    ingested_at timestamp with time zone DEFAULT now() NOT NULL,
    is_offshore_ne boolean
);


--
-- Name: stage1_helicopter_chunks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stage1_helicopter_chunks (
    chunk_label text NOT NULL,
    year smallint NOT NULL,
    month smallint NOT NULL,
    start_date date NOT NULL,
    end_date date NOT NULL,
    n_hits integer NOT NULL,
    elapsed_s double precision NOT NULL,
    completed_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stage1_helicopter_hits; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stage1_helicopter_hits (
    icao24 text NOT NULL,
    flight_date date NOT NULL,
    n_positions integer NOT NULL,
    min_alt_m double precision,
    max_alt_m double precision,
    chunk_label text NOT NULL,
    fetched_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stage1_vessel_chunks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stage1_vessel_chunks (
    chunk_label text NOT NULL,
    year smallint NOT NULL,
    month smallint NOT NULL,
    start_date date NOT NULL,
    end_date date NOT NULL,
    n_hits integer NOT NULL,
    elapsed_s double precision NOT NULL,
    completed_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stage1_vessel_hits; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stage1_vessel_hits (
    mms_id text NOT NULL,
    vessel_date date NOT NULL,
    n_positions integer NOT NULL,
    min_speed double precision,
    max_speed double precision,
    vessel_name text,
    vessel_type smallint,
    chunk_label text NOT NULL,
    fetched_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stage2_helicopter_dates; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stage2_helicopter_dates (
    flight_date date NOT NULL,
    year smallint NOT NULL,
    n_icao24s integer NOT NULL,
    n_positions integer NOT NULL,
    elapsed_s real NOT NULL,
    fetched_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stage2_helicopter_tracks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stage2_helicopter_tracks (
    icao24 text NOT NULL,
    flight_date date NOT NULL,
    time_unix bigint NOT NULL,
    time_utc timestamp with time zone NOT NULL,
    lat double precision,
    lon double precision,
    baro_alt_m double precision,
    velocity_ms double precision,
    heading double precision,
    onground boolean
);


--
-- Name: stage2_vessel_dates; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stage2_vessel_dates (
    vessel_date date NOT NULL,
    year smallint NOT NULL,
    n_vessels integer NOT NULL,
    n_positions integer NOT NULL,
    elapsed_s real NOT NULL,
    fetched_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stage2_vessel_tracks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stage2_vessel_tracks (
    mms_id text NOT NULL,
    vessel_date date NOT NULL,
    time_utc timestamp with time zone NOT NULL,
    latitude double precision,
    longitude double precision,
    speed_over_ground double precision,
    course_over_ground double precision,
    heading smallint,
    vessel_name text,
    vessel_type smallint,
    navigation_status smallint
);


--
-- Name: stage3_helicopter_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stage3_helicopter_events (
    icao24 text NOT NULL,
    project_name text NOT NULL,
    turbine_code text NOT NULL,
    turbine_name text,
    visit_start timestamp with time zone NOT NULL,
    visit_end timestamp with time zone NOT NULL,
    duration_minutes real NOT NULL,
    n_positions integer NOT NULL,
    min_distance_m real,
    min_alt_m real,
    min_speed_ms real,
    max_speed_ms real,
    median_speed_ms real,
    score real,
    s_evidence real,
    s_duration real,
    departure_airport text,
    return_airport text,
    transit_out_min real,
    transit_back_min real,
    airport_distance_km real,
    departure_airport_inferred text,
    return_airport_inferred text,
    inference_method text,
    inference_confidence real,
    n_virtual_fixes integer DEFAULT 0
);


--
-- Name: stage3_helicopter_events_legacy; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stage3_helicopter_events_legacy (
    icao24 text,
    project_name text,
    turbine_code text,
    turbine_name text,
    visit_start timestamp with time zone,
    visit_end timestamp with time zone,
    duration_minutes real,
    n_positions integer,
    min_distance_m real,
    min_alt_m real,
    min_speed_ms real,
    max_speed_ms real,
    median_speed_ms real,
    score real,
    s_evidence real,
    s_duration real,
    departure_airport text,
    return_airport text,
    transit_out_min real,
    transit_back_min real,
    airport_distance_km real,
    departure_airport_inferred text,
    return_airport_inferred text,
    inference_method text,
    inference_confidence real,
    n_virtual_fixes integer
);


--
-- Name: stage3_helicopter_events_old_scores; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stage3_helicopter_events_old_scores (
    icao24 text,
    project_name text,
    turbine_code text,
    visit_start timestamp with time zone,
    score real,
    s_evidence real,
    s_duration real
);


--
-- Name: stage3_helicopter_events_pre_c1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stage3_helicopter_events_pre_c1 (
    icao24 text,
    project_name text,
    turbine_code text,
    turbine_name text,
    visit_start timestamp with time zone,
    visit_end timestamp with time zone,
    duration_minutes real,
    n_positions integer,
    min_distance_m real,
    min_alt_m real,
    min_speed_ms real,
    max_speed_ms real,
    median_speed_ms real,
    score real,
    s_evidence real,
    s_duration real,
    departure_airport text,
    return_airport text,
    transit_out_min real,
    transit_back_min real,
    airport_distance_km real,
    departure_airport_inferred text,
    return_airport_inferred text,
    inference_method text,
    inference_confidence real
);


--
-- Name: stage3_vessel_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stage3_vessel_events (
    id integer NOT NULL,
    mms_id text NOT NULL,
    vessel_name text,
    vessel_type smallint,
    project_name text NOT NULL,
    turbine_code text,
    turbine_name text,
    visit_start timestamp with time zone NOT NULL,
    visit_end timestamp with time zone NOT NULL,
    duration_minutes real NOT NULL,
    n_positions integer NOT NULL,
    min_distance_m real NOT NULL,
    mean_distance_m real NOT NULL,
    min_sog_kt real,
    median_sog_kt real,
    max_gap_minutes real NOT NULL,
    tier smallint NOT NULL,
    tier_reason text NOT NULL,
    vessel_category text,
    flag_duration boolean,
    flag_proximity boolean,
    flag_sog boolean,
    flag_continuity boolean,
    operation_type text,
    departure_port text,
    departure_time timestamp with time zone,
    transit_out_min real,
    return_port text,
    return_time timestamp with time zone,
    transit_back_min real,
    transit_dist_km real,
    flag_dwell boolean,
    flag_proximity_extended boolean,
    score real,
    s_evidence real,
    s_duration real,
    n_virtual_fixes integer DEFAULT 0
);


--
-- Name: stage3_vessel_events_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.stage3_vessel_events_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stage3_vessel_events_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.stage3_vessel_events_id_seq OWNED BY public.stage3_vessel_events.id;


--
-- Name: stage3_vessel_events_legacy; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stage3_vessel_events_legacy (
    id integer,
    mms_id text,
    vessel_name text,
    vessel_type smallint,
    project_name text,
    turbine_code text,
    turbine_name text,
    visit_start timestamp with time zone,
    visit_end timestamp with time zone,
    duration_minutes real,
    n_positions integer,
    min_distance_m real,
    mean_distance_m real,
    min_sog_kt real,
    median_sog_kt real,
    max_gap_minutes real,
    tier smallint,
    tier_reason text,
    vessel_category text,
    flag_duration boolean,
    flag_proximity boolean,
    flag_sog boolean,
    flag_continuity boolean,
    operation_type text,
    departure_port text,
    departure_time timestamp with time zone,
    transit_out_min real,
    return_port text,
    return_time timestamp with time zone,
    transit_back_min real,
    transit_dist_km real,
    flag_dwell boolean,
    flag_proximity_extended boolean,
    score real,
    s_evidence real,
    s_duration real,
    n_virtual_fixes integer
);


--
-- Name: stage3_vessel_scores; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stage3_vessel_scores (
    id integer NOT NULL,
    mms_id text NOT NULL,
    vessel_name text,
    vessel_type smallint,
    project_name text NOT NULL,
    turbine_code text NOT NULL,
    turbine_name text,
    visit_start timestamp with time zone NOT NULL,
    visit_end timestamp with time zone NOT NULL,
    score real NOT NULL,
    s_evidence real NOT NULL,
    s_continuity real NOT NULL,
    s_duration real NOT NULL,
    min_distance_m real NOT NULL,
    min_sog_kt real,
    max_gap_min real NOT NULL,
    duration_min real NOT NULL,
    n_positions integer NOT NULL
);


--
-- Name: stage3_vessel_scores_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.stage3_vessel_scores_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stage3_vessel_scores_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.stage3_vessel_scores_id_seq OWNED BY public.stage3_vessel_scores.id;


--
-- Name: stage4_sov_interactions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stage4_sov_interactions (
    sov_mmsi text NOT NULL,
    sov_name text,
    sov_lat double precision,
    sov_lon double precision,
    sov_station_start timestamp with time zone NOT NULL,
    sov_station_end timestamp with time zone NOT NULL,
    asset_type text NOT NULL,
    asset_id text NOT NULL,
    asset_name text,
    interaction_type text NOT NULL,
    interaction_start timestamp with time zone NOT NULL,
    interaction_end timestamp with time zone NOT NULL,
    duration_minutes real NOT NULL,
    min_distance_m real,
    min_alt_m real,
    min_sog_kt real,
    score real,
    project_name text
);


--
-- Name: vessel_overrides; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.vessel_overrides (
    mms_id text NOT NULL,
    vessel_category text NOT NULL,
    note text
);


--
-- Name: wind_turbines; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.wind_turbines (
    project_name text NOT NULL,
    turbine_code text NOT NULL,
    turbine_name text,
    latitude double precision,
    longitude double precision,
    location public.geography(Point,4326),
    height_meters real,
    timezone_name text,
    timezone_offset smallint
);


--
-- Name: stage3_vessel_events id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stage3_vessel_events ALTER COLUMN id SET DEFAULT nextval('public.stage3_vessel_events_id_seq'::regclass);


--
-- Name: stage3_vessel_scores id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stage3_vessel_scores ALTER COLUMN id SET DEFAULT nextval('public.stage3_vessel_scores_id_seq'::regclass);


--
-- Name: osm_airports osm_airports_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.osm_airports
    ADD CONSTRAINT osm_airports_pkey PRIMARY KEY (osm_id);


--
-- Name: osm_ports osm_ports_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.osm_ports
    ADD CONSTRAINT osm_ports_pkey PRIMARY KEY (osm_id);


--
-- Name: osm_wind_turbines osm_wind_turbines_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.osm_wind_turbines
    ADD CONSTRAINT osm_wind_turbines_pkey PRIMARY KEY (osm_id);


--
-- Name: stage1_helicopter_chunks stage1_chunks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stage1_helicopter_chunks
    ADD CONSTRAINT stage1_chunks_pkey PRIMARY KEY (chunk_label);


--
-- Name: stage1_helicopter_hits stage1_hits_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stage1_helicopter_hits
    ADD CONSTRAINT stage1_hits_pkey PRIMARY KEY (icao24, flight_date);


--
-- Name: stage2_helicopter_dates stage2_dates_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stage2_helicopter_dates
    ADD CONSTRAINT stage2_dates_pkey PRIMARY KEY (flight_date);


--
-- Name: stage2_helicopter_tracks stage2_tracks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stage2_helicopter_tracks
    ADD CONSTRAINT stage2_tracks_pkey PRIMARY KEY (icao24, time_unix);


--
-- Name: stage3_helicopter_events stage3_helicopter_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stage3_helicopter_events
    ADD CONSTRAINT stage3_helicopter_events_pkey PRIMARY KEY (icao24, turbine_code, visit_start);


--
-- Name: stage3_vessel_events stage3_vessel_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stage3_vessel_events
    ADD CONSTRAINT stage3_vessel_events_pkey PRIMARY KEY (id);


--
-- Name: stage3_vessel_scores stage3_vessel_scores_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stage3_vessel_scores
    ADD CONSTRAINT stage3_vessel_scores_pkey PRIMARY KEY (id);


--
-- Name: stage4_sov_interactions stage4_sov_interactions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stage4_sov_interactions
    ADD CONSTRAINT stage4_sov_interactions_pkey PRIMARY KEY (asset_type, asset_id, interaction_start, sov_mmsi);


--
-- Name: vessel_overrides vessel_overrides_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vessel_overrides
    ADD CONSTRAINT vessel_overrides_pkey PRIMARY KEY (mms_id);


--
-- Name: vessel_data_ais vessel_positions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vessel_data_ais
    ADD CONSTRAINT vessel_positions_pkey PRIMARY KEY (mms_id, "time");


--
-- Name: stage1_vessel_chunks vessel_stage1_chunks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stage1_vessel_chunks
    ADD CONSTRAINT vessel_stage1_chunks_pkey PRIMARY KEY (chunk_label);


--
-- Name: stage1_vessel_hits vessel_stage1_hits_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stage1_vessel_hits
    ADD CONSTRAINT vessel_stage1_hits_pkey PRIMARY KEY (mms_id, vessel_date);


--
-- Name: stage2_vessel_dates vessel_stage2_dates_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stage2_vessel_dates
    ADD CONSTRAINT vessel_stage2_dates_pkey PRIMARY KEY (vessel_date);


--
-- Name: stage2_vessel_tracks vessel_stage2_tracks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stage2_vessel_tracks
    ADD CONSTRAINT vessel_stage2_tracks_pkey PRIMARY KEY (mms_id, time_utc);


--
-- Name: weather_observations weather_observations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.weather_observations
    ADD CONSTRAINT weather_observations_pkey PRIMARY KEY (station_id, "time");


--
-- Name: wind_turbines wind_turbines_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wind_turbines
    ADD CONSTRAINT wind_turbines_pkey PRIMARY KEY (project_name, turbine_code);


--
-- Name: gshhg_land_geom_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX gshhg_land_geom_idx ON public.gshhg_land USING gist (geometry);


--
-- Name: idx_gshhg_land_geometry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_gshhg_land_geometry ON public.gshhg_land USING gist (geometry);


--
-- Name: idx_helicopter_stage1_hits_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_helicopter_stage1_hits_date ON public.stage1_helicopter_hits USING btree (flight_date, icao24);


--
-- Name: idx_helicopter_stage2_tracks_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_helicopter_stage2_tracks_date ON public.stage2_helicopter_tracks USING btree (flight_date, icao24);


--
-- Name: idx_ne_land_geometry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ne_land_geometry ON public.ne_land USING gist (geometry);


--
-- Name: idx_osm_airports_aeroway_latlon; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_osm_airports_aeroway_latlon ON public.osm_airports USING btree (aeroway, latitude, longitude);


--
-- Name: idx_osm_airports_icao; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_osm_airports_icao ON public.osm_airports USING btree (icao) WHERE (icao IS NOT NULL);


--
-- Name: idx_osm_land_geometry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_osm_land_geometry ON public.osm_land USING gist (geometry);


--
-- Name: idx_osm_ports_type_latlon; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_osm_ports_type_latlon ON public.osm_ports USING btree (port_type, latitude, longitude);


--
-- Name: idx_osm_turbines_offshore; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_osm_turbines_offshore ON public.osm_wind_turbines USING btree (is_offshore, latitude, longitude);


--
-- Name: idx_stage1_helicopter_hits_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stage1_helicopter_hits_date ON public.stage1_helicopter_hits USING btree (flight_date, icao24);


--
-- Name: idx_stage1_hits_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stage1_hits_date ON public.stage1_helicopter_hits USING btree (flight_date, icao24);


--
-- Name: idx_stage1_vessel_hits_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stage1_vessel_hits_date ON public.stage1_vessel_hits USING btree (vessel_date, mms_id);


--
-- Name: idx_stage2_helicopter_tracks_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stage2_helicopter_tracks_date ON public.stage2_helicopter_tracks USING btree (flight_date, icao24);


--
-- Name: idx_stage2_tracks_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stage2_tracks_date ON public.stage2_helicopter_tracks USING btree (flight_date, icao24);


--
-- Name: idx_stage2_vessel_tracks_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stage2_vessel_tracks_date ON public.stage2_vessel_tracks USING btree (vessel_date, mms_id);


--
-- Name: idx_turbines_location; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_turbines_location ON public.wind_turbines USING gist (location);


--
-- Name: idx_turbines_project; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_turbines_project ON public.wind_turbines USING btree (project_name);


--
-- Name: idx_vessel_positions_location; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_vessel_positions_location ON public.vessel_data_ais USING gist (location);


--
-- Name: idx_vessel_positions_mms_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_vessel_positions_mms_id ON public.vessel_data_ais USING btree (mms_id, "time" DESC);


--
-- Name: idx_vessel_stage1_hits_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_vessel_stage1_hits_date ON public.stage1_vessel_hits USING btree (vessel_date, mms_id);


--
-- Name: idx_vessel_stage2_tracks_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_vessel_stage2_tracks_date ON public.stage2_vessel_tracks USING btree (vessel_date, mms_id);


--
-- Name: idx_weather_location; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_weather_location ON public.weather_observations USING gist (location);


--
-- Name: idx_weather_station; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_weather_station ON public.weather_observations USING btree (station_id, "time" DESC);


--
-- Name: ne_land_geom_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ne_land_geom_idx ON public.ne_land USING gist (geometry);


--
-- Name: osm_land_geom_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX osm_land_geom_idx ON public.osm_land USING gist (geometry);


--
-- Name: stage3_vessel_events_upsert_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX stage3_vessel_events_upsert_idx ON public.stage3_vessel_events USING btree (mms_id, project_name, visit_start, COALESCE(turbine_code, ''::text));


--
-- Name: stage3_vessel_scores_upsert_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX stage3_vessel_scores_upsert_idx ON public.stage3_vessel_scores USING btree (mms_id, turbine_code, visit_start);


--
-- Name: vessel_positions_time_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX vessel_positions_time_idx ON public.vessel_data_ais USING btree ("time" DESC);


--
-- Name: weather_observations_local_time_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX weather_observations_local_time_idx ON public.weather_observations USING btree ("time" DESC);


--
-- PostgreSQL database dump complete
--


