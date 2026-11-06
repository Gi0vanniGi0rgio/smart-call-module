--
-- PostgreSQL database dump
--

\restrict nGvI26Y1z8UeOJeU2qmOjjEb4SHLcDDjqw1fsddAnOP7HrKumrt87W9h1X3QxZt

-- Dumped from database version 16.15 (Debian 16.15-1.pgdg12+2)
-- Dumped by pg_dump version 16.15 (Debian 16.15-1.pgdg12+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: voice_integration; Type: SCHEMA; Schema: -; Owner: voice_migrator
--

CREATE SCHEMA voice_integration;


ALTER SCHEMA voice_integration OWNER TO voice_migrator;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: call_events; Type: TABLE; Schema: voice_integration; Owner: voice_migrator
--

CREATE TABLE voice_integration.call_events (
    id bigint NOT NULL,
    idempotency_key character varying(256) NOT NULL,
    session_id character varying(128) NOT NULL,
    attempt_id character varying(128) NOT NULL,
    event_name character varying(64) NOT NULL,
    details jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE voice_integration.call_events OWNER TO voice_migrator;

--
-- Name: call_events_id_seq; Type: SEQUENCE; Schema: voice_integration; Owner: voice_migrator
--

CREATE SEQUENCE voice_integration.call_events_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE voice_integration.call_events_id_seq OWNER TO voice_migrator;

--
-- Name: call_events_id_seq; Type: SEQUENCE OWNED BY; Schema: voice_integration; Owner: voice_migrator
--

ALTER SEQUENCE voice_integration.call_events_id_seq OWNED BY voice_integration.call_events.id;


--
-- Name: call_sessions; Type: TABLE; Schema: voice_integration; Owner: voice_migrator
--

CREATE TABLE voice_integration.call_sessions (
    session_id character varying(128) NOT NULL,
    attempt_id character varying(128) NOT NULL,
    direction character varying(16) NOT NULL,
    state character varying(32) NOT NULL,
    room_name character varying(255) NOT NULL,
    caller_number character varying(32),
    called_number character varying(32),
    purpose character varying(500),
    scenario_id character varying(128) NOT NULL,
    customer_id character varying(128),
    customer_name character varying(256),
    backend_registered boolean DEFAULT false NOT NULL,
    blind_mode boolean DEFAULT false NOT NULL,
    backend_context jsonb DEFAULT '{}'::jsonb NOT NULL,
    result_code character varying(32),
    result_note character varying(500),
    final_status character varying(32),
    completion_event_id character varying(128),
    transcript jsonb,
    structured_data jsonb,
    duration_seconds integer,
    recording_bucket character varying(63),
    recording_object_key character varying(1024),
    recording_content_type character varying(64),
    recording_size_bytes bigint,
    recording_sha256 character(64),
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT call_sessions_attempt_id_check CHECK (((attempt_id)::text ~ '^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$'::text)),
    CONSTRAINT call_sessions_direction_check CHECK (((direction)::text = ANY ((ARRAY['inbound'::character varying, 'outbound'::character varying])::text[]))),
    CONSTRAINT call_sessions_duration_seconds_check CHECK (((duration_seconds IS NULL) OR ((duration_seconds >= 0) AND (duration_seconds <= 7200)))),
    CONSTRAINT call_sessions_final_status_check CHECK (((final_status IS NULL) OR ((final_status)::text = ANY ((ARRAY['success'::character varying, 'callback_required'::character varying, 'rejected'::character varying, 'no_answer'::character varying])::text[])))),
    CONSTRAINT call_sessions_result_code_check CHECK (((result_code IS NULL) OR ((result_code)::text = ANY ((ARRAY['success'::character varying, 'callback'::character varying, 'refused'::character varying, 'wrong_number'::character varying, 'unresolved'::character varying])::text[])))),
    CONSTRAINT call_sessions_session_id_check CHECK (((session_id)::text ~ '^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$'::text))
);


ALTER TABLE voice_integration.call_sessions OWNER TO voice_migrator;

--
-- Name: delivery_outbox; Type: TABLE; Schema: voice_integration; Owner: voice_migrator
--

CREATE TABLE voice_integration.delivery_outbox (
    id bigint NOT NULL,
    event_id character varying(128) NOT NULL,
    session_id character varying(128) NOT NULL,
    state character varying(16) DEFAULT 'pending'::character varying NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    locked_at timestamp with time zone,
    last_error character varying(500),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    delivered_at timestamp with time zone,
    CONSTRAINT delivery_outbox_attempts_check CHECK (((attempts >= 0) AND (attempts <= 100))),
    CONSTRAINT delivery_outbox_state_check CHECK (((state)::text = ANY ((ARRAY['pending'::character varying, 'processing'::character varying, 'delivered'::character varying, 'failed'::character varying])::text[])))
);


ALTER TABLE voice_integration.delivery_outbox OWNER TO voice_migrator;

--
-- Name: delivery_outbox_id_seq; Type: SEQUENCE; Schema: voice_integration; Owner: voice_migrator
--

CREATE SEQUENCE voice_integration.delivery_outbox_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE voice_integration.delivery_outbox_id_seq OWNER TO voice_migrator;

--
-- Name: delivery_outbox_id_seq; Type: SEQUENCE OWNED BY; Schema: voice_integration; Owner: voice_migrator
--

ALTER SEQUENCE voice_integration.delivery_outbox_id_seq OWNED BY voice_integration.delivery_outbox.id;


--
-- Name: idempotency_records; Type: TABLE; Schema: voice_integration; Owner: voice_migrator
--

CREATE TABLE voice_integration.idempotency_records (
    scope character varying(64) NOT NULL,
    idempotency_key character varying(256) NOT NULL,
    request_hash character(64) NOT NULL,
    state character varying(16) NOT NULL,
    owner_token character(32) NOT NULL,
    response_status integer,
    response_body jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT idempotency_records_state_check CHECK (((state)::text = ANY ((ARRAY['processing'::character varying, 'completed'::character varying])::text[])))
);


ALTER TABLE voice_integration.idempotency_records OWNER TO voice_migrator;

--
-- Name: schema_migrations; Type: TABLE; Schema: voice_integration; Owner: voice_migrator
--

CREATE TABLE voice_integration.schema_migrations (
    version character varying(255) NOT NULL,
    sha256 character(64) NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE voice_integration.schema_migrations OWNER TO voice_migrator;

--
-- Name: voice_panel_call_status; Type: VIEW; Schema: voice_integration; Owner: voice_migrator
--

CREATE VIEW voice_integration.voice_panel_call_status AS
 SELECT session_id,
    attempt_id,
    state,
    room_name,
    started_at,
    finished_at,
    duration_seconds,
    final_status,
    result_code,
    structured_data
   FROM voice_integration.call_sessions
  WHERE ((session_id)::text ~~ 'web-%'::text);


ALTER VIEW voice_integration.voice_panel_call_status OWNER TO voice_migrator;

--
-- Name: call_events id; Type: DEFAULT; Schema: voice_integration; Owner: voice_migrator
--

ALTER TABLE ONLY voice_integration.call_events ALTER COLUMN id SET DEFAULT nextval('voice_integration.call_events_id_seq'::regclass);


--
-- Name: delivery_outbox id; Type: DEFAULT; Schema: voice_integration; Owner: voice_migrator
--

ALTER TABLE ONLY voice_integration.delivery_outbox ALTER COLUMN id SET DEFAULT nextval('voice_integration.delivery_outbox_id_seq'::regclass);


--
-- Name: call_events call_events_idempotency_key_key; Type: CONSTRAINT; Schema: voice_integration; Owner: voice_migrator
--

ALTER TABLE ONLY voice_integration.call_events
    ADD CONSTRAINT call_events_idempotency_key_key UNIQUE (idempotency_key);


--
-- Name: call_events call_events_pkey; Type: CONSTRAINT; Schema: voice_integration; Owner: voice_migrator
--

ALTER TABLE ONLY voice_integration.call_events
    ADD CONSTRAINT call_events_pkey PRIMARY KEY (id);


--
-- Name: call_sessions call_sessions_completion_event_id_key; Type: CONSTRAINT; Schema: voice_integration; Owner: voice_migrator
--

ALTER TABLE ONLY voice_integration.call_sessions
    ADD CONSTRAINT call_sessions_completion_event_id_key UNIQUE (completion_event_id);


--
-- Name: call_sessions call_sessions_pkey; Type: CONSTRAINT; Schema: voice_integration; Owner: voice_migrator
--

ALTER TABLE ONLY voice_integration.call_sessions
    ADD CONSTRAINT call_sessions_pkey PRIMARY KEY (session_id);


--
-- Name: delivery_outbox delivery_outbox_event_id_key; Type: CONSTRAINT; Schema: voice_integration; Owner: voice_migrator
--

ALTER TABLE ONLY voice_integration.delivery_outbox
    ADD CONSTRAINT delivery_outbox_event_id_key UNIQUE (event_id);


--
-- Name: delivery_outbox delivery_outbox_pkey; Type: CONSTRAINT; Schema: voice_integration; Owner: voice_migrator
--

ALTER TABLE ONLY voice_integration.delivery_outbox
    ADD CONSTRAINT delivery_outbox_pkey PRIMARY KEY (id);


--
-- Name: idempotency_records idempotency_records_pkey; Type: CONSTRAINT; Schema: voice_integration; Owner: voice_migrator
--

ALTER TABLE ONLY voice_integration.idempotency_records
    ADD CONSTRAINT idempotency_records_pkey PRIMARY KEY (scope, idempotency_key);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: voice_integration; Owner: voice_migrator
--

ALTER TABLE ONLY voice_integration.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: call_events_session_idx; Type: INDEX; Schema: voice_integration; Owner: voice_migrator
--

CREATE INDEX call_events_session_idx ON voice_integration.call_events USING btree (session_id, created_at);


--
-- Name: call_sessions_finished_idx; Type: INDEX; Schema: voice_integration; Owner: voice_migrator
--

CREATE INDEX call_sessions_finished_idx ON voice_integration.call_sessions USING btree (finished_at) WHERE (finished_at IS NOT NULL);


--
-- Name: call_sessions_state_idx; Type: INDEX; Schema: voice_integration; Owner: voice_migrator
--

CREATE INDEX call_sessions_state_idx ON voice_integration.call_sessions USING btree (state, updated_at);


--
-- Name: delivery_outbox_ready_idx; Type: INDEX; Schema: voice_integration; Owner: voice_migrator
--

CREATE INDEX delivery_outbox_ready_idx ON voice_integration.delivery_outbox USING btree (state, next_attempt_at, id) WHERE ((state)::text = ANY ((ARRAY['pending'::character varying, 'processing'::character varying])::text[]));


--
-- Name: idempotency_updated_idx; Type: INDEX; Schema: voice_integration; Owner: voice_migrator
--

CREATE INDEX idempotency_updated_idx ON voice_integration.idempotency_records USING btree (updated_at);


--
-- Name: call_events call_events_session_id_fkey; Type: FK CONSTRAINT; Schema: voice_integration; Owner: voice_migrator
--

ALTER TABLE ONLY voice_integration.call_events
    ADD CONSTRAINT call_events_session_id_fkey FOREIGN KEY (session_id) REFERENCES voice_integration.call_sessions(session_id);


--
-- Name: delivery_outbox delivery_outbox_session_id_fkey; Type: FK CONSTRAINT; Schema: voice_integration; Owner: voice_migrator
--

ALTER TABLE ONLY voice_integration.delivery_outbox
    ADD CONSTRAINT delivery_outbox_session_id_fkey FOREIGN KEY (session_id) REFERENCES voice_integration.call_sessions(session_id);


--
-- Name: SCHEMA voice_integration; Type: ACL; Schema: -; Owner: voice_migrator
--

GRANT USAGE ON SCHEMA voice_integration TO voice_api;
GRANT USAGE ON SCHEMA voice_integration TO voice_panel_reader;


--
-- Name: TABLE call_events; Type: ACL; Schema: voice_integration; Owner: voice_migrator
--

GRANT SELECT,INSERT ON TABLE voice_integration.call_events TO voice_api;


--
-- Name: SEQUENCE call_events_id_seq; Type: ACL; Schema: voice_integration; Owner: voice_migrator
--

GRANT SELECT,USAGE ON SEQUENCE voice_integration.call_events_id_seq TO voice_api;


--
-- Name: TABLE call_sessions; Type: ACL; Schema: voice_integration; Owner: voice_migrator
--

GRANT SELECT,INSERT,UPDATE ON TABLE voice_integration.call_sessions TO voice_api;


--
-- Name: TABLE delivery_outbox; Type: ACL; Schema: voice_integration; Owner: voice_migrator
--

GRANT SELECT,INSERT,UPDATE ON TABLE voice_integration.delivery_outbox TO voice_api;


--
-- Name: SEQUENCE delivery_outbox_id_seq; Type: ACL; Schema: voice_integration; Owner: voice_migrator
--

GRANT SELECT,USAGE ON SEQUENCE voice_integration.delivery_outbox_id_seq TO voice_api;


--
-- Name: TABLE idempotency_records; Type: ACL; Schema: voice_integration; Owner: voice_migrator
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE voice_integration.idempotency_records TO voice_api;


--
-- Name: TABLE voice_panel_call_status; Type: ACL; Schema: voice_integration; Owner: voice_migrator
--

GRANT SELECT ON TABLE voice_integration.voice_panel_call_status TO voice_panel_reader;


--
-- PostgreSQL database dump complete
--

\unrestrict nGvI26Y1z8UeOJeU2qmOjjEb4SHLcDDjqw1fsddAnOP7HrKumrt87W9h1X3QxZt

