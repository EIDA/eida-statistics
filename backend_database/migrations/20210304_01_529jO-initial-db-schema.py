"""
Initial DB schema
"""

from yoyo import step

__depends__ = {}

steps = [
    step("""
    CREATE EXTENSION hll;
    """),
    step("""
    CREATE TABLE public.dataselect_stats (
    node_id integer,
    date date,
    network character varying(6),
    station character varying(5),
    location character varying(2),
    channel character varying(3),
    country character varying(2),
    bytes bigint,
    nb_reqs integer,
    nb_successful_reqs integer,
    nb_failed_reqs integer,
    clients public.hll(11,5),
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone)
    """),
    step("""
    CREATE TABLE public.nodes (
        id serial PRIMARY KEY,
        name text,
        contact text)
    """),
    step("""
    CREATE TABLE public.tokens (
    id serial PRIMARY KEY,
    node_id integer,
    value character varying(32),
    valid_from timestamp with time zone NOT NULL,
    valid_until timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now())
    """),
    step("""
    CREATE TABLE public.payloads (
    id serial PRIMARY KEY,
    node_id integer,
    hash bigint,
    generated_at timestamp with time zone,
    version character varying(32),
    first_stat_at  timestamp with time zone,
    last_stat_at  timestamp with time zone,
    created_at timestamp with time zone DEFAULT now())
    """),
    step("""
    ALTER TABLE ONLY public.dataselect_stats
    ADD CONSTRAINT fk_nodes FOREIGN KEY (node_id) REFERENCES public.nodes(id);
    """),
    step("""
    ALTER TABLE ONLY public.dataselect_stats
    ADD CONSTRAINT uniq_stat UNIQUE (node_id,date,network,station,location,channel,country);
    """),
    step("""
    ALTER TABLE ONLY public.tokens
    ADD CONSTRAINT fk_nodes FOREIGN KEY (node_id) REFERENCES public.nodes(id);
    """),
    step("""
    ALTER TABLE ONLY public.payloads
    ADD CONSTRAINT fk_nodes FOREIGN KEY (node_id) REFERENCES public.nodes(id);
    """)
]
