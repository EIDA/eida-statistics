"""
Add network table
Fill network table from dataselect_stats table
Add foreign keys from dataselect_stats table to network table
Add restriction_policy and eas_group at nodes table
"""

from yoyo import step

__depends__ = {'20210323_01_zdwVN-add-coverage-view'}

steps = [
    step("""
    CREATE TABLE public.networks (
    node_id integer,
    name character varying(6),
    inverted_policy boolean,
    eas_group character varying(20),
    PRIMARY KEY (node_id, name),
    CONSTRAINT fk_nodes
      FOREIGN KEY(node_id)
	  REFERENCES public.nodes(id));
    """),
    step("""
    INSERT INTO public.networks (node_id, name, inverted_policy)
    SELECT DISTINCT node_id, network
    FROM public.dataselect_stats;
    """),
    step("""
    ALTER TABLE ONLY public.dataselect_stats
    ADD CONSTRAINT fk_network FOREIGN KEY (node_id, network) REFERENCES public.networks(node_id, name);
    """),
    step("""
    ALTER TABLE public.nodes
    ADD COLUMN restriction_policy boolean,
    ADD COLUMN eas_group character varying(20)
    """)
]
