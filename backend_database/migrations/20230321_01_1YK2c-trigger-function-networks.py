"""
Add trigger function that automatically inserts appropriate record at networks table when new stats are ingested
"""

from yoyo import step

__depends__ = {'20230314_02_G5l8F-insert-default-inverted-policy'}

steps = [
    step("""
    CREATE OR REPLACE FUNCTION insert_network_if_not_exists()
    RETURNS TRIGGER AS $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM public.networks WHERE node_id = NEW.node_id AND name = NEW.network) THEN
            INSERT INTO public.networks (node_id, name, inverted_policy) VALUES (NEW.node_id, NEW.network, '0');
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """),
    step("""
    CREATE TRIGGER dataselect_stats_trigger
    BEFORE INSERT ON public.dataselect_stats
    FOR EACH ROW
    EXECUTE FUNCTION insert_network_if_not_exists();
    """)
]
