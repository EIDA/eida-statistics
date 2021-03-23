"""
Add coverage view
"""

from yoyo import step

__depends__ = {'20210315_01_FxTxo-add-created-at-and-updated-at-columns'}

steps = [
    step("CREATE OR REPLACE VIEW coverage AS SELECT a.name AS node, unnest(b.stats) AS stat_day FROM (SELECT nodes.id,nodes.name FROM nodes) a LEFT JOIN (SELECT node_id, array_agg(splitup) as stats FROM (select node_id, unnest(coverage) splitup FROM payloads ) splitup group by 1) b ON a.id=b.node_id;",
         "DROP VIEW coverage")
]
