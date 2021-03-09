"""
Add payloads constraints
"""

from yoyo import step

__depends__ = {'20210304_01_529jO-initial-db-schema'}

steps = [
    step("alter table payloads add constraint uniq_payload UNIQUE(hash)")
]
