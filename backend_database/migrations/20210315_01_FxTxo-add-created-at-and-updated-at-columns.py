"""
Add created_at and updated_at columns
"""

from yoyo import step

__depends__ = {'20210309_01_9d1QC-add-payloads-coverage'}

steps = [
    step("alter table nodes add column created_at timestamp with time zone default now()",
         "alter table nodes drop column created_at"),
    step("alter table nodes add column updated_at timestamp with time zone default now()",
         "alter table nodes drop column updated_at"),
    step("alter table tokens add column updated_at timestamp with time zone default now()",
         "alter table tokens drop column updated_at"),
    step("alter table tokens alter column created_at set default now()")
]
