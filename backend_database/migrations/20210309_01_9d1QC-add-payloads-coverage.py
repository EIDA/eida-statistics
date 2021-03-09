"""
Add payloads coverage
"""

from yoyo import step

__depends__ = {'20210304_02_hGk3U-add-payloads-constraints'}

steps = [
    step("ALTER TABLE payloads ADD column coverage date[]",
         "ALTER TABLE payloads DROP column coverage")
]
