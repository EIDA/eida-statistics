"""
Insert default inverted_policy
"""

from yoyo import step

__depends__ = {'20230314_01_ldczn-add-network-table'}

steps = [
    step("UPDATE public.networks SET inverted_policy = '0';"),
    step("UPDATE public.nodes SET restriction_policy = '0';")
]
