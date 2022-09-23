-- SQLITE
-- incoming

drop view if exists nodes_in_degree;
create view nodes_in_degree as

with pre as (
select
	to_node as node,
	from_node_in_filter as flg_from_journey,
	sum(freq) as in_degree
from edges_filtered
where
	to_node_in_filter
group by
	to_node,
	from_node_in_filter
)
select
	node,
	sum(flg_from_journey * in_degree) as in_degree_from_journey,
	sum((1-flg_from_journey) * in_degree) as in_degree_from_other_journey
from pre
group by
	node
;
