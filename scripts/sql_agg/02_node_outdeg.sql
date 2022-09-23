-- SQLITE
-- outgoing

drop view if exists nodes_out_degree;
create view nodes_out_degree as

with pre as (
select
	from_node as node,
	to_node_in_filter as flg_to_journey,
	case when to_node = "STOP" then 1 else 0 end as flg_abandoned,
	sum(freq) as out_degree
from edges_filtered
where
	from_node_in_filter
group by
	from_node,
	to_node_in_filter,
	(case when to_node = "STOP" then 1 else 0 end)
)
select
	node,
	sum(flg_abandoned * out_degree) as out_degree_abandoned,
	sum((1-flg_abandoned) * out_degree) as out_degree_progressed,
	sum(flg_to_journey * out_degree) as out_degree_to_journey,
	sum((1-flg_to_journey) * out_degree) as out_degree_to_other_journey
from pre
group by
	node
;
