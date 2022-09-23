-- SQLITE
-- all node stats

drop view if exists nodes_stats;
create view nodes_stats as

with
total_visitors as (
	select max(total_visitors) as total_visitors from edges
),
pre as (
	select
		coalesce(i.node, o.node) as node,
		in_degree_from_journey,
		in_degree_from_other_journey,
		out_degree_to_journey,
		out_degree_to_other_journey,
		out_degree_abandoned,
		out_degree_progressed
	from nodes_in_degree i
	left join nodes_out_degree o on i.node = o.node
union 
	select
		coalesce(o.node, i.node) as node,
		in_degree_from_journey,
		in_degree_from_other_journey,
		out_degree_to_journey,
		out_degree_to_other_journey,
		out_degree_abandoned,
		out_degree_progressed
	from nodes_out_degree o
	left join nodes_in_degree i on o.node = i.node
	where i.node is null
)
select
	p.node,
	
	p.in_degree_from_journey,
	p.in_degree_from_other_journey,
	p.out_degree_to_journey,
	p.out_degree_to_other_journey,
	p.out_degree_abandoned,
	p.out_degree_progressed,
	
	p.in_degree_from_journey / v.total_visitors as in_degree_from_journey_pct,
	p.in_degree_from_other_journey / v.total_visitors as in_degree_from_other_journey_pct,
	p.out_degree_to_journey / v.total_visitors as out_degree_to_journey_pct,
	p.out_degree_to_other_journey / v.total_visitors as out_degree_to_other_journey_pct,
	p.out_degree_abandoned / v.total_visitors as out_degree_abandoned_pct,
	p.out_degree_progressed / v.total_visitors as out_degree_progressed_pct
from pre p
cross join total_visitors v
