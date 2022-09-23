-- SQLITE
-- filter top N (specified at the end)

drop view if exists edges_filtered;


create view edges_filtered as
with
top_n as (
	-- filter
	select 2 as n
),
pre_2 as (
	select
		url_subdomain,
		source_type,
		from_node as node,
		from_node_type as node_type,
		row_number() over (partition by from_node_type order by freq desc) as row_number,
		sum(freq) as freq
	from edges
	where (
		from_node not in ("START", "STOP", "CART")
		and to_node not in ("STOP")
	)
	group by
		url_subdomain,
		source_type,
		from_node,
		node_type
),
nodes_filtered as (
	select node
	from pre_2
	where row_number <= (select n from top_n)
)
select
	*,
	(from_node in ('START', 'CART', 'STOP') or from_node in (select node from nodes_filtered)) as from_node_in_filter,
	(to_node in ('START', 'CART', 'STOP') or to_node in (select node from nodes_filtered)) as to_node_in_filter
from edges
