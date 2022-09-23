# Dashboard data preparation pipeline

## Usage

### Full pipeline execution

```sh
python dev/src/dashprep/main.py data/stats.csv data/edges.csv
```

## Test data generation and test mode

```sh
python dev/scripts/generate_test_log.py data/test/probs.csv data/test/pages.json data/log.txt --seed 0 -k 1000
python dev/src/dashprep/main.py data/stats.csv data/edges.csv --test data/log.txt
```

## Results

### `stats_global`

Data for the top section of the dashboard.

### `edges`

Data for the graph section (node and edge labels, tooltips) of the dashboard.

Columns:

- **url_subdomain**: page name
- **source_type**: journey category
- **from_node**: source endpoint name
- **from_node_type**: source endpoint type
- **to_node**: target endpoint name
- **to_node_type**: target endpoint type
- **freq**: number of visitors passing through this edge
- **total_visitors**: total number of visitors

The scripts in directory `dev/scripts/sql_agg` can be used as a template for obtaining the specific numbers from this data set.
