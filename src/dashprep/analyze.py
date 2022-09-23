import argparse
import logging
from pathlib import Path

import pandas as pd


logger = logging.getLogger(Path(__file__).stem)


def get_global_stats(df: pd.DataFrame) -> pd.Series:
    """Calculates journey-level stats.
    """

    journeys = df.groupby(['journey_id']).agg(
        abandoned=('abandoned', 'max'),
        total_steps=('total_steps', 'max'),
        total_time=('total_time', 'max'),
    )

    d = pd.Series(dtype=float, name='value')
    d.index.name = 'name'

    d['visitors'] = len(journeys)

    d['browse_abandonment_n'] = journeys['abandoned'].sum()
    d['browse_abandonment_pct'] = d['browse_abandonment_n'] / d['visitors']
    d['avg_steps_abandonment'] = journeys.loc[journeys['abandoned'], 'total_steps'].mean()
    d['avg_time_abandonment'] = journeys.loc[journeys['abandoned'], 'total_time'].mean()

    d['cart_conversion_n'] = d['visitors'] - d['browse_abandonment_n']
    d['cart_conversion_pct'] = 1 - d['browse_abandonment_pct']
    d['avg_steps_cart_conversion'] = journeys.loc[~journeys['abandoned'],
                                                  'total_steps'].mean()
    d['avg_time_cart_conversion'] = journeys.loc[~journeys['abandoned'], 'total_time'].mean()

    d['sales_conversion_n'] = 0
    d['sales_conversion_pct'] = 0
    d['avg_steps_sales_conversion'] = None
    d['avg_time_sales_conversion'] = None

    d = d.to_frame()

    return d


def get_weighted_edges(df: pd.DataFrame) -> pd.DataFrame:
    """Compresses edges using node names.
    """

    edges = df.groupby(
        ['url_subdomain', 'source_type',
         'from_node', 'from_node_type',
         'to_node', 'to_node_type'],
        dropna=False)['freq'].sum().reset_index()

    return edges


def analyze(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates global stats and weighted edges.
    """

    df['abandoned'] = (df['to_node'] == 'STOP') & (df['from_node'] != 'CART')
    df['freq'] = 1

    stats_global = get_global_stats(df)
    edges = get_weighted_edges(df)
    edges['total_visitors'] = stats_global.loc['visitors', 'value']
    stats_global = stats_global.reset_index()

    # TODO: prediction

    logger.info('Analysis completed successfully.')

    return stats_global, edges


def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', action='store',
                        help='input csv file')
    parser.add_argument('output_file_stats', action='store',
                        help='output stats csv file')
    parser.add_argument('output_file_edges', action='store',
                        help='output edges csv file')
    args = parser.parse_args()

    input_file = Path(args.input_file)
    output_file_stats = Path(args.output_file_stats)
    output_file_edges = Path(args.output_file_edges)

    content = pd.read_csv(input_file)
    logger.debug(f'Reading from: {input_file.absolute()}')

    stats_global, edges = analyze(content)

    stats_global.to_csv(output_file_stats, index=False)
    edges.to_csv(output_file_edges, index=False)

    logger.info(
        f'Data written to: {output_file_stats.absolute()} and {output_file_edges.absolute()}')


if __name__ == '__main__':
    main()
