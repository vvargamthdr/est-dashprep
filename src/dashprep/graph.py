import argparse
import logging
from pathlib import Path

import pandas as pd


logger = logging.getLogger(Path(__file__).stem)

PHASE_INDEX_NM = 'index_view'
PHASE_CATEGORY_NM = 'category_view'
PHASE_PRODUCT_NM = 'product_view'
PHASE_CART_NM = 'product_in_cart'
PHASE_STOP_NM = 'STOP'

PHASE_ORDER = [
    PHASE_INDEX_NM,
    PHASE_CATEGORY_NM,
    PHASE_PRODUCT_NM,
    PHASE_CART_NM,
    PHASE_STOP_NM
]
PHASE_LEVEL_MAP = {name: i for (i, name) in enumerate(PHASE_ORDER)}


def identify_journeys(df: pd.DataFrame,
                      timeout_threshold: str = '2:00:00') -> pd.DataFrame:
    """Identify journeys by cart events and timeout.
    """

    timeout_threshold = pd.to_timedelta(timeout_threshold)

    journey_id_columns = ['ip', 'cid', 'device', 'url_subdomain']
    df['_jid_base'] = (
        df[journey_id_columns].apply('_'.join, axis=1)
        .astype('category').cat.codes.astype(str)
    )

    df = df.sort_values('timestamp', ignore_index=True)

    # cart event before -> new journey
    df['_flg_cart'] = (df['event'] == PHASE_CART_NM).astype(int)
    df['_flg_cart_before'] = (df
                              .groupby('_jid_base')['_flg_cart']
                              .shift().fillna(0).astype(int))

    # too much time elapsed -> new journey
    df['_diff_time'] = df.groupby('_jid_base')['timestamp'].diff()
    df['_flg_timeout_before'] = ((df['_diff_time'] > timeout_threshold)
                                 .astype(int))

    # compose journey id
    df['_flg_journey_start'] = (df['_flg_cart_before']
                                | df['_flg_timeout_before'])
    df['_jid_sub'] = (df.groupby('_jid_base')['_flg_journey_start']
                      .cumsum().astype(str))
    df['journey_id'] = (df['_jid_base'] + '_' + df['_jid_sub'])

    # correct time between steps in journey and sum
    df['_diff_time'] = df.groupby('journey_id')[
        'timestamp'].diff().dt.total_seconds().fillna(0)
    # print(df.groupby('journey_id')['_diff_time'].sum())
    df['total_time'] = df.groupby('journey_id')['_diff_time'].transform('sum')
    df['total_steps'] = df.groupby('journey_id')[
        'timestamp'].transform('count')

    # calculate cart event
    df['flg_cart_event'] = (
        df.groupby('journey_id')['_flg_cart'].transform('max'))

    # cleanup
    df = df.drop(
        [c for c in df.columns if c.startswith('x_') or c.startswith('_')],
        axis=1)

    return df


def identify_phases(df: pd.DataFrame,
                    drop_unknown: bool = True) -> pd.DataFrame:
    """Adds phase level number using event name
    and drops unknown events if required.
    """

    df['phase_level'] = df['event'].apply(PHASE_LEVEL_MAP.get)
    if drop_unknown:
        df = df[df['phase_level'].notna()]

    return df


def compress_to_phases(df: pd.DataFrame) -> pd.DataFrame:
    """Compresses event series to phase series.
    """

    phase_lvl_cart = PHASE_ORDER.index(PHASE_CART_NM)
    phase_lvl_stop = PHASE_ORDER.index(PHASE_STOP_NM)

    def _generate_stop_event(r: pd.Series) -> pd.Series:
        """Generates STOP event as a pandas Series.
        """

        stop_event = r.copy()
        stop_event['event'] = PHASE_STOP_NM
        stop_event['phase_level'] = phase_lvl_stop

        del stop_event['category']
        del stop_event['url']
        del stop_event['referrer']
        del stop_event['price']
        del stop_event['quantity']
        return stop_event

    records = []
    for __, _df in df.groupby('journey_id'):
        phases = [None, None, None, None, None]
        prev_phase_lvl = -1
        for i, r in _df.iterrows():
            phase_lvl = int(r['phase_level'])
            if prev_phase_lvl < phase_lvl:
                phases[phase_lvl] = r
                if phase_lvl == phase_lvl_cart:
                    break
            else:
                phases[phase_lvl] = r
                for i in range(phase_lvl + 1, len(phases)):
                    phases[i] = None
            prev_phase_lvl = phase_lvl
        phases[phase_lvl_stop] = _generate_stop_event(r)

        records += [p for p in phases if p is not None]
    df = pd.DataFrame.from_records(records)
    return df


def get_edges(df: pd.DataFrame) -> pd.DataFrame:

    def _get_node_name(row: pd.Series) -> str:
        if row['event'] == PHASE_INDEX_NM:
            return row['url']
        elif row['event'] == PHASE_PRODUCT_NM:
            return row['name']
        elif row['event'] == PHASE_CATEGORY_NM:
            return row['category']
        elif row['event'] == PHASE_CART_NM:
            return 'CART'
        elif row['event'] == PHASE_STOP_NM:
            return 'STOP'
        else:
            return None

    df = df.loc[:,
                ['journey_id', 'url_subdomain', 'event', 'phase_level', 'device', 'source_type', 'url', 'name', 'category',
                 'total_time', 'total_steps']].copy()
    df['to_node'] = df.apply(_get_node_name, axis=1)
    df['to_node_type'] = df['event']
    df['from_node'] = df.groupby('journey_id')[
        'to_node'].shift().fillna('START')
    df['from_node_type'] = df.groupby('journey_id')[
        'to_node_type'].shift().fillna('START')

    return df


def build_graph(df: pd.DataFrame) -> pd.DataFrame:
    df = identify_journeys(df)
    df = identify_phases(df)
    df = compress_to_phases(df)
    df = get_edges(df)

    logger.info(f'Graph was created successfully.')

    return df


def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='input csv file')
    parser.add_argument('output_file', help='output csv file')
    args = parser.parse_args()

    input_file = Path(args.input_file)
    logger.debug(f'Reading from: {input_file.absolute()}')
    df = pd.read_csv(input_file, dtype=str)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    df = build_graph(df)

    output_file = Path(args.output_file)
    df.to_csv(output_file, index=False)
    logger.info(f'Data written to: {output_file.absolute()}')


if __name__ == '__main__':
    main()
