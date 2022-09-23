import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Tuple
from zipfile import ZipFile, ZIP_DEFLATED

import pandas as pd


logger = logging.getLogger(Path(__file__).stem)


def find_parts(row: str) -> Tuple[str, str, str]:
    """Finds the main parts of the log row.

    Returns a tuple:
        0: timestamp and IP address
        1: data in json format
        2: user agent
    """

    # bnd = 10 + 1 + 6 + 2
    part_ts_ip = ' '.join(row.split(' ')[:3])

    resid = row[len(part_ts_ip) + 1:]
    level = 0
    cur = 0
    for cur, char in enumerate(resid):
        if char == '{':
            level += 1
        elif char == '}':
            level -= 1
        if level == 0:
            break

    part_data = resid[:cur + 1].strip()
    part_agent = resid[cur + 1:].strip()

    return part_ts_ip, part_data, part_agent


def process_parts(raw_ts_ip: str, raw_data: str, raw_agent: str) -> dict:
    """Processes the raw string data to record format (flat dict).
    """

    record = dict()

    # timestamp and IP
    date, time, ip = raw_ts_ip.split(' ')
    timestamp = datetime.strptime(date + time, '%Y-%m-%d%H:%M:%S')
    record['timestamp'] = timestamp
    record['ip'] = ip

    # data json
    if raw_data != '':
        d = json.loads(raw_data)
        if 'data' in d:
            d = {**d, **{k: v for k, v in d['data'].items()}}
            del d['data']
        if '0' in d:
            d = {**d, **{k: v for k, v in d['0'].items()}}
            del d['0']
        record = {**record, **d}

    # user agent
    record['user_agent'] = raw_agent

    return record


def _get_content_from_zip(zp: Path) -> str:
    """Utility function to get log txt from zip.
    """

    with ZipFile(zp, compression=ZIP_DEFLATED) as zf:
        content = zf.read('log.txt')
    return content


def convert_to_df(content: str) -> pd.DataFrame:
    """Transforms raw log to pandas DataFrame.
    """

    rows = content.splitlines(keepends=False)

    records = []
    for text in rows:
        row = text.strip()
        raw_ts_ip, raw_data, raw_agent = find_parts(row)
        record = process_parts(raw_ts_ip, raw_data, raw_agent)
        records.append(record)
    df = pd.DataFrame.from_records(records)

    df['id'] = df['id'].astype(str).str.split('.', expand=True)[0]

    return df


def extract_arguments_from_url(df: pd.DataFrame,
                               url_field: str) -> pd.DataFrame:
    """Extracts subdomain and GET request arguments
    from the dataframe's specified url field,
    and adds them to the dataframe with prefixes.
    """

    # subdomain
    try:
        df[f'{url_field}_subdomain'] = (
            df[url_field]
            .str.split('/', expand=True)[2]
            .str.replace(r'^.*?\.?([^\.]+)\.[^\.]+$', r'\1', regex=True)
        )
    except KeyError:
        pass

    # get request arguments
    try:
        args = (
            df[url_field]
            .str.split('?', expand=True)[1]
            .str.split('&').explode()
            .str.split('=', expand=True)
        )
        args = (
            args[args.notna().any(axis=1)].pivot(columns=0, values=1)
        )
        prefix = f'x_{url_field}_'
        args.columns = [(prefix + a) for a in args.columns]
        df = pd.merge(df, args, how='left', left_index=True, right_index=True)
    except KeyError:
        pass

    return df


def extract_device_type(df):
    """Identifies device type from user agent data,
    and appends it to the dataframe.
    """

    mobile = (df['user_agent']
              .str.upper()
              .str.contains('MOBILE|ANDROID|IPHONE', regex=True))
    device_types = mobile.apply(lambda x: 'Mobile' if x else 'Desktop')
    df['device'] = device_types
    return df


def extract_source_type(df):
    """Identifies the source type.
    """

    required_cols = [
        'url',
        'x_url_utm_medium',
        'x_url_utm_source',
        'x_referrer_utm_medium',
        'x_url_gclid',
        'x_url_fbclid',
        'referrer',
        'x_referrer_utm_source',
        'x_referrer_gclid',
        'x_referrer_fbclid',
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    paid_search = (
        0 == 1
        | (df['x_url_utm_medium'].str.upper() == 'CPC')
        | (df['x_referrer_utm_medium'].str.upper() == 'CPC')
    ).apply(lambda x: 'Paid Search' if x else None)

    google = (
        0 == 1
        | (df['referrer'].str.upper().str.contains('GOOGLE'))
        | (df['x_referrer_utm_source'].str.upper() == 'GOOGLE')
        | (df['x_referrer_gclid'].notna())
        | (df['url'].str.upper().str.contains('GOOGLE'))
        | (df['x_url_utm_source'].str.upper() == 'GOOGLE')
        | (df['x_url_gclid'].notna())
    )
    bing = (
        0 == 1
        | (df['referrer'].str.upper().str.contains('BING'))
        | (df['url'].str.upper().str.contains('BING'))
    )
    youtube = (
        0 == 1
        | (df['referrer'].str.upper().str.contains('YOUTUBE'))
        | (df['url'].str.upper().str.contains('YOUTUBE'))
    )
    organic_search = (google | bing | youtube).apply(
        lambda x: 'Organic Search' if x else None)

    facebook = (
        0 == 1
        | (df['referrer'].str.upper().str.contains('FACEBOOK'))
        | (df['x_referrer_utm_source'].str.upper() == 'FACEBOOK')
        | (df['x_referrer_fbclid'].notna())
        | (df['url'].str.upper().str.contains('FACEBOOK'))
        | (df['x_url_utm_source'].str.upper() == 'FACEBOOK')
        | (df['x_url_fbclid'].notna())
    )
    social_sites = (facebook).apply(lambda x: 'Social Sites' if x else None)

    df['source_type'] = (paid_search
                         .combine_first(organic_search)
                         .combine_first(social_sites)).fillna('Misc')

    return df


def drop_irrelevant_rows(df):
    """Drops test rows.
    """

    df = df[df.url.notna()
            & ~df.url.fillna('').str.contains('exsightech')
            ].copy()
    return df


def prepare(content: bytes) -> pd.DataFrame:
    """Main data preparation process.
    Transforms the raw event log to pandas DataFrame.
    """

    df = convert_to_df(content)
    df = extract_arguments_from_url(df, 'url')
    df = extract_arguments_from_url(df, 'referrer')
    df = drop_irrelevant_rows(df)
    df = extract_device_type(df)
    df = extract_source_type(df)
    df = df.drop([c for c in df.columns if c.startswith('x_')], axis=1)

    logger.info('Data preparation completed successfully.')

    return df


def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', action='store',
                        help='input zip file with log.txt content')
    parser.add_argument('output_file', action='store',
                        help='output csv file')
    args = parser.parse_args()

    input_file = Path(args.input_file)
    output_file = Path(args.output_file)

    content = _get_content_from_zip(input_file)
    content = content.decode('utf8')

    logger.debug(f'Reading from: {input_file.absolute()}')

    df = prepare(content)

    df.to_csv(output_file, index=False)
    logger.info(f'Data written to: {output_file.absolute()}')


if __name__ == '__main__':
    main()
