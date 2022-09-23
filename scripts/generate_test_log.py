import argparse
import json
import random
from datetime import datetime
from zipfile import ZipFile, ZIP_DEFLATED

import pandas as pd
from tqdm import tqdm


def get_possible_next_state_types(current_state_type: str,
                                  probs: pd.DataFrame) -> tuple:
    """Get a list of possible next state types
    with probabilities.
    """

    key = current_state_type.split('_')[0]
    S = probs.loc[key, 'target'].tolist()
    W = probs.loc[key, 'p'].tolist()
    return S, W


def generate_path_template(probs: pd.DataFrame, max_length: int = 20) -> list:
    """Generates a path template with event types using probabilities.
    Maximum length can be specified if required.
    """

    path = []
    length = 0
    current_state_type = 'START'
    while current_state_type != 'STOP' and length <= max_length:
        path.append(current_state_type)
        length += 1
        S, W = get_possible_next_state_types(current_state_type, probs)
        current_state_type = random.choices(S, weights=W).pop()

    return path


def generate_specific_path(path_template: list,
                           pages: dict,
                           products: list,
                           categories: list) -> list:
    """Generate specific path
    from template using possible values and topology
    as a list of (type, value) tuples.
    """

    path = ['START']
    previous_state = 'START'
    for state_type in path_template[1:]:
        if state_type == 'landing':
            current_state = random.choice(pages['landing'])
        elif state_type == 'product':
            current_state = random.choice(products)
        elif state_type == 'category':
            current_state = random.choice(categories)
        elif state_type == 'category_other':
            current_state = random.choice(
                [c for c in categories if c != previous_state])
        elif state_type == 'product_other':
            current_state = random.choice(
                [p for p in products if p != previous_state])
        elif state_type == 'product_in_category':
            current_state = random.choice(
                pages['topology'][previous_state])
        elif state_type == 'product_in_other_category':
            current_state = random.choice(
                [p for (c, l) in pages['topology'].items()
                    if c != previous_state for p in l])
        else:
            current_state = state_type
        path.append(current_state)
        previous_state = current_state
    return list(zip(path_template, path))


def generate_paths(k: int, probs: pd.DataFrame, pages: pd.DataFrame) -> list:
    """Generate `k` number of paths (event series)
    as a list of lists.
    """

    path_templates = []
    print('getting path templates...')
    for __ in tqdm(range(k)):
        path_template = generate_path_template(probs, max_length=20)
        path_templates.append(path_template)
    print('>> done')

    print('getting paths...')
    all_products = [p for l in pages['topology'].values() for p in l]
    all_categories = [c for c in pages['topology'].keys()]
    paths = []
    for path_template in tqdm(path_templates):
        path = generate_specific_path(
            path_template, pages, all_products, all_categories
        )
        paths.append(path)
    print('>> done')
    return paths


def dump_path_to_log_entries(path, i, user_agents) -> list:
    """Converts data to log entries (list of strings).
    """

    records = []
    ip = '{}.{}.{}.{}'.format(i, i, i, i)
    user_agent = random.choice(user_agents)

    cid = str(i)
    record = dict()
    url_stub = 'http://example.com/{}/{}'
    for j, (state_type, step) in enumerate(path):
        ts = datetime.fromtimestamp(float(i + j) + 100)
        state_type = state_type.split('_')[0]
        record['cid'] = cid
        record['id'] = 'TEST'

        data = dict()
        if state_type == 'START':
            continue
        if state_type == 'landing':
            data['event'] = 'index_view'
        elif state_type == 'category':
            data['event'] = 'category_view'
            data['category'] = step
        elif state_type == 'product':
            data['event'] = 'product_view'
            data['name'] = step
            data['price'] = 0
            data['quantity'] = 0
        elif state_type == 'cart':
            data['event'] = 'product_in_cart'

        data['url'] = url_stub.format(
            state_type, step.lower().replace(' ', '-')
        )
        data['id'] = '{}_{}'.format(cid, str(j))
        data['referrer'] = ''
        record['data'] = data

        record_str = "{ts} {ip} {data} {user_agent}".format(
            ts=ts.strftime('%Y-%m-%d %H:%M:%S'),
            ip=ip,
            data=json.dumps(record, ensure_ascii=False),
            user_agent=user_agent
        )
        records.append(record_str)
    return records


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('probs_path', help='probabilities csv file')
    parser.add_argument('pages_path', help='pages json file')
    parser.add_argument('output_path', help='output zip file')
    parser.add_argument('--seed', action='store', type=int, help='seed')
    parser.add_argument('-k', action='store', type=int,
                        help='number of paths (event series) to generate')
    parser.add_argument('--zip', action='store_true', default=False,
                        help='compress result to zip file')
    args = parser.parse_args()

    PROBS_NEXT_STATE = pd.read_csv(args.probs_path).set_index('source')
    with open(args.pages_path) as f:
        PAGES = json.load(f)

    seed = args.seed
    if seed is None:
        seed = 0
    random.seed(seed)

    K = args.k
    if K is None:
        K = 100
    paths = generate_paths(K, PROBS_NEXT_STATE, PAGES)

    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:104.0) Gecko/20100101 Firefox/104.0',
        'Mozilla/5.0 (Linux; Android 11; SM-A715F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Mobile Safari/537.36',
        'Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.115 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
    ]

    print('dumping records to text...')
    records = []
    for i, path in tqdm(enumerate(paths)):
        path_records = dump_path_to_log_entries(path, i, user_agents)
        records.extend(path_records)
    print('>> done ({} records)'.format(len(records)))

    print(args.zip)
    if args.zip:
        with ZipFile(args.output_path, 'w', compression=ZIP_DEFLATED) as zf:
            zf.writestr('log.txt', '\n'.join(records))
    else:
        with open(args.output_path, 'w') as f:
            f.write('\n'.join(records))


if __name__ == '__main__':
    main()
