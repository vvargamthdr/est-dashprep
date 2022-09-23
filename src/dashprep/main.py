import argparse
import logging
from pathlib import Path

from dashprep.analyze import analyze
from dashprep.collect import collect
from dashprep.graph import build_graph
from dashprep.prepare import prepare


logger = logging.getLogger(Path(__file__).stem)


def main():
    logging.basicConfig(level=logging.INFO)

    logger.info(f'Program started.')

    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store', type=str,
                        help='test log file path')
    parser.add_argument('output_file_stats',
                        help='output stats csv file')
    parser.add_argument('output_file_edges',
                        help='output edges csv file')
    args = parser.parse_args()

    output_file_stats = Path(args.output_file_stats)
    output_file_edges = Path(args.output_file_edges)

    if args.test is not None:
        input_file = Path(args.test)
        logger.info(f'Test mode: reading from: {input_file.absolute()}')
        with open(input_file) as f:
            content = f.read()
    else:
        content = collect()

    df = prepare(content)
    df = build_graph(df)
    stats_global, edges = analyze(df)

    stats_global.to_csv(output_file_stats, index=False)
    edges.to_csv(output_file_edges, index=False)

    logger.info(
        f'Data written to: {output_file_stats.absolute()} and {output_file_edges.absolute()}')


if __name__ == '__main__':
    main()
