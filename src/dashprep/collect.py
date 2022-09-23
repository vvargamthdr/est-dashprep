import argparse
import logging
from pathlib import Path
from urllib.request import urlopen
from urllib.error import URLError
from zipfile import ZipFile, ZIP_DEFLATED


logger = logging.getLogger(Path(__file__).stem)


def get_log(url: str) -> bytes:
    """Gets most recent log file content from the web.
    """

    content = None
    try:
        content = urlopen(url).read()
    except URLError as e:
        logger.error('Could not fetch log file. '
                     'Please check internet connection ({})'.format(e))
        raise

    return content


def save_to_file(content: bytes, zip_path: Path):
    """Saves raw log txt to the zip file path.
    """

    with ZipFile(zip_path, 'w', compression=ZIP_DEFLATED) as zf:
        zf.writestr('log.txt', content)


def collect() -> bytes:
    """Main collection process.
    Downloads data from url.
    """

    url = 'https://exsightech.com/l/log.txt'
    content = get_log(url)

    msg = ['Data collection completed successfully.']
    last_line = content.splitlines()[-1].decode('utf8')
    msg += [
        '\t> source: {}'.format(url),
        '\t> number of lines: {}'.format(content.count(b'\n')),
        '\t> last date: {}'.format(' '.join(last_line.split(' ')[0:2]))
    ]
    logger.info('\n'.join(msg))

    return content


def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('output_file', action='store',
                        help='target zip path')
    args = parser.parse_args()

    content = collect()

    output_file = Path(args.output_file)
    save_to_file(content, output_file)
    logger.info(f'Data written to: {output_file.absolute()}')


if __name__ == '__main__':
    main()
