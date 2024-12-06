import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg as pg

BULK_WORKERS = 4  # Number of threads to use - adjust as needed


def connect() -> pg.Connection:
    '''
    Returns a connection to the tenant database

    Returns:
        psycopg.Connection: The connection object
    '''
    return pg.connect(
        host=os.getenv('YOUR_HOST'),
        port=os.getenv('YOUR_PORT'),
        dbname=os.getenv('YOUR_TENANT'),
        user=os.getenv('YOUR_USERNAME'),
        password=os.getenv('YOUR_PASSWORD'),
        # I work with gigs at a pop, so I need to keep the connection alive
        keepalives=1,
        keepalives_idle=5,
        keepalives_interval=2,
        keepalives_count=2
    )


def bulk_publish(data: dict, table: str) -> None:
    '''
    Set up the threading and the SQL insert statement with a progress bar.

    Args:
        data (dict): data to insert formatted as a dictionary of lists
        table (str): table name
    '''
    print(f'Publishing {table}')
    chunks = chunk_data(data)
    total_chunks = len(chunks)
    with ThreadPoolExecutor(max_workers=BULK_WORKERS) as executor:
        futures = {executor.submit(insert, chunk, table) for chunk in chunks}
        for i, future in enumerate(as_completed(futures), start=1):
            try:
                future.result()
            except Exception as e:
                print(f'Error publishing > {str(e)}')
                raise RuntimeError(f'Error publishing > {str(e)}') from e
            progress_bar(i, total_chunks)
    print('\nFinished publishing')


def progress_bar(i, ttl, bar_len=50) -> None:
    '''
    Stupid simple progress bar

    Args:
        i (int): current iteration
        ttl (int): total iterations
        bar_len (int, optional): length of the progress bar. Defaults to 50.
    '''
    assert i != 0, 'i cannot start with 0'
    percent = 100 * i / float(ttl)
    pct = int(bar_len * percent // 100)
    print(f'\r[{'#' * pct + '-' * (bar_len - pct)}] {percent:.2f}%', end='')


def chunk_data(data: dict, chunk_size: int = 50000) -> list[dict]:
    '''
    Chunk the data into smaller pieces

    Args:
        data (dict): data to insert formatted as a dictionary of lists
        chunk_size (int): size of the chunks

    Returns:
        list[dict]: list of dictionaries of the chunked data
    '''
    try:
        keys = list(data.keys())
        values = list(data.values())
        chunked_data = []
        for i in range(0, len(values[0]), chunk_size):
            chunk_keys = keys
            chunk_values = [v[i:i + chunk_size] for v in values]
            chunked_data.append(dict(zip(chunk_keys, chunk_values)))
        return chunked_data
    except Exception as e:
        print(f'Error chunking data > {str(e)}')
        raise RuntimeWarning from e


def insert(chunk: dict, table: str) -> None:
    '''
    Execute the copy statement

    Args:
        chunk (dict): list of dictionaries of the chunked data
        table (str): table name
    '''
    def cln(x) -> str:
        return '' if x is None else comp.sub('', str(x).replace('"', '""').strip()).lower()

    comp = re.compile(r"^\s*(''|nan|N/A|NULL|NAT)\s*$", re.IGNORECASE)
    val = '\n'.join('|'.join(comp.sub('', cln(col)).replace('\n', '').replace('\r', '')
                             for col in row) for row in zip(*chunk.values()))
    query = (f'''
        COPY rxmedimate.{table} ({chunk.keys()}) FROM STDIN
        WITH (FORMAT CSV, DELIMITER E'|', QUOTE E'"', ESCAPE E'"')
    ''')
    with connect() as conn:
        try:
            cur = conn.cursor()
            with cur.copy(query) as writer:  # type: ignore
                writer.write(val)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise RuntimeError(f'Error inserting > {str(e)}') from e
