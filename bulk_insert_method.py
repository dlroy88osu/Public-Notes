import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import psycopg as pg

BULK_WORKERS = 4  # Number of threads to use - adjust as needed


def connect(tenant: str) -> pg.Connection:
    '''
    Returns a connection to the tenant database

    Args:
        tenant (str): The tenant name
        server (str): The server name

    Returns:
        psycopg.Connection: The connection object
    '''
    return pg.connect(
        host=your_host,
        port=your_port,
        dbname=your_tenant,
        user=your_username,
        password=your_password,

        # I work with gigs at a pop, so I need to keep the connection alive
        keepalives=1,
        keepalives_idle=5,
        keepalives_interval=2,
        keepalives_count=2
    )


def publish(df: pd.DataFrame, tenant: str, table: str, schema: str) -> None:
    '''
    Set up the threading and the SQL insert statement

    Args:
        df (pandas.DataFrame): dataframe to insert
        tenant (str): db tenant
        schema (str): schema name
        table (str): table name
    '''
    print(f'Publishing {table} to {tenant}')
    chunks = chunk_data(df)
    with ThreadPoolExecutor(max_workers=BULK_WORKERS) as exe:
        f = {exe.submit(insert, chunk, tenant, schema, table) for chunk in chunks}  # type: ignore
        for future in as_completed(f):
            try:
                future.result()
            except Exception as e:
                print(e)
                raise RuntimeError(f'Error in: {e}') from e
    print('Finished publishing')


def chunk_data(df: pd.DataFrame, chunk_size: int = 50000) -> list[dict]:
    '''
    Chunk the data into smaller pieces for bulk insert operations

    Args:
        df (pandas.DataFrame): dataframe to insert
        chunk_size (int): size of the chunks (default: 50,000)

    Returns:
        list[dict]: list of dictionaries of the chunked data
    '''
    try:
        data_dict = df.to_dict(orient='list')
        keys = list(data_dict.keys())
        values = list(data_dict.values())
        chunked_data = []
        for i in range(0, len(values[0]), chunk_size):
            chunk_keys = keys
            chunk_values = [v[i:i + chunk_size] for v in values]
            chunked_data.append(dict(zip(chunk_keys, chunk_values)))
        return chunked_data
    except Exception as e:
        print(e)
        raise RuntimeWarning from e


def insert(chunk: list[dict], tenant: str, schema: str, table: str) -> None:
    '''
    Execute the copy statement

    Args:
        chunk (list): list of dictionaries of the chunked data
        tenant (str): db tenant
        schema (str): schema name
        table (str): table name
    '''
    def cln(x) -> str:
        return '' if x is None else comp.sub('', str(x).replace('"', '""').strip()).lower()

    comp = re.compile(r"^\s*(''|nan|N/A|NULL|NAT)\s*$", re.IGNORECASE)

    # this is the format that works for me though by technicality it just needs to be in CSV format
    val = '\n'.join('|'.join(comp.sub('', cln(col)).replace('\n', '').replace('\r', '')
                             for col in row) for row in zip(*chunk.values()))  # type: ignore
    col = ', '.join([x.replace('order', '"order"') if x == 'order'
                     else x for x in chunk.keys()])  # type: ignore
    sql = (f'''
        COPY {schema}.{table} ({col}) FROM STDIN
        WITH (FORMAT CSV, DELIMITER E'|', QUOTE E'"', ESCAPE E'"')
    ''')

    with connect(tenant) as conn:
        try:
            cur = conn.cursor()
            with cur.copy(sql) as writer:  # type: ignore
                writer.write(val)
            conn.commit()
        except Exception as e:
            print(e)
            conn.rollback()
            raise RuntimeError(f'Error in: {e}') from e
