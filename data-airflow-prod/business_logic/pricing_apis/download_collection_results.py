import logging
import os
import zipfile
from glob import glob
from os.path import join

import awswrangler as wr
import pandas as pd
import requests

RESULT_SET_ID = 1
START_COLLECTIONS = True


def list_collections(run_date: str, api_key: str, api_domain: str):
    page_num = 1
    total_pages = 1

    collection_ids = []

    while total_pages >= page_num:
        params = {
            'api_key': api_key,
            'page': page_num
        }
        api_response = requests.get(f'https://{api_domain}/collections',
                                    params=params).json()

        page_num = api_response['current_page'] + 1
        total_pages = api_response['total_pages']

        for number, collection in enumerate(api_response['collections'], start=1):
            if collection['name'] == run_date:
                collection_ids.append(collection["id"])

    logging.info(collection_ids)

    return collection_ids


def get_all_results(result_set_id: int, collection_id: str, api_domain: str, api_key: str) -> str:
    download_url = get_result_set_download_url(result_set_id, collection_id, api_domain, api_key)
    logging.info(f'Download URL: {download_url}')
    return download_file(download_url, api_key)


def get_result_set_download_url(id: int, collection_id: str, api_domain: str,
                                api_key: str) -> str:
    r = requests.get(
        f"https://{api_domain}/collections/{collection_id}/results/{id}/jsonlines?api_key={api_key}").json()  # noqa: E501
    check_response_success(r)
    return r["result"]["download_links"]["all_pages"]


def download_file(from_url: str, api_key: str) -> str:
    r = requests.get(f'{from_url}?={api_key}', allow_redirects=True)
    filename = from_url.split('/')[-1]
    with open(filename, 'wb') as f:
        f.write(r.content)
    logging.info(f'Downloaded result set to {filename}')
    return filename


def unzip_file(path: str, to_dir: str):
    with zipfile.ZipFile(path, 'r') as f:
        logging.info(f'Extracting file {path}')
        f.extractall(to_dir)
    os.remove(path)


class RequestBodyError(Exception):
    pass


def check_response_success(response: dict) -> None:
    if not isinstance(response, dict):
        raise RequestBodyError
    if response.get("request_info").get("success", False) is False:
        raise RequestBodyError


def read_result_sets(from_dir: str, api_domain: str) -> pd.DataFrame:
    meta = [['request_parameters'], ['request_metadata']]
    if api_domain == 'api.countdownapi.com':
        meta.append(['search_information'])
        record_path = [['search_results']]
    elif api_domain == 'api.rainforestapi.com':
        # meta.append(['request_info'])
        meta.append(['product'])
        record_path = [['offers']]
    else:
        raise Exception('api_domain must be api.countdownapi.com or api.rainforestapi.com')

    dfs = []  # an empty list to store the data frames

    logging.info('Reading results into Dataframe')
    for f in glob(join(from_dir, './*.jsonl')):
        # read data frame from json file
        data = pd.read_json(f, lines=True)
        if api_domain == 'api.rainforestapi.com':
            if not hasattr(data, 'result'):
                logging.warn(f'No .result for {data}')
                logging.info(data.keys())
                continue
            data.result = data.result.apply(
                lambda x: x if x.get('offers') is not None else x | {"offers": []})

        results = pd.json_normalize(data.result,
                                    sep='__',
                                    record_path=record_path,
                                    meta=meta,
                                    errors='ignore')
        dfs.append(results)  # append the data frame to the list
        os.remove(f)

    df = pd.concat(dfs, ignore_index=True)
    logging.info(f'Dataframe rowcount : {len(df.index)}')

    logging.info('Normalizing metadata columns')

    for m in meta[::-1]:
        logging.info(f'Normalizing metadata col {m}')

        meta_col = m[0]
        logging.info(f'meta_col: {meta_col}')

        metadata = pd.json_normalize(df[meta_col], sep='__', max_level=1)

        if meta_col == 'request_parameters' and 'rainforestapi' in api_domain:
            logging.info('Dropping asin from request_parameters')
            metadata.drop('asin', axis=1, inplace=True)
        else:
            pass

        df.drop(meta_col, axis=1, inplace=True)
        df = metadata.join(df)
    return df


def process_collection_results(run_date: str, api_domain: str, api_key: str, s3_destination: str):
    collection_ids = list_collections(run_date, api_key, api_domain)
    logging.info(f'Downloaded collections: {collection_ids} for {run_date}')

    output_dir = f"./output/{api_domain.split(sep='.')[1]}_{run_date}"
    os.system(f'rm -rf {output_dir}/*')

    for collection_id in collection_ids:
        try:
            logging.info(f'Downloading results from collection id :{collection_id}')
            results_zipfile = get_all_results(RESULT_SET_ID, collection_id, api_domain, api_key)
            unzip_file(results_zipfile, output_dir)
        except RequestBodyError:
            logging.error(f'Error downloading results for collection id :{collection_id}')

    logging.info(os.listdir(output_dir))

    output_df = read_result_sets(output_dir, api_domain)
    logging.info('Uploading to S3 in parquet format')
    file_path = os.path.join(s3_destination, f"{api_domain.split(sep='.')[1]}_{run_date}.parquet")

    wr.s3.to_parquet(
        df=output_df,
        path=file_path,
        index=False,
    )
