import os
import subprocess

from dotenv import find_dotenv, load_dotenv
from honeybadger import honeybadger

load_dotenv(find_dotenv())
# honey_badger_api_key = os.getenv('HONEYBADGER')
# honeybadger.configure(api_key=honey_badger_api_key)


def main():
    print('Starting ETL Script For ETH RPC')
    PROVIDER = os.getenv('PROVIDER')
    gcs = 'gs://the-tie-polygon'
    subprocess.run(
        f"python3 ethereumetl.py stream --provider-uri {PROVIDER} -e block,transaction,log,token_transfer --output {gcs} --lag 250 --block-batch-size 2", shell=True)


if __name__ == "__main__":
    main()
    raise Exception(
        "Error GCS ETL Program Was Terminated! We are No Longer Updating Blockchain Data for Polygon! FIX ASAP")
