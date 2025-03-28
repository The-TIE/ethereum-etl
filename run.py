import subprocess
from honeybadger import honeybadger
import os
import requests
from dotenv import load_dotenv,find_dotenv
load_dotenv(find_dotenv())
#honey_badger_api_key = os.getenv('HONEYBADGER')
#honeybadger.configure(api_key=honey_badger_api_key)

def main():
    print('Starting ETL Script For ETH RPC')
    PROVIDER = os.getenv('PROVIDER')
    ETL_CONNECTION = os.getenv('ETL_CONNECTION')
    SS = os.getenv('SS')
    #subprocess.run(f"python3 ethereumetl.py stream --provider-uri {PROVIDER} -e block,transaction,log,token_transfer,trace,contract,token --output {SS} --lag 14"
    #                                                                        ,shell=True)
    #subprocess.run(f"python3 ethereumetl.py stream --provider-uri {PROVIDER} -e block,transaction,log,token_transfer,trace,contract,token --output {SS} --lag 14"
    #                                                                        ,shell=True)
    subprocess.run(f"python3 ethereumetl.py stream --provider-uri {PROVIDER} -e block,transaction,log,token_transfer,trace,contract,token --output {SS} --lag 500 --block-batch-size 1", shell=True)

if __name__ == "__main__":
    main()
