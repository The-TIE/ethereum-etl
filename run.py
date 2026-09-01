import subprocess
from honeybadger import honeybadger
import os
from dotenv import load_dotenv,find_dotenv
load_dotenv(find_dotenv())
#honey_badger_api_key = os.getenv('HONEYBADGER')
#honeybadger.configure(api_key=honey_badger_api_key)

def main():
    """
    Goal: Main function for run_etl it will just run our etl-stream but in a python script so we can get notified by HoneyBadger if it breaks
    Parameters: None
    Return: None
    """
    print('Starting ETL Script For ETH RPC')
    PROVIDER = os.getenv('PROVIDER')
    ETL_CONNECTION = os.getenv('ETL_CONNECTION')
    KAFKA = os.getenv('KAFKA')
    SS = os.getenv('SS')
    subprocess.run(f"python3 ethereumetl.py stream --provider-uri {PROVIDER} -e block,transaction,log,token_transfer --output {SS} --lag 14"
                    ,shell=True)
    #subprocess.run(f"python3 ethereumetl.py stream --provider-uri {PROVIDER} -e block,transaction,log,token_transfer --output {KAFKA} --lag 14"
    #                ,shell=True)

if __name__ == "__main__":
    main()
    raise Exception ("Error ETL Program Was Terminated! We are No Longer Updating Blockchain Data for Ethereum! FIX ASAP")
