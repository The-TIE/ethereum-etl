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
    subprocess.run(f"python3 ethereumetl.py stream --provider-uri {PROVIDER} -e block,transaction,log,token_transfer,trace,contract,token --output {SS} --lag 250 --block-batch-size 1", shell=True)

if __name__ == "__main__":
    main()
    requests.post("https://discord.com/api/webhooks/1159169280982663198/vG4BlCWD9X42Mx_KmJ5Q0Lul3Elaatyd6E2rKAuO81L_7r7fuI8HMMT5FzKDVUuhy2FV", json={"content": "EtherLink ETL stopped streaming. <@238509144087330818>"})
    raise Exception ("Error ETL Program Was Terminated! We are No Longer Updating Blockchain Data for EtherLink! FIX ASAP")
