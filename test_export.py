#!/usr/bin/env python3
"""
Test script - runs EXACTLY the same commands as export_all.sh
No skipping, no shortcuts. Tests all export types.
"""
import os
import subprocess
import tempfile
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('ETHERLINK_API_KEY')
PROVIDER_URI = f"https://the-tie-mainnet-evm.octez.io?apikey={API_KEY}"

# Test with 3 blocks
START_BLOCK = 24900000
END_BLOCK = 24900002

def run_cmd(cmd, description):
    print(f"\n{'='*60}")
    print(f"### {description}")
    print(f"{'='*60}")
    print(f"Command: {' '.join(cmd)}\n")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"❌ FAILED")
        print(f"STDERR: {result.stderr[:2000]}")
        return False
    print(f"✅ SUCCESS")
    if result.stdout:
        print(result.stdout[:1500] if len(result.stdout) > 1500 else result.stdout)
    return True

def show_file(filepath, label, max_lines=4):
    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
        print(f"\n--- {label} ---")
        with open(filepath) as f:
            for i, line in enumerate(f):
                if i < max_lines:
                    print(line.strip()[:200])
    else:
        print(f"\n--- {label}: (empty or missing) ---")

def main():
    print(f"Testing Etherlink RPC: {PROVIDER_URI[:60]}...")
    print(f"Block range: {START_BLOCK} - {END_BLOCK}")
    print(f"\nRunning EXACT same commands as export_all.sh\n")
    
    results = {}
    
    with tempfile.TemporaryDirectory() as tmpdir:
        blocks_file = f"{tmpdir}/blocks.csv"
        transactions_file = f"{tmpdir}/transactions.csv"
        token_transfers_file = f"{tmpdir}/token_transfers.csv"
        tx_hashes_file = f"{tmpdir}/tx_hashes.csv"
        receipts_file = f"{tmpdir}/receipts.csv"
        logs_file = f"{tmpdir}/logs.csv"
        contract_addresses_file = f"{tmpdir}/contract_addresses.csv"
        contracts_file = f"{tmpdir}/contracts.csv"
        token_addresses_file = f"{tmpdir}/token_addresses.txt"
        tokens_file = f"{tmpdir}/tokens.csv"

        # 1. BLOCKS AND TRANSACTIONS (export_all.sh line 94)
        results['blocks_and_transactions'] = run_cmd([
            "python3", "ethereumetl", "export_blocks_and_transactions",
            f"--start-block={START_BLOCK}",
            f"--end-block={END_BLOCK}",
            f"--provider-uri={PROVIDER_URI}",
            f"--blocks-output={blocks_file}",
            f"--transactions-output={transactions_file}"
        ], "1. BLOCKS AND TRANSACTIONS (line 94)")
        
        show_file(blocks_file, "Blocks")
        show_file(transactions_file, "Transactions")

        # 2. TOKEN TRANSFERS (export_all.sh line 104)
        results['token_transfers'] = run_cmd([
            "python3", "ethereumetl", "export_token_transfers",
            f"--start-block={START_BLOCK}",
            f"--end-block={END_BLOCK}",
            f"--provider-uri={PROVIDER_URI}",
            f"--output={token_transfers_file}"
        ], "2. TOKEN TRANSFERS (line 104)")
        
        show_file(token_transfers_file, "Token Transfers")

        # 3. EXTRACT TX HASHES (export_all.sh line 114)
        results['extract_tx_hashes'] = run_cmd([
            "python3", "ethereumetl", "extract_csv_column",
            "--input", transactions_file,
            "--output", tx_hashes_file,
            "--column", "hash"
        ], "3. EXTRACT TX HASHES (line 114)")
        
        show_file(tx_hashes_file, "TX Hashes")

        # 4. RECEIPTS AND LOGS (export_all.sh line 126)
        results['receipts_and_logs'] = run_cmd([
            "python3", "ethereumetl", "export_receipts_and_logs",
            "--transaction-hashes", tx_hashes_file,
            f"--provider-uri={PROVIDER_URI}",
            f"--receipts-output={receipts_file}",
            f"--logs-output={logs_file}"
        ], "4. RECEIPTS AND LOGS (line 126)")
        
        show_file(receipts_file, "Receipts")
        show_file(logs_file, "Logs")

        # 5. EXTRACT CONTRACT ADDRESSES (export_all.sh line 136)
        results['extract_contract_addresses'] = run_cmd([
            "python3", "ethereumetl", "extract_csv_column",
            "--input", receipts_file,
            "--column", "contract_address",
            "--output", contract_addresses_file
        ], "5. EXTRACT CONTRACT ADDRESSES (line 136)")
        
        show_file(contract_addresses_file, "Contract Addresses")

        # 6. CONTRACTS (export_all.sh line 144)
        results['contracts'] = run_cmd([
            "python3", "ethereumetl", "export_contracts",
            "--contract-addresses", contract_addresses_file,
            f"--provider-uri={PROVIDER_URI}",
            f"--output={contracts_file}"
        ], "6. CONTRACTS (line 144)")
        
        show_file(contracts_file, "Contracts")

        # 7. EXTRACT TOKEN ADDRESSES (export_all.sh line 154)
        # Uses: sort | uniq
        print(f"\n{'='*60}")
        print("### 7. EXTRACT TOKEN ADDRESSES (line 154)")
        print(f"{'='*60}")
        cmd = f"python3 ethereumetl extract_csv_column -i {token_transfers_file} -c token_address -o - | sort | uniq > {token_addresses_file}"
        print(f"Command: {cmd}\n")
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ FAILED: {result.stderr}")
            results['extract_token_addresses'] = False
        else:
            print("✅ SUCCESS")
            results['extract_token_addresses'] = True
        
        show_file(token_addresses_file, "Token Addresses")

        # 8. TOKENS (export_all.sh line 162)
        results['tokens'] = run_cmd([
            "python3", "ethereumetl", "export_tokens",
            "--token-addresses", token_addresses_file,
            f"--provider-uri={PROVIDER_URI}",
            f"--output={tokens_file}"
        ], "8. TOKENS (line 162)")
        
        show_file(tokens_file, "Tokens")

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    for step, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} - {step}")
    
    passed = sum(1 for s in results.values() if s)
    total = len(results)
    print(f"\nTotal: {passed}/{total} passed")

if __name__ == "__main__":
    main()
