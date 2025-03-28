# MIT License
#
# Copyright (c) 2018 Evgeniy Filatov, evgeniyfilatov@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.mainnet_daofork_state_changes import DAOFORK_BLOCK_NUMBER
from ethereumetl.mappers.trace_mapper import EthTraceMapper
from ethereumetl.service.eth_special_trace_service import EthSpecialTraceService

from ethereumetl.service.trace_id_calculator import calculate_trace_ids
from ethereumetl.service.trace_status_calculator import calculate_trace_statuses
from ethereumetl.utils import validate_range

import requests
import json
import os

#NODE_URL = "https://eth-mainnet.g.alchemy.com/v2/ewRt8e3H3ysORQpNwdTRXwyyIMvuDpxO"
NODE_URL = "https://node.mainnet.etherlink.com"
HEADERS = {"Content-Type": "application/json"}

def get_node_version():
    """
    Fetches the Ethereum node version to determine its client type.
    
    :return: Node version string (e.g., "Geth/v1.12.0-stable" or "OpenEthereum/v3.3.5").
    """
    payload = {
        "jsonrpc": "2.0",
        "method": "web3_clientVersion",
        "params": [],
        "id": 1
    }
    
    response = requests.post(NODE_URL, headers=HEADERS, data=json.dumps(payload))
    
    if response.status_code == 200:
        result = response.json()
        return result.get("result", "Unknown")
    else:
        raise Exception(f"Error {response.status_code}: {response.text}")

def get_block_by_number(block_number: str):
    """
    Gets block information including block hash.
    
    :param block_number: The block number in hex format
    :return: Block information including hash
    """
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [block_number, False],
        "id": 1
    }
    
    response = requests.post(NODE_URL, headers=HEADERS, data=json.dumps(payload))
    
    if response.status_code == 200:
        result = response.json()
        return result.get("result", {})
    else:
        raise Exception(f"Error {response.status_code}: {response.text}")

def trace_block_by_number(block_number: str):
    """
    Calls debug_traceBlockByNumber if supported.
    
    :param block_number: The block number in hex format (e.g., '0x10d4f').
    :return: The traced block data or an error message.
    """
    node_version = get_node_version()
    print(f"Connected to Ethereum Node: {node_version}")

    # Determine if tracing is supported
    if "Geth" in node_version:
        print("⚠️ Geth detected - Ensure debug API is enabled.")
    elif "OpenEthereum" in node_version or "Nethermind" in node_version:
        print("✅ Node supports tracing.")

    payload = {
        "jsonrpc": "2.0",
        "method": "debug_traceBlockByNumber",
        "params": [block_number, {"tracer": "callTracer"}],
        "id": 1
    }
    
    response = requests.post(NODE_URL, headers=HEADERS, data=json.dumps(payload))
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error {response.status_code}: {response.text}")

def convert_debug_trace_to_trace_block(debug_traces, block_number, block_hash):
    """
    Converts debug_trace format to trace_block format.
    
    :param debug_traces: Debug trace data from debug_traceBlock
    :param block_number: Block number (int)
    :param block_hash: Block hash (string)
    :return: List of traces in trace_block format
    """
    traces = []
    
    def process_call(call, trace_address=None, parent_tx_hash=None):
        if trace_address is None:
            trace_address = []
            
        # Create trace entry
        trace_entry = {
            "action": {
                "to": call.get("to", ""),
                "from": call.get("from", ""),
                "callType": call.get("type", "call").lower(),
                "gas": hex(int(call.get("gas", "0"), 16)) if isinstance(call.get("gas"), str) else hex(call.get("gas", 0)),
                "input": call.get("input", "0x"),
                "to": call.get("to", ""),
                "value": call.get("value", "0x0")
            },
            "blockHash": block_hash,
            "blockNumber": block_number,
            "address": call.get("to", ""),
            "result": {
                "gasUsed": hex(int(call.get("gasUsed", "0"), 16)) if isinstance(call.get("gasUsed"), str) else hex(call.get("gasUsed", 0)),
                "output": call.get("output", "0x"),
                "address": call.get("to", ""),
            } if "gasUsed" in call else {},
            "subtraces": len(call.get("calls", [])),
            "traceAddress": trace_address,
            "transactionHash": parent_tx_hash or "",
            "transactionPosition": 0,  # Would need actual position
            "type": call.get("type", "call").lower()
        }
        
        traces.append(trace_entry)
        
        # Process nested calls
        for i, subcall in enumerate(call.get("calls", [])):
            new_trace_address = trace_address + [i]
            process_call(subcall, new_trace_address, parent_tx_hash)
    
    # Process each transaction's traces
    for tx_trace in debug_traces.get("result", []):
        tx_hash = tx_trace.get("txHash", "")
        process_call(tx_trace.get("result", {}), [], tx_hash)
    
    return traces


class ExportTracesJob(BaseJob):
    def __init__(
            self,
            start_block,
            end_block,
            batch_size,
            web3,
            item_exporter,
            max_workers,
            include_genesis_traces=False,
            include_daofork_traces=False):
        validate_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block

        self.web3 = web3

        # TODO: use batch_size when this issue is fixed https://github.com/paritytech/parity-ethereum/issues/9822
        self.batch_work_executor = BatchWorkExecutor(1, max_workers)
        self.item_exporter = item_exporter

        self.trace_mapper = EthTraceMapper()

        self.special_trace_service = EthSpecialTraceService()
        self.include_genesis_traces = include_genesis_traces
        self.include_daofork_traces = include_daofork_traces

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self._export_batch,
            total_items=self.end_block - self.start_block + 1
        )

    def _export_batch(self, block_number_batch):
        # TODO: Change to len(block_number_batch) > 0 when this issue is fixed
        # https://github.com/paritytech/parity-ethereum/issues/9822
        assert len(block_number_batch) == 1
        block_number = block_number_batch[0]

        all_traces = []

        if self.include_genesis_traces and 0 in block_number_batch:
            genesis_traces = self.special_trace_service.get_genesis_traces()
            all_traces.extend(genesis_traces)

        if self.include_daofork_traces and DAOFORK_BLOCK_NUMBER in block_number_batch:
            daofork_traces = self.special_trace_service.get_daofork_traces()
            all_traces.extend(daofork_traces)

        # TODO: Change to traceFilter when this issue is fixed
        # https://github.com/paritytech/parity-ethereum/issues/9822

        # Src: https://github.com/blockchain-etl/ethereum-etl/issues/394
        try:
            block_info = get_block_by_number(hex(block_number))
            block_hash = block_info.get("hash", "")
            debug_traces = trace_block_by_number(hex(block_number))

   
            traces = convert_debug_trace_to_trace_block(debug_traces, block_number, block_hash)
 
            json_traces = traces
  
        except ValueError as e:
            if 'insufficient funds' in str(e):
                print(e)
                return
            else:
                raise e

        if json_traces is None:
            raise ValueError('Response from the node is None. Is the node fully synced? Is the node started with tracing enabled? Is trace_block API enabled?')
        
        traces = [self.trace_mapper.json_dict_to_trace(json_trace) for json_trace in json_traces]
        
        all_traces.extend(traces)
        calculate_trace_statuses(all_traces)
        calculate_trace_ids(all_traces)
        calculate_trace_indexes(all_traces)

        for trace in all_traces:
            self.item_exporter.export_item(self.trace_mapper.trace_to_dict(trace))

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()


def calculate_trace_indexes(traces):
    # Only works if traces were originally ordered correctly which is the case for Parity traces
    for ind, trace in enumerate(traces):
        trace.trace_index = ind




