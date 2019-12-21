#!/usr/bin/env python3
from pyln.client import LightningRpc, RpcError
from concurrent import futures

executor = futures.ThreadPoolExecutor(max_workers=20)

def add_chan(node, channel):
    if 'channels' not in node:
        node['channels'] = []
    node['channels'].append(channel)


def merge_channels(node_map, channels):
    for c in channels:
        if c['source'] in node_map:
            add_chan(node_map[c['source']], c)
        if c['destination'] in node_map:
            add_chan(node_map[c['destination']], c)


def build_node_map(nodes):
    """ Returns node_map, a map of nodes keyed by nodeid """
    node_map = {}
    for n in nodes:
        node_map[n['nodeid']] = n
    return node_map


def prune_nodes(nodes):
    """ Prune to nodes that at least one publicly addressable address """
    return [ n for n in nodes if 'addresses' in n and len(n['addresses']) > 0]


def get_eligible(node_map, num_chans):
    """ Return the list of nodes that have at least {num_chans} active channels """
    eligible = []
    for nodeid, n in node_map.items():
        count = 0
        for c in n['channels']:
            if c['active'] and c['public']:
                count += 1
        if count >= num_chans:
            eligible.append(n)
    return eligible


def connect_to(rpc, nodes):
    """ For now we only connect to ipv4 nodes
        returns the count of successful connections
    """
    count = 0
    for n in nodes:
        count = connect_addr_inner(rpc, n, count)
        # for now, we limit ourselves to 10 connects
        print(count)
        if count == 10:
            return count
    return count


def connect_addr_inner(rpc, n, count):
    for a in n['addresses']:
        if a['type'] == 'ipv4':
            fut = executor.submit(rpc.connect, n['nodeid'], a['address'], a['port'])
            try:
                # We time out after 3s
                fut.result(5)
                return count + 1
            except (futures.TimeoutError, RpcError) as e:
                print(e)
                continue
    return count


def fund_connected(rpc):
    eligible_peers = [p for p in rpc.listpeers()['peers'] if len(p['channels']) == 0]
    for p in eligible_peers:
        fut = executor.submit(rpc.fundchannel, p['id'], 1600000, minconf=0)
        try:
            fut.result(5)
            print("funded channel with {}!!".format(p['id']))
        except (futures.TimeoutError, RpcError) as e:
            print("error!! {}".format(e))


def main(connect, fund):
    rpc = LightningRpc("/home/bits/.lightning/testnet/lightning-rpc")
    channels = rpc.listchannels()['channels']
    nodes = rpc.listnodes()['nodes']

    pruned = prune_nodes(nodes)
    node_map = build_node_map(pruned)
    merge_channels(node_map, channels)
    eligible = get_eligible(node_map, 2)

    if connect:
        connected = connect_to(rpc, eligible)
        print("connected to {} of {} eligible nodes".format(connected, len(eligible)))

    if fund:
        fund_connected(rpc)
