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
    attempts = 0
    for n in nodes:
        attempts += 1
        count = connect_addr_inner(rpc, n, count)
        print("connected {} of {}/{}".format(count, attempts, len(nodes)))
    return count


def connect_addr_inner(rpc, n, count):
    for a in n['addresses']:
        if a['type'] in ['ipv4', 'ipv6', 'torv3', 'torv2']:
            # some nodes have a poorly configured ipv6 of '::'
            if a['address'] in ['0.0.0.0', '127.0.0.1', '::']:
                print('skipping null address')
                continue
            fut = executor.submit(rpc.connect, n['nodeid'], a['address'], a['port'])
            try:
                # We time out after 3s
                fut.result(3)
                return count + 1
            except (futures.TimeoutError, RpcError) as e:
                print("errror encountered! {}".format(e))
                continue
    print("unable to connect to {}".format(n['nodeid']))
    return count


def fund_connected(rpc):
    amount_sat = 16000000
    eligible_peers = [p for p in rpc.listpeers()['peers'] if len(p['channels']) == 0]
    count = 0
    for p in eligible_peers:
        # break early, at least until i've solved the borking error
        if count == 25:
            return count
        fut = executor.submit(rpc.fundchannel, p['id'], amount_sat, minconf=0, push_msat=(amount_sat // 2) * 1000)
        try:
            fut.result(5)
            print("funded channel with {}!!".format(p['id']))
            count += 1
        except (futures.TimeoutError, RpcError) as e:
            print("error!! {}".format(e))
    return count


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
