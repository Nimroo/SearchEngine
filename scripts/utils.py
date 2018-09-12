import base64
import json
import struct
import heapq

from graphviz import Digraph
from requests import post, get

class DomainGraphDrawer:

    def __init__(self):
        self.dot = Digraph(comment="Domain Connections")

    def add_edge(self, start, end, weight):
        self.dot.node(start)
        self.dot.node(end)
        self.dot.edge(start, end, label="\""+str(weight)+"\"", penwidth=str(weight))

    def draw(self):
        self.dot.render(view=True)

    def store_dot_file(self, file_name):
        self.dot.render(file_name + ".dot")
        self.dot.render(file_name + ".gov")
        self.dot.render(file_name + ".png")


def scan(start_row=None, end_row=None, batch=None, table_name=None):
    data = {}
    if not batch is None:
        data['batch'] = batch
    if not start_row is None:
        data['startRow'] = base64.b64encode(start_row).decode()
    if not end_row is None:
        data['endRow'] = base64.b64encode(end_row).decode()
    if table_name is None:
        print("Not table specified")
        return None

    headers = {'content-type': 'application/json', 'Accept': 'application/json'}
    result = post("http://omlet0:60006/" + table_name + "/scanner", headers=headers, data=json.dumps(data))
    print(result)
    return get(result.headers['location'], headers=headers).json()


def decode(data):
    return base64.b64decode(data)


def abs_link(link):
    if "https" in link:
        return link[8:]
    return link[7:]


def get_top_domains(top_count):
    result = scan(table_name="pageRankDomain")

    h = []
    for i in result['Row']:
        item = []
        for cell in i['Cell']:
            # print(base64.b64decode(cell['column'].encode()).decode())
            if str(base64.b64decode(cell['column'].encode()).decode()) == 'pageRank:pageRank':
                # print(base64.b64decode(cell['column']),
                #       struct.unpack('>d', base64.standard_b64decode(cell['$']))[0])
                item.insert(0, struct.unpack('>d', base64.standard_b64decode(cell['$']))[0])
            else:
                item.append(base64.b64decode(cell['$'].encode()))
                # print(base64.b64decode(cell['column'].encode()), base64.b64decode(cell['$'].encode()))
        heapq.heappush(h, item)
        item.append(base64.b64decode(i['key']))
    return heapq.nlargest(top_count, h)


def get_node_edges(row):
    headers = {'content-type': 'application/json', 'Accept': 'application/json'}
    result = get("http://omlet0:60006/domain/"+row, headers=headers) # base64.standard_b64encode(row)
    if result.status_code == 200:
        return result.json()
    print(result.status_code)
    return None


def draw_domain_graph():
    d = DomainGraphDrawer()
    tops = get_top_domains(100)
    top100 = [a[-1].decode() for a in tops]
    print(top100)
    results = []
    a = set()
    for item in top100:

        result = get_node_edges(item)
        if result is None:
            continue
        results.append(result)
        for i in result['Row']:

            for cell in i['Cell']:
                if decode(cell['column']).decode() == 'domainGraph:domain':
                    a.add(abs_link(decode(cell['$']).decode()))
    print(a)

    for result in results:

        for i in result['Row']:
            edges = {'dests': []}
            for cell in i['Cell']:
                if not decode(cell['column']).decode() == 'domainGraph:domain':
                    # print(decode(cell['column']).decode()[12:],
                    #       struct.unpack('>i', decode(cell['$']))[0])
                    edges['dests'].append([abs_link(decode(cell['column']).decode()[12:]), struct.unpack('>i', decode(cell['$']))[0]])
                else:
                    # print(decode(i['key']), decode(cell['column']),
                    #       decode(cell['$']).decode())
                    edges['src'] = abs_link(decode(cell['$']).decode())

            for e in edges['dests']:
                if e[0] in a:
                    d.add_edge(edges['src'], e[0], e[1])

    d.store_dot_file("a")