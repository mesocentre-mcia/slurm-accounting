import re


_node_re = re.compile(r'^([A-Za-z_\-]+)(\d+)$')


def node_split_number(node_name):
    m = _node_re.match(node_name)

    return m.groups()


class Node:
    def __init__(self, name):
        self.name = name

        self.prefix, self.suffix = node_split_number(self.name)

        self.number = int(self.suffix, 10)

        #self.number_format = f'{{:0{}d}}'.format(len(self.name) - len(self.prefix))

    def is_right_neighbour(self, other):
        return (
            (self.prefix == other.prefix)
            and (len(self.suffix) == len(other.suffix))
            and ((other.number - self.number) == 1)
        )

    def slice(self, right):
        if self.prefix != right.prefix:
            raise ValueError()

        if self.number > right.number:
            raise ValueError()

        if right.number == self.number:
            return self.name

        return '{}[{}-{}]'.format(self.prefix, self.suffix, right.suffix)

def node_spec_from_list(node_list):
    ret = []

    node_list.sort()

    first, node_list = Node(node_list[0]), node_list[1:]
    last = first

    while len(node_list) > 0:
        current, node_list = Node(node_list[0]), node_list[1:]

        if last.is_right_neighbour(current):
            last = current
            continue

        ret.append(first.slice(last))

        first = current
        last = first

    ret.append(first.slice(last))

    return ','.join(ret)


def nodenum_format(s):
    length = len(s)
    return f'{{:0{length}}}'


def parse_node_sequence(s):
    prefix, num_slice = s.rstrip(']').split('[')

    if '-' not in num_slice:
        return prefix, [num_slice]

    start, end = num_slice.split('-')

    return prefix, [
        nodenum_format(start).format(n)
        for n in range(int(start), int(end) + 1)
    ]

def parse_node_spec(s):
    words = s.split(',')

    ret = []

    for w in words:
        if '[' not in w:
            ret.append(w)
            continue

        prefix, nums = parse_node_sequence(w)

        for num in nums:
            ret.append(prefix + num)

    return ret

def int_or_string(s):
    try:
        return int(s)
    except ValueError:
        return s

def parse_properties(s):
    return {k: int_or_string(v) for k, v in [w.split('=', 1) for w in s]}

def parse_node_name(line):
    words = line.split()

    node_spec, properties = words[0], words[1:]

    return parse_node_spec(node_spec), parse_properties(properties)

def parse_partition_name(line):
    words = line.split()

    name = words[0]
    properties = parse_properties(words[1:])

    if 'Nodes' in properties:
        node_spec = properties['Nodes']

        nodes = parse_node_spec(node_spec)

        properties['Nodes'] = nodes

    return name, properties

def parse_slurm_conf(f):
    node_dict = {}
    partition_dict = {}

    for l in f.readlines():
        l = l.strip()
        l = l.split('#', 1)[0].strip()
        if not l: continue
        key, value = [s.strip() for s in l.split('=', 1)]

        if key not in ['NodeName', 'PartitionName']: continue

        if key == 'NodeName':
            nodes, properties = parse_node_name(value)
            # print(key, nodes, properties)

            for node in nodes:
                node_dict[node] = properties

        elif key == 'PartitionName':
            # print(value)
            partition, properties = parse_partition_name(value)
            #print(key, partition, properties)

            partition_dict[partition] = properties

    return {'nodes': node_dict, 'partitions': partition_dict}

def nodes_procs(nodes, node_dict):
    procs = 0
    for n in nodes:
        procs += node_dict[n]['Procs']

    return procs

def partition_nodes(partition):
    return {n for n in partition['Nodes']}


def partition_procs(partition, node_dict):
    return nodes_procs(partition_nodes(partition), node_dict)

if __name__ == '__main__':
    f = open('/etc/slurm/slurm.conf', 'r')

    conf = parse_slurm_conf(f)

    # print(conf['nodes'])

    from configparser import ConfigParser

    cfg= ConfigParser()
    cfg.read('etc/sreporting.conf')

    all_procs = nodes_procs(conf['nodes'].keys(), conf['nodes'])

    for section_name in cfg.sections():
        if not section_name.startswith('report:'):
            continue

        section = cfg[section_name]

        print(section_name, section)
        report_partition = section.get('partition')
        print('  partition', report_partition)

        print('  nodes', section.get('nodes'))
        print('  cores', section.get('cores'))

        procs = all_procs
        if report_partition is not None:
            procs = partition_procs(conf['partitions'][report_partition], conf['nodes'])

        print('  conf partition procs', procs)
