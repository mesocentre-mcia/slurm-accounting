import subprocess
import os
import re
import tempfile
import datetime
import argparse

from . import config

from .slurm_config import parse_slurm_conf, node_spec_from_list, nodes_procs, parse_node_spec, partition_nodes

class Command(object):
    def __init__(self, cmd, opts=[], output_filter=lambda e: e, verbose=False, remote_host=None):
        self.cmd = cmd
        self.opts = opts
        self.verbose = verbose
        self.output_filter = output_filter
        self.remote_host = remote_host

    def __call__(self, cmdline=[]):
        cmdlist = [self.cmd] + self.opts + cmdline

        if self.remote_host is not None:
            cmdlist = ['ssh', self.remote_host] + cmdlist

        if self.verbose:
            print("Command:", " ".join([repr(e) for e in cmdlist]))

        with tempfile.TemporaryFile() as stdout:
            p = subprocess.Popen(cmdlist, stdout = stdout, stderr = subprocess.STDOUT)

            p.wait()

            if p.returncode != 0:
                print(type(p.returncode))
                raise subprocess.CalledProcessError(cmd=" ".join(cmdlist), returncode=p.returncode)

            stdout.seek(0)

            for l in stdout:
                yield self.output_filter(l.decode())

class SreportCluster(Command):
    @classmethod
    def filter(cls, e):
        return e.strip().split('|')

    def __init__(self, include_header=True, skip_groups=False, skip_users=False, verbose=False,
                 remote_host=None):
        super(SreportCluster, self).__init__('sreport', ['-n', '-P', '-t', 'Hour', 'cluster',
                                                        'AccountUtilizationByUser',
                                                        'format=account%30,login%30,used%30'],
                                             SreportCluster.filter, verbose=verbose,
                                             remote_host=remote_host)

        self.include_header = include_header
        self.skip_groups = skip_groups
        self.skip_users = skip_users

    def __call__(self, start=None, end=None):
        cmdline = []
        if start is not None:
            cmdline.append('Start=%s' % start)

        if end is not None:
            cmdline.append('End=%s' % end)

        super_call = super(SreportCluster, self).__call__(cmdline)

        if self.include_header:
            header = ['account', 'user', 'used(hours)']
            yield header

        for r in super_call:
            if r is None:
                continue

            if self.skip_groups and r[1] == '':
                continue

            if self.skip_users and r[1] != '':
                continue

            yield r

class Sacct(Command):
    @classmethod
    def filter(cls, e):
        return e.strip().split('|')

    def __init__(self, format=None, extra_options=[], verbose=False,
                 remote_host=None):

        self.format = format or (
            'jobid',
            'user',
            'elapsed',
            'ncpus',
            'partition',
            'nodelist',
            'group',
            'start',
            'end',
            'state',
        )

        super(Sacct, self).__init__(
            'sacct',
            ['-a', '--parsable2', '--noheader', '-X',
             '--format=%s' % ','.join(self.format)] + extra_options,
            Sacct.filter, verbose=verbose,
            remote_host=remote_host)

    def __call__(self, start=None, end=None, partition=None, nodes=None, states=[], other_args=[]):
        cmdline = []
        if start is not None:
            cmdline.append('--starttime=%s' % start)

        if end is not None:
            cmdline.append('--endtime=%s' % end)

        if partition is not None:
            cmdline.append('--partition=%s' % partition)

        if nodes is not None:
            cmdline.append('--nodelist=%s' % nodes)

        if states:
            cmdline.append('--state=%s' % ','.join(states))

        cmdline += other_args

        super_call = super(Sacct, self).__call__(cmdline)

        for r in super_call:
            if r is None:
                continue

            yield dict(list(zip(self.format, r)))

def parse_elapsed(s):
    days = 0
    hspec = s
    if '-' in s:
        days, hspec = s.split('-',1)
        days = int(days)
    hours, minutes, seconds = list(map(int, hspec.split(':')))

    return datetime.timedelta(days=days, seconds=seconds, minutes=minutes, hours=hours)

def parse_slurm_datetime(s):
    if s == 'Unknown':
        return None
    return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")


def parse_slurm_date(s):
    c = s.count('-')
    if c == 0:
        s += '-01-01'
    elif c == 1:
        s += '-01'

    return datetime.datetime.strptime(s, "%Y-%m-%d")


def parse_slurm_month(s):
    return datetime.datetime.strptime(s + '-01', "%Y-%m-%d")

def print_date(d):
    return d.strftime("%Y-%m-%d")

def print_month(d):
    return d.strftime("%Y-%m")

def print_datetime(d):
    return d.strftime("%Y-%m-%dT%H:%M:%S")

class Bin(object):
    def new(self):
        raise NotImplementedError

    def job(self, job):
        raise NotImplementedError

    def indices(self, indices):
        return []

    def __getitem__(self, key):
        raise NotImplementedError

    def __contains__(self, key):
        return True

    def __str__(self):
        return str(self[0])

class CpuSecondsBin(Bin):
    def __init__(self, newbin=None):
        self.cpuseconds = 0.

    def new(self):
        return self.__class__()

    def job(self, job):
        self.cpuseconds += job['cpuseconds']

    def __getitem__(self, key):
        return self.cpuseconds

class CpuHoursBin(CpuSecondsBin):
    def __getitem__(self, key):
        return self.cpuseconds / 3600.

    def __str__(self):
        return '%.f' % self[0]

class PercentBin(Bin):
    def __init__(self, bin, refval):
        self.bin = bin
        self.refval = refval

    def new(self):
        return self.__class__(self.bin.new(), self.refval)

    def job(self, job):
        self.bin.job(job)

    def __getitem__(self, key):
        return 100. * self.bin[key] / self.refval

    def __str__(self):
        return '%02.1f%%' % self[0]


class JobCountBin(Bin):
    def __init__(self, bin=None):
        self.count = 0.

    def new(self):
        return self.__class__()

    def job(self, job):
        self.count += 1

    def __getitem__(self, key):
        return self.count


class GroupingBin(Bin):
    separators = ('\n', ',', )
    def __init__(self, hashfunc, orderfunc, newbin):
        self.hashfunc = hashfunc
        self.orderfunc = orderfunc
        self.newbin = newbin
        self.bindict = {}

    def new(self):
        return self.__class__(self.newbin.new())

    def job(self, job):
        keys = self.hashfunc(job)

        if not isinstance(keys, list):
            keys = [keys]

        for k in keys:
            if k not in self.bindict:
                self.bindict[k] = self.newbin.new()

            bin = self.bindict[k]
            bin.job(job)

    def key_list(self):
        keys = list(self.bindict.keys())

        keys.sort(key=self.orderfunc)

        return keys

    def indices(self, indices):
        if len(indices) == 0: indices.append([])

        keys = list(set(indices[0] + self.key_list()))

        keys.sort(key=self.orderfunc)

        subindices = indices[1:]
        for b in self.bindict.values():
            subindices = b.indices(subindices)

        return [keys] + subindices

    def __getitem__(self, key):
        return self.bindict[key]

    def __contains__(self, key):
        return key in self.bindict

    def __str__(self):
        return str(self.bindict)


class UserGroupingBin(GroupingBin):
    def __init__(self, newbin):
        super(UserGroupingBin, self).__init__(hashfunc=lambda j: j['user'],
                                              orderfunc=None, newbin=newbin)


class GroupGroupingBin(GroupingBin):
    def __init__(self, newbin):
        super(GroupGroupingBin, self).__init__(hashfunc=lambda j: j['group'],
                                               orderfunc=None, newbin=newbin)

class StartGroupingBin(GroupingBin):
    def __init__(self, newbin):
        def hashfunc(j):
            return j['start'].split('T')[0]

        super(StartGroupingBin, self).__init__(hashfunc, orderfunc=parse_slurm_date, newbin=newbin)


class SpanGroupingBin(GroupingBin):
    def __init__(self, hashfunc, newbin, filling=(None, None)):

        super(SpanGroupingBin, self).__init__(hashfunc, orderfunc=parse_slurm_date, newbin=newbin)

        self.filling = filling

        self.__fill()

    def __fill(self):
        b, e = self.filling

        if b is None or e is None:
            return

        keys = self.hashfunc(dict(start=b, end=e))

        for k in keys[:-1]: # don't put a bin on last span

            if k not in self.bindict:
                self.bindict[k] = self.newbin.new()

    def next_span(self, date):
        raise NotImplementedError

    def new(self):
        return self.__class__(self.newbin.new(), self.filling)

    def job(self, job):
        keys = self.hashfunc(job)

        if not isinstance(keys, list):
            keys = [keys]

#        print('job', [job[k] for k in ['jobid', 'ncpus', 'start', 'end', 'cpuseconds']])
        cpuseconds = 0
        spans = []

        for k in keys:
            spanjob = job.copy()
            kd = parse_slurm_date(k)
            kdp1 = self.next_span(kd)

            jstart = parse_slurm_datetime(spanjob['start'])
            if jstart  < kd:
                jstart = kd
                spanjob['start'] = print_datetime(jstart)

            jend = parse_slurm_datetime(spanjob['end'])
            if jend > kdp1:
                jend = kdp1
                spanjob['end'] = print_datetime(jend)

            elapsed = jend - jstart
            cpus = int(spanjob['ncpus'])
            spanjob['cpuseconds'] = elapsed.total_seconds() * cpus

            if spanjob['cpuseconds'] == 0:
                # don't register empty jobs
                continue

            cpuseconds += spanjob['cpuseconds']
            spans.append((k, cpuseconds))

            if k not in self.bindict:
                self.bindict[k] = self.newbin.new()

            bin = self.bindict[k]
            bin.job(spanjob)

class DailyGroupingBin(SpanGroupingBin):
    span = datetime.timedelta(days=1)

    def __init__(self, newbin, filling=(None, None)):
        def hashfunc(j):
            # start bin
            b = parse_slurm_datetime(j['start'])

            # end bin is date of end, ep1 is for correct ending of the while loop
            ep1 = parse_slurm_date(j['end'].split('T', 1)[0]) + self.span

            ret = []
            while b < ep1:
                ret.append(print_date(b))
                b += self.span

            return ret

        super(DailyGroupingBin, self).__init__(hashfunc, newbin, filling)

    def next_span(self, date):
        return date + self.span

class MonthlyGroupingBin(SpanGroupingBin):

    def __init__(self, newbin, filling=(None, None)):
        def hashfunc(j):
            # start bin
            b = parse_slurm_month(j['start'].split('T', 1)[0].rsplit('-', 1)[0])

            # end bin is date of end, ep1 is for correct ending of the while loop
            ep1 = self.next_span(parse_slurm_month(j['end'].split('T', 1)[0].rsplit('-', 1)[0]))

            ret = []
            while b < ep1:
                ret.append(print_date(b))
                b = self.next_span(b)

            return ret

        super(MonthlyGroupingBin, self).__init__(hashfunc, newbin, filling)

    def next_span(self, date):
        ret = date + datetime.timedelta(days=31)
        return parse_slurm_month(print_month(ret))


def sreporting(conf_file, report=None, grouping_specs=None, start=None, end=None, extra_options=[]):
    # read slurm configuration
    with open('/etc/slurm/slurm.conf', 'r') as f:
        slurm_conf = parse_slurm_conf(f)

    slurm_nodes = slurm_conf['nodes']
    slurm_partitions = slurm_conf['partitions']

    # read report configuration
    cfg = config.Config(conf_file)

    query_grace = parse_elapsed(cfg.get('general', 'query_grace', '00:00:00'))

    query_start_date = cfg.getdate('general', 'default_start', '1970-01-01')
    start_date = query_start_date
    if start is not None:
        start_date = parse_slurm_date(start)
        query_start_date = max(query_start_date, start_date - query_grace)

    query_end_date = datetime.datetime.now()
    end_date = query_end_date
    if end is not None:
        end_date = parse_slurm_date(end)
        query_end_date = end_date + query_grace

    # select report
    report_section = 'report:' + (report or cfg.get('general', 'default_report'))

    src = Sacct(extra_options=extra_options, verbose=False)

    partition = cfg.get(report_section, 'partition', False) or None

    selected_nodes = set(slurm_nodes.keys())  # all nodes from cluster

    if partition is not None:
        # restrict to jobs running nodes from selected partition
        pnodes = set(partition_nodes(slurm_partitions[partition]))
        selected_nodes &= pnodes

    node_restriction = False

    restrict_to_partitions_nodes = cfg.get(report_section, 'restrict_to_partitions_nodes', False) or None
    if restrict_to_partitions_nodes is not None:
        # restrict to jobs running on nodes from specified partitions nodes 

        restrict_to_partitions_nodes = sorted(restrict_to_partitions_nodes.split(','))
        for part in restrict_to_partitions_nodes:
            pnodes = set(partition_nodes(slurm_partitions[part]))
            selected_nodes &= pnodes

            node_restriction = True


    restrict_to_nodes_spec = cfg.get(report_section, 'restrict_to_nodes', False) or None
    if restrict_to_nodes_spec is not None:
        # restrict to jobs running on certain nodes
        nnodes = set(parse_node_spec(restrict_to_nodes_spec))
        selected_nodes &= nnodes

        node_restriction = True


    selected_nodes_spec = None
    if node_restriction:
        selected_nodes_spec = node_spec_from_list(list(selected_nodes))

    print(report_section, partition, restrict_to_partitions_nodes, restrict_to_nodes_spec, selected_nodes_spec)

    maxseconds = 1
    # cores = cfg.get(report_section, 'cores', None)
    cores = nodes_procs(selected_nodes, slurm_nodes)

    cores = int(cores)
    duration = (end_date - start_date).total_seconds()
    maxseconds = int(cores * duration)

    bins_dict = {
        'cpu_seconds':CpuSecondsBin,
        'cpu_hours':CpuHoursBin,
        'job_count':JobCountBin,
        'user':UserGroupingBin,
        'group':GroupGroupingBin,
        'job_start':StartGroupingBin,
        'daily':lambda b: DailyGroupingBin(b, filling=(print_datetime(start_date), print_datetime(end_date))),
        'monthly':lambda b: MonthlyGroupingBin(b, filling=(print_datetime(start_date), print_datetime(end_date))),
    }

    grouping_specs = (grouping_specs or cfg.get(report_section, 'grouping', False) or 'cpu_hours').split(',')
    groupings = []
    for grouping_spec in grouping_specs:

        grouping_def = [s.strip() for s in grouping_spec.split('*')]
        title = grouping_def + []
        grouping_def.reverse()

        grouping = None
        for g in grouping_def:
            grouping = bins_dict[g](grouping)

        groupings.append((grouping, title))

    jobs = src(start=query_start_date.strftime('%Y-%m-%dT%H:%M:%S'),
               end=query_end_date.strftime('%Y-%m-%dT%H:%M:%S'),
               partition=partition,
               nodes=selected_nodes_spec,
               states=['RUNNING'])

    #jobs.next()
    for r in jobs:
        if r['state'] == 'PENDING':
            continue
        jstart = parse_slurm_datetime(r['start'])
        jend = parse_slurm_datetime(r['end']) or end_date

        if jend and jend < start_date:
            continue

        if jstart > end_date:
            continue

        jstart = max(jstart, start_date)
        r['start'] = print_datetime(jstart)

        if jend is not None:
            jend = min(jend, end_date)
        else:
            jend = end_date

        r['end'] = print_datetime(jend)

        elapsed = jend - jstart
        cpus = int(r['ncpus'])
        r['cpuseconds'] = elapsed.total_seconds() * cpus

        for grouping, _title in groupings:
            grouping.job(r)

        #print(','.join([r[k] for k in src.format] + ['%.2f' % (elapsed.total_seconds()/3600)]))

    rets = {}
    for grouping, title in groupings:
        ret = ''

        if partition is not None:
            ret += 'partition,{}\n'.format(partition)

        if restrict_to_partitions_nodes is not None:
            ret += 'restrict_to_partitions_nodes,{}\n'.format(','.join(restrict_to_partitions_nodes))

        if restrict_to_nodes_spec is not None:
            ret += 'restrict_to_nodes,"{}"\n'.format(restrict_to_nodes_spec)

        ret += 'selected_nodes,"{}"\n'.format(
            selected_nodes_spec or node_spec_from_list(list(selected_nodes))
        )
        ret += 'cores,{}\n'.format(cores)
        ret += 'max_seconds,{}\n'.format(maxseconds)
        ret += 'max_hours,{}\n'.format(maxseconds // 3600)
        ret += 'max_daily_hours,{}\n'.format(cores * 24)
        ret += '\n'



        indices = grouping.indices([])

        if len(indices) == 0:
            ret += '{}\n'.format(title[0])
            ret += '{}\n'.format(grouping)
        elif len(indices) == 1:
            ret += '{}\n'.format('*'.join(title))
            for i in indices[0]:
                ret += '{}\n'.format('%s,%s' % (i, grouping[i]))
        elif len(indices) == 2:
            y, x = indices
            ret += '{}\n'.format('*'.join(title))
            ret += ','
            ret += '{}\n'.format(','.join(x))
            for i in y:
                ret += i
                for j in x:
                    v = ''
                    if j in grouping[i]:
                        v = grouping[i][j][0]

                    ret += ',%s' % v

                ret += '\n'
        else:
            raise NotImplementedError

        rets['*'.join(title)] = ret

    return rets

def main(cfg_path='sreporting.conf'):
    if not os.path.isabs(cfg_path):
        cfg_path = config.find_config_file(__file__, cfg_path)

    parser = argparse.ArgumentParser(description='accounting report')
    parser.add_argument('report', metavar='REPORT', nargs='?',
                        default=None, help='use section [report:REPORT] '
                        'section in configuration file')
    parser.add_argument('-s', '--start', metavar='START_DATE',
                        default=None, help='account jobs from START_DATE')
    parser.add_argument('-e', '--end', metavar='END_DATE',
                        default=None, help='account jobs up to END_DATE')
    parser.add_argument('-g', '--grouping', metavar='GROUPING_SPEC',
                        default=None, help='grouping')
    parser.add_argument('-o', '--options', metavar='EXTRA_SACCT_OPTIONS',
                        default='', help='sacct extra options')

    parser.add_argument('--cfg', metavar='PATH',
                        default=cfg_path, help='config file (default=%s)' % cfg_path)

    args = parser.parse_args()

    rets = sreporting(args.cfg, args.report, grouping_specs=args.grouping, start=args.start, end=args.end,
                      extra_options=args.options.split())

    for ret in rets.values():
        print(ret)
