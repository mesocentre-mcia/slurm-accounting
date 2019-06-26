import subprocess
import os
import re
import datetime
import argparse

from six import print_

import config


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
            print_("Command:", " ".join([repr(e) for e in cmdlist]))

        stdout = os.tmpfile()
        p = subprocess.Popen(cmdlist, stdout = stdout, stderr = subprocess.STDOUT)

        p.wait()

        if p.returncode != 0:
            print_(type(p.returncode))
            raise subprocess.CalledProcessError(cmd=" ".join(cmdlist), returncode=p.returncode)

        stdout.seek(0)

        for l in stdout:
            yield self.output_filter(l)

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

    def __init__(self, verbose=False, remote_host=None):

        fmt = 'jobid', 'user', 'elapsed', 'ncpus', 'partition', 'nodelist', 'group', 'start', 'end', 'state'

        super(Sacct, self).__init__('sacct', ['-a', '--parsable2', '--noheader', '-X',
                                              '--format=%s' % ','.join(fmt)],
                                             Sacct.filter, verbose=verbose,
                                             remote_host=remote_host)

        self.format=fmt

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

            yield dict(zip(self.format, r))

def parse_elapsed(s):
    days = 0
    hspec = s
    if '-' in s:
        days, hspec = s.split('-',1)
        days = int(days)
    hours, minutes, seconds = map(int, hspec.split(':'))

    return datetime.timedelta(days=days, seconds=seconds, minutes=minutes, hours=hours)

def parse_slurm_datetime(s):
    if s == 'Unknown':
        return None
    return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")


def parse_slurm_date(s):
    return datetime.datetime.strptime(s, "%Y-%m-%d")

def print_date(d):
    return d.strftime("%Y-%m-%d")

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
        keys = self.bindict.keys()

        keys.sort(self.orderfunc)

        return keys

    def indices(self, indices):
        if len(indices) == 0: indices.append([])

        keys = list(set(indices[0] + self.key_list()))

        keys.sort(self.orderfunc)

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
        super(UserGroupingBin, self).__init__(lambda j: j['user'],
                                              lambda l, r: cmp(l, r), newbin)


class GroupGroupingBin(GroupingBin):
    def __init__(self, newbin):
        super(GroupGroupingBin, self).__init__(lambda j: j['group'],
                                               lambda l, r: cmp(l, r), newbin)

class StartGroupingBin(GroupingBin):
    def __init__(self, newbin):
        def hashfunc(j):
            return j['start'].split('T')[0]
        def orderfunc(l, r):
            ld = parse_slurm_date(l)
            rd = parse_slurm_date(r)

            return cmp(ld, rd)
        super(StartGroupingBin, self).__init__(hashfunc, orderfunc, newbin)


class DailyGroupingBin(GroupingBin):
    day = datetime.timedelta(days=1)

    def __init__(self, newbin, filling=(None, None)):
        def hashfunc(j):
            # start bin
            b = parse_slurm_datetime(j['start'])

            # end bin is date of end, ep1 is for correct ending of the while loop
            ep1 = parse_slurm_date(j['end'].split('T', 1)[0]) + self.day

            ret = []
            while b < ep1:
                ret.append(print_date(b))
                b += self.day

            return ret

        def orderfunc(l, r):
            ld = parse_slurm_date(l)
            rd = parse_slurm_date(r)

            return cmp(ld, rd)

        super(DailyGroupingBin, self).__init__(hashfunc, orderfunc, newbin)

        self.filling = filling

        self.__fill()

    def __fill(self):
        b, e = self.filling

        if b is None or e is None:
            return

        keys = self.hashfunc(dict(start=b, end=e))

        for k in keys[:-1]: # don't put a bin on last day

            if k not in self.bindict:
                self.bindict[k] = self.newbin.new()

    def new(self):
        return self.__class__(self.newbin.new(), self.filling)

    def job(self, job):
        keys = self.hashfunc(job)

        if not isinstance(keys, list):
            keys = [keys]

#        print_('job', [job[k] for k in ['jobid', 'ncpus', 'start', 'end', 'cpuseconds']])
        cpuseconds = 0
        days = []

        for k in keys:
            dayjob = job.copy()
            kd = parse_slurm_date(k)
            kdp1 = kd + self.day

            jstart = parse_slurm_datetime(dayjob['start'])
            if jstart  < kd:
                jstart = kd
                dayjob['start'] = print_datetime(jstart)

            jend = parse_slurm_datetime(dayjob['end'])
            if jend > kdp1:
                jend = kdp1
                dayjob['end'] = print_datetime(jend)

            elapsed = jend - jstart
            cpus = int(dayjob['ncpus'])
            dayjob['cpuseconds'] = elapsed.total_seconds() * cpus

            if dayjob['cpuseconds'] == 0:
                # don't register empty jobs
                continue

            cpuseconds += dayjob['cpuseconds']
            days.append((k, cpuseconds))

            if k not in self.bindict:
                self.bindict[k] = self.newbin.new()

            bin = self.bindict[k]
            bin.job(dayjob)


def sreporting(conf_file, report=None, start=None, end=None):

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

    report_section = 'report:' + (report or cfg.get('general', 'default_report'))

    src = Sacct(verbose=False)

    partition = cfg.get(report_section, 'partition', False) or None
    nodes = cfg.get(report_section, 'nodes', False) or None

    maxseconds = 1
    cores = cfg.get(report_section, 'cores', None)
    if cores is not None:
        cores = int(cores)
        duration = (end_date - start_date).total_seconds()
        maxseconds = cores * duration

    bins_dict = {
        'cpu_seconds':CpuSecondsBin,
        'cpu_hours':CpuHoursBin,
        #'percent':PercentBin,
        'job_count':JobCountBin,
        'user':UserGroupingBin,
        'group':GroupGroupingBin,
        'job_start':StartGroupingBin,
        'daily':lambda b: DailyGroupingBin(b, (print_datetime(start_date), print_datetime(end_date))),
    }

    grouping = CpuHoursBin()
    title = ['cpu_hours']

    grouping_spec = cfg.get(report_section, 'grouping', False) or None
    if grouping_spec is not None:
        groupings = [s.strip() for s in grouping_spec.split('*')]
        title = groupings[:]
        groupings.reverse()

        grouping = None
        for g in groupings:
            grouping = bins_dict[g](grouping)

    percent_grouping = PercentBin(CpuSecondsBin(), maxseconds)


    jobs = src(start=query_start_date.strftime('%Y-%m-%dT%H:%M:%S'),
               end=query_end_date.strftime('%Y-%m-%dT%H:%M:%S'), partition=partition, nodes=nodes,
               states=['RUNNING'])

    cpuseconds = 0
    jobs.next()
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
        cpuseconds += elapsed.total_seconds() * cpus
        r['cpuseconds'] = elapsed.total_seconds() * cpus

        grouping.job(r)
        percent_grouping.job(r)

        #print_(','.join([r[k] for k in src.format] + ['%.2f' % (elapsed.total_seconds()/3600)]))

    indices = grouping.indices([])

    if len(indices) == 0:
        print_(title[0])
        print_(grouping, end='')
        if cores is not None:
            print_('', percent_grouping, end='')
        print_()
    elif len(indices) == 1:
        print_(','.join(title))
        for i in indices[0]:
            print_('%s,%s' % (i, grouping[i]))
    elif len(indices) == 2:
        y, x = indices
        print_(','.join(title))
        print_(',', end='')
        print_(','.join(x))
        for i in y:
            print_(i, end='')
            for j in x:
                v = ''
                if j in grouping[i]:
                    v = grouping[i][j][0]
                print_(',%s' % v, end='')
            print_()
    else:
        raise NotImplementedError

def main():
    parser = argparse.ArgumentParser(description='accounting report')
    parser.add_argument('report', metavar='REPORT', nargs='?',
                        default=None, help='use section [report:REPORT] '
                        'section in configuration file')
    parser.add_argument('-s', '--start', metavar='START_DATE',
                        default=None, help='account jobs from START_DATE')
    parser.add_argument('-e', '--end', metavar='END_DATE',
                        default=None, help='account jobs up to END_DATE')

    args = parser.parse_args()

    sreporting('curta.conf', args.report, start=args.start, end=args.end)
