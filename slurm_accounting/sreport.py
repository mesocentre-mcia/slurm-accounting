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

    jobs = src(start=query_start_date.strftime('%Y-%m-%dT%H:%M:%S'),
               end=query_end_date.strftime('%Y-%m-%dT%H:%M:%S'), partition=partition, nodes=nodes,
               states=['RUNNING'])

    n = 0
    cpuseconds = 0
    jobs.next()
    for r in jobs:
        if r['state'] == 'PENDING':
            continue
        jstart = parse_slurm_datetime(r['start'])
        jend = parse_slurm_datetime(r['end'])
        if start_date and jend and jend < start_date:
            continue

        if end_date and jstart and jstart > end_date:
            continue

        jstart = max(jstart, start_date)
        if jend is not None:
            jend = min(jend, end_date)
        else:
            jend = end_date

        n += 1
#        elapsed = parse_elapsed(r['elapsed'])
        elapsed = jend - jstart
        cpus = int(r['ncpus'])
        cpuseconds += elapsed.total_seconds() * cpus
        
        #print_(','.join([r[k] for k in src.format] + ['%.2f' % (elapsed.total_seconds()/3600)]))

    print_('jobs=%d' % n, 'cpuhours=%d' % (cpuseconds / 3600), end='')

    cores = cfg.get(report_section, 'cores', None)
    if cores is not None:
        cores = int(cores)
        duration = (end_date - start_date).total_seconds()
        maxseconds = cores * duration

        print_(' (%.1f%%)' % (100 * cpuseconds / maxseconds), end='')

    print_()

def main():
    parser = argparse.ArgumentParser(description='acconting report')
    parser.add_argument('report', metavar='REPORT', nargs='?',
                        default=None, help='use section [report:REPORT] '
                        'section in configuration file')
    parser.add_argument('-s', '--start', metavar='START_DATE',
                        default=None, help='account jobs from START_DATE')
    parser.add_argument('-e', '--end', metavar='END_DATE',
                        default=None, help='account jobs up to END_DATE')

    args = parser.parse_args()

    sreporting('curta.conf', args.report, start=args.start, end=args.end)
