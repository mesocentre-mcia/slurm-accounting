import subprocess
import os
import re

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
            print "Command:", " ".join([repr(e) for e in cmdlist])

        stdout = os.tmpfile()
        p = subprocess.Popen(cmdlist, stdout = stdout, stderr = subprocess.STDOUT)

        p.wait()

        if p.returncode != 0:
            print type(p.returncode)
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

if __name__ == '__main__':
    src = SreportCluster(verbose=True)

    print '\n'.join(src('2019-01-01'))
