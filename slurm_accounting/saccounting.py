from . import sreport
from . import date

import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v','--verbose', action='store_true', default=False, help='be verbose')
    parser.add_argument('startdate', metavar='START', type=date.SlurmDate, nargs='?',
                        help='start date')
    parser.add_argument('enddate', metavar='END', type=date.SlurmDate, nargs='?',
                        help='end date')
    parser.add_argument('-t', '--type', choices=('u', 'g', 'ug'), default='ug', metavar='TYPE',
                        help='report type (u, g or ug)')
    parser.add_argument('-n','--no-header', dest='header', action='store_false', default=True,
                        help='don\'t print header')

    args = parser.parse_args()

    if args.startdate is not None and args.enddate is not None:
        if args.startdate > args.enddate:
            raise ValueError('start date posterior to end date: %s > %s' % (args.startdate,
                                                                            args.enddate))

    skip_users = 'u' not in args.type
    skip_groups = 'g' not in args.type

    r = sreport.SreportCluster(include_header=args.header, skip_users=skip_users,
                               skip_groups=skip_groups, verbose=args.verbose)

    if args.header:
        print 'Period: start=%s end=%s' % (args.startdate, args.enddate)
        print

    for row in r(args.startdate, args.enddate):
        print ','.join(row)
