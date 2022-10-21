#!/usr/bin/env python3

import os
import os.path
import shutil

from six import print_

from datetime import datetime

from slurm_accounting.sreport import sreporting
from slurm_accounting import config


def yearly(cfg_path, report_dir, year):

    start_date = datetime(year, 1, 1)
    start = '{}-{}-{}'.format(start_date.year, start_date.month, 1)

    end_date = datetime(year + 1, 1, 1)
    end = '{}-{}-{}'.format(end_date.year, end_date.month, 1)

    year_dir = os.path.join(report_dir, str(year))

    simple_groupings = ['group*cpu_hours', 'cpu_hours']
    daily_groupings = ['daily*cpu_hours'] + simple_groupings
    user_groupings = ['user*cpu_hours'] + daily_groupings
    reports = {
        'all': user_groupings,
        'main': daily_groupings,
        'visu': simple_groupings,
        'gpu': simple_groupings,
        'imb': simple_groupings,
        'i2m': simple_groupings,
        'preemptible': daily_groupings,
        'preemptible-i2m': daily_groupings,
        'preemptible-imb': daily_groupings,
    }

    modified = False

    for report, groupings in reports.items():
        files = ['{}-{}.csv'.format(report, g) for g in groupings]
        if all([os.path.isfile(os.path.join(year_dir, f)) for f in files]):
            continue

        modified = True

        print_(start_date.year, report, groupings)

        if not os.path.isdir(year_dir):
            os.makedirs(year_dir)

        rets = sreporting(
            cfg_path, report=report,
            grouping_specs=','.join(groupings),
            start=start,
            end=end
        )

        for k, v in rets.items():
            report_path = os.path.join(year_dir, '{}-{}.csv'.format(report, k))

            with open(report_path, 'w') as f:
                f.write('report={},grouping={},start={},end={}\n'.format(
                    report, k, start, end
                ))
                f.write('\n')
                f.write(v)

    if modified:
        shutil.copy(cfg_path, year_dir)


def monthly(cfg_path, report_dir, year, month):

    start_date = datetime(year, month, 1)
    start = '{}-{}-{}'.format(start_date.year, start_date.month, 1)

    if month == 12:
        end_date = datetime(year + 1, 1, 1)
    else:
        end_date = datetime(year, month + 1, 1)
    end = '{}-{}-{}'.format(end_date.year, end_date.month, 1)

    month_dir = os.path.join(report_dir, str(year), '{:02d}'.format(month))

    simple_groupings = ['group*cpu_hours', 'cpu_hours']
    daily_groupings = ['daily*cpu_hours'] + simple_groupings
    user_groupings = ['user*cpu_hours'] + daily_groupings
    reports = {
        'all': user_groupings,
        'main': daily_groupings,
        'visu': simple_groupings,
        'gpu': simple_groupings,
        'imb': simple_groupings,
        'i2m': simple_groupings,
        'preemptible': simple_groupings,
        'preemptible-i2m': daily_groupings,
        'preemptible-imb': daily_groupings,
    }

    modified = False

    for report, groupings in reports.items():

        files = ['{}-{}.csv'.format(report, g) for g in groupings]

        if all([os.path.isfile(os.path.join(month_dir, f)) for f in files]):
            continue

        modified = True

        print_(start_date.year, start_date.month, report, groupings)

        if not os.path.isdir(month_dir):
            os.makedirs(month_dir)

        rets = sreporting(
            cfg_path, report=report,
            grouping_specs=','.join(groupings),
            start=start,
            end=end
        )

        for k, v in rets.items():
            report_path = os.path.join(
                month_dir,
                '{}-{}.csv'.format(report, k)
            )

            with open(report_path, 'w') as f:
                f.write('report={},grouping={},start={},end={}\n'.format(
                    report, k, start, end
                ))
                f.write('\n')
                f.write(v)

    if modified:
        shutil.copy(cfg_path, month_dir)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Gather cluster Slurm accounting reports'
    )

    parser.add_argument(
        '--cfg', metavar='PATH',
        default=config.find_config_file(__file__, 'sreporting.conf'),
        help='config file'
    )

    parser.add_argument('-d', '--destination-dir', metavar='DIR',
                        default=os.path.join(os.getcwd(), 'reports'),
                        help='Reports destination directory')

    args = parser.parse_args()

    report_dir = args.destination_dir

    today = datetime.today()
    year_start = 2021
    year_end = today.year

    for year in range(year_start, year_end + 1):
        month_end = today.month - 1
        if year < year_end:
            yearly(args.cfg, report_dir, year)

            month_end = 12

        for month in range(1, month_end + 1):
            monthly(args.cfg, report_dir, year, month)


if __name__ == '__main__':
    main()
