
import re
import datetime

class SlurmDate:
    __spec_re = re.compile('^(?P<year>[0-9]{4})(-(?P<month>[0-9]{2})(-(?P<day>[0-9]{2}))?)?$')
    __slurm_fmt = '%Y-%m-%dT%H:%M'

    def __init__(self, specification):
        self.specification = specification
        self.date = datetime.datetime(*self.canonicalize(self.specification))

    def canonicalize(self, specification):
        m = self.__spec_re.match(specification)

        if m is None:
            raise ValueError('Invalid SlurmDate format: should be YYY[-MM[-DD]], got \'%s\'' %
                             specification)

        year = int(m.group('year'))
        month = m.group('month')
        if month is None:
            month = 1
        else:
            month = int(month)

        day = m.group('day')
        if day is None:
            day = 1
        else:
            day = int(day)

        return year, month, day

    def to_slurm(self):
        return self.date.strftime(self.__slurm_fmt)

    def __str__(self):
        return self.to_slurm()

    def __repr__(self):
        return 'SlurmDate(\'%s\')' % self.specification

    def __lt__(self, other):
        return self.date < other.date
    def __le__(self, other):
        return self.date <= other.date
    def __gt__(self, other):
        return self.date > other.date
    def __ge__(self, other):
        return self.date >= other.date
    def __eq__(self, other):
        return self.date == other.date
    def __ne__(self, other):
        return self.date != other.date
