# slurm-accounting

Just a wrapper around sreport/sacct.

## Use

Examples:

Basic user and/or group accounting between 2 dates

```
./saccounting 2021-12-01
```

More configurable retporting tool, for use with a configuration file like this:
```
[general]

default_start=2019-03-25
query_grace=30-00:00:00

default_report=main

[report:main]
nodes = n[001-315],bigmem[01-04]
cores = 10368
grouping = group * cpu_hours, monthly*cpu_hours, cpu_hours

[report:all]
cores = 12160
grouping = group * cpu_hours, monthly*cpu_hours, cpu_hours

[report:longq]
partition = longq
cores = 11008

[report:visu]
partition = visu
nodes = visu[01-04]
cores = 128
grouping = monthly * cpu_hours , cpu_hours

```

```
sreporting visu
```

Generate yearly/monthly permanent reports:
```
./report
```
