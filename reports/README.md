# Slurm accounting

Rapports de comptabilité de Curta à destination du Comité des Utilisateurs.

Les rapports générés mensuellement sont mis dans `/gpfs/home/comut/slurm-accounting`

## Répertoires

Les fichiers de comptabilité sont classées das `reports/`

- par année:
  - `2019/*.csv`
  - `2020/*.csv`
  - etc...
- par mois:
  - `2020/01/*.csv`
  - `2020/02/*.csv`
  - `2020/03/*.csv`
  - etc...

## Fichiers CSV

Plusieurs rapports sont situés dans chaque répertoire.

Leur nom dépend du type de rapport et du groupement de la métrique (le nombre d'heures CPU `cpu_hours`): `<rapport>-[<groupement>*]cpu_hours.csv`.

Esemples:
- `all-cpu_hours.csv`: rapport `all` (tous les jobs, tous les nœuds), métrique `cpu_hours` globale.
- `all-group*cpu_hours.csv`: rapport `all` (tous les jobs, tous les nœuds), métrique `cpu_hours` par groupe d'utilisateurs.
- `all-daily*cpu_hours.csv`: rapport `all` (tous les jobs, tous les nœuds), métrique `cpu_hours` par par jour (groupement `daily`).

### Rapports

Il y a différents types de rapports. Ils sont définis dans le fichier `slurm-accounting/sreporting.conf`.

Extrait:

```
[general]

default_start=2019-03-25
query_grace=30-00:00:00

default_report=main

[report:main]
nodes = n[001-315],bigmem[01-04]
cores = 10336
grouping = group * cpu_hours, monthly*cpu_hours, cpu_hours

[report:all]
cores = 12160
grouping = group * cpu_hours, monthly*cpu_hours, cpu_hours

```

On peut voir que le rapport `main` restreint les jobs ayant tourné sur les nœuds n001 à n315 et les bigmem. Ca représente 10336 cœurs (valeur informative à modifier si on change les nœuds sur lesquels le rapport est fait).

Le rapport `all` contient tous les nœuds et toutes les partitions du cluster.

Autre extrait:


```

[report:preemptible]
partition = preemptible
nodes=n[001-364],gpu[01-04]
cores = 11776

grouping = user * cpu_hours, group * cpu_hours, cpu_hours

[report:preemptible-imb]
partition = preemptible
nodes=n[337-364]
cores = 896

grouping = user * cpu_hours, monthly * cpu_hours, cpu_hours

[report:preemptible-i2m]
partition = preemptible
nodes=n[316-336]
cores = 672

grouping = user * cpu_hours, monthly * cpu_hours, cpu_hours
```

On peut y voir que le rapport `preemptible` comprend tous les jobs de la partition `preemptible` ayant tourné sur les nœuds compute ainsi que les nœuds gpu (en fait tous les nœuds accessibles à la partition, dans ce cas).

Les rapports `preemptible-imb` et `preemptible-i2m` concernent aussi la même partition mais sont limités aux jobs ayant tourné sur les nœuds normalement réservés à ces communautés.

Ainsi, en combinant les informations de plusieurs rapports, on pourra par exemple:

- `preemptible-i2m-cpu_hours.csv` et `i2m-cpu_hours.csv`: connaître la charge totale des nœuds `i2m` en additionnant les valeurs et la comparer avec la charge due aux jobs préemptibles.
- `preemptible-imb-cpu_hours.csv` et  `preemptible-i2m-cpu_hours.csv`: connaître le nombre d'heures CPU récupérées par la communauté MCIA sur les ressources réservées grâce aux jobs préemptibles. En comparant avec `all-cpu_hours.csv`, on peut calculer la proportion que ça représente.
- etc...

**Note:** Le fichier `sreporting.conf` pouvant changer (affectation de nœuds dans des partitions différentes), on copie la version utilisée au moment de générer le rapport dans le répertoire concerné. Ca signifie aussi que la précision des rapports, surtout les annuels, n'est pas absolue, les changements pouvant intervenir en mileu de période.

### Format des fichiers CSV

Les fichiers CSV commencent tous par un entête.

Par exemple, pour un fichier `all-cpu_hours.csv`:

```
report=all,grouping=cpu_hours,start=2022-9-1,end=2022-10-1

cores,12160
max_seconds,31518720000
max_hours,8755200
max_daily_hours,291840

...
```

On y lit:
- en première ligne, le nom du rapport (`all`), le groupement (`cpu_hours`), la date de début et de fin du rapport (mois de septembre).
- un deuxième bloc pour simplifier les calculs dans un tableur, qui contient
  - le nombre de cœurs (voir la définition du rapport dans `sreporting.conf`)
  - le nombre seconds et d'heures CPU maximum dns la période (`cores * (end - start)`)
  - le nombre d'heures CPU par jour sur ces nœuds
  
Ensuite, selon le type de groupement, on a une ou plusieurs lignes.

Par exemple, pour `grouping=cpu_hours`:

```
cpu_hours
6562203
```

Pour `grouping=daily*cpu_hours`:

```
daily*cpu_hours
2022-09-01,246523
2022-09-02,233608
2022-09-03,225034
2022-09-04,225451
...
```


Pour `grouping=group*cpu_hours`:

```
group*cpu_hours
celia,355881
cenbg,192436
epoc,0
i2m,167730
icmcb,334699
imb,171414
ims,3
...
```

