# Slurm accounting

Rapports de comptabilité de Curta à destination du Comité des Utilisateurs.

Les rapports générés mensuellement sont mis dans `/gpfs/home/comut/slurm-accounting`

Certaines données ayant un caractère confidentiel, il vous ait demandé de ne pas les communiquer à l'extérieur.

## Répertoires

Les fichiers de comptabilité sont classées dans `reports/`

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

Exemples:
- `all-cpu_hours.csv`: rapport `all` (tous les jobs, tous les nœuds), métrique `cpu_hours` globale.
- `all-group*cpu_hours.csv`: rapport `all` (tous les jobs, tous les nœuds), métrique `cpu_hours` par groupe d'utilisateurs.
- `all-daily*cpu_hours.csv`: rapport `all` (tous les jobs, tous les nœuds), métrique `cpu_hours` par jour (groupement `daily`).

### Rapports

Il y a différents types de rapports. Ils sont définis dans le fichier `slurm-accounting/sreporting.conf`.

Extrait:

```
[general]

default_start=2019-03-25
query_grace=30-00:00:00

default_report=main


[report:main]
restrict_to_nodes = n[001-315],bigmem[01-04]

grouping = group * cpu_hours, monthly*cpu_hours, cpu_hours

[report:all]
grouping = group * cpu_hours, monthly*cpu_hours, cpu_hours

[report:longq]
partition = longq

```

On peut voir que le rapport `main` restreint les jobs ayant tourné sur les nœuds n001 à n315 et les bigmem.

Le rapport `all` contient tous les nœuds et toutes les partitions du cluster.

Autre extrait:


```


[report:preemptible-imb]
partition = preemptible
restrict_to_partitions_nodes=imb-resources

grouping = user * cpu_hours, monthly * cpu_hours, cpu_hours

[report:preemptible-i2m]
partition = preemptible
restrict_to_partitions_nodes=i2m-resources

grouping = user * cpu_hours, monthly * cpu_hours, cpu_hours

[report:preemptible]
partition = preemptible

grouping = user * cpu_hours, group * cpu_hours, cpu_hours
```

Les rapports `preemptible-imb` et `preemptible-i2m` concernent la partition préemptible mais sont limités aux jobs ayant tourné sur les nœuds normalement réservés à ces communautés (respectivement `imb-resources` et `i2m-resources`).

Ainsi, en combinant les informations de plusieurs rapports, on pourra par exemple:

- `preemptible-i2m-cpu_hours.csv` et `i2m-cpu_hours.csv`: connaître la charge totale des nœuds `i2m` en additionnant les valeurs et la comparer avec la charge due aux jobs préemptibles.
- `preemptible-imb-cpu_hours.csv` et  `preemptible-i2m-cpu_hours.csv`: connaître le nombre d'heures CPU récupérées par la communauté MCIA sur les ressources réservées grâce aux jobs préemptibles. En comparant avec `all-cpu_hours.csv`, on peut calculer la proportion que ça représente.
- etc...

### Format des fichiers CSV

Les fichiers CSV commencent tous par un entête.

On y lit:
- en première ligne, le nom du rapport (`all`), le groupement (`cpu_hours`), la date de début et de fin du rapport (mois de septembre).
- un deuxième bloc pour simplifier les calculs dans un tableur, qui contient
  - partition: optionnellement la partition des jobs sélectionnés pour ce rapport
  - restrict_to_partitions_nodes: optionnellement la restriction à la liste des partitions sur les nœuds desquelles les jobs sélectionnés pour le rapport ont tourné
  - restrict_to_nodes: optionnellement la restriction à la liste des nœuds sur lesquels les jobs sélectionnés pour le rapport ont tourné
  - selected_nodes: liste des nœuds effectivement sélectionnés pour ce rapport (en tenant compte de la partition et des restrictions)
  - le nombre de cœurs sélectionnés
  - le nombre seconds et d'heures CPU maximum dans la période (`cores * (end - start)`)
  - le nombre d'heures CPU par jour sur ces nœuds
  
Par exemple, pour un fichier `main-cpu_hours.csv`:

```
report=main,grouping=cpu_hours,start=2022-9-1,end=2022-10-1

restrict_to_nodes,"n[001-315],bigmem[01-04]"
selected_nodes,"bigmem[01-04],n[001-315]"
cores,10336
max_seconds,26790912000
max_hours,7441920
max_daily_hours,248064

...
```

Ou encore:

```
report=preemptible-imb,grouping=cpu_hours,start=2022-9-1,end=2022-10-1

partition,preemptible
restrict_to_partitions_nodes,imb-resources
selected_nodes,"n[337-364]"
cores,896
max_seconds,2322432000
max_hours,645120
max_daily_hours,21504

...
```

**Note:** La configuration des partitions pouvant changer (affectation de nœuds dans des partitions différentes, `preemptible` notamment), la précision des rapports, surtout les annuels, n'est pas absolue, les changements pouvant intervenir en mileu de période.

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

