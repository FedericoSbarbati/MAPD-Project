# Giulia - Word Count CORD-19

Cartella personale per il task di word count. E' pensata per evitare conflitti
con il lavoro degli altri: notebook, script e ambiente sono qui dentro, mentre i
dati restano nella struttura comune del progetto.

## Contenuto

- `notebooks/word_count_giulia.ipynb`: sviluppo e benchmark locale su subset.
- `notebooks/word_count_giulia_cloudveneto.ipynb`: run distribuito su CloudVeneto.
- `scripts/word_count_dask.py`: implementazione MapReduce/Dask riusabile.
- `scripts/run_word_count_cloudveneto.py`: avvio di `SSHCluster` e run completo.
- `scripts/task_2_3_2_affiliation_representation.py`: paesi e istituti meglio/peggio rappresentati.
- `scripts/setup_mapd_env.sh`: crea l'ambiente Python isolato in `Giulia/.venv`.
- `requirements.txt` e `environment.yml`: dipendenze.

## Dati attesi

Gli script non committano e non generano dati dentro Git. Si aspettano il dataset
pulito in:

```text
data/silver/paragraphs
```

Su CloudVeneto il path completo atteso e':

```text
/data/MAPD-Project/data/silver/paragraphs
```

Gli output vengono scritti in:

```text
Giulia/reports/
```

Quella cartella e' ignorata da Git.

## Task 2.3.2: paesi e istituti

Da root della repo:

```bash
python Giulia/scripts/task_2_3_2_affiliation_representation.py
```

Su CloudVeneto, se usi l'ambiente creato nella cartella Giulia:

```bash
/data/MAPD-Project/Giulia/.venv/bin/python \
  /data/MAPD-Project/Giulia/scripts/task_2_3_2_affiliation_representation.py
```

Lo script legge:

```text
data/silver/paper_countries
data/silver/paper_institutions
```

e scrive:

```text
Giulia/reports/task_2_3_2_affiliations/
```

Metriche principali:

- `paper_count` per paese: numero di paper distinti in cui compare almeno
  un'affiliazione di quel paese;
- `paper_count` per istituto: numero di paper distinti in cui compare almeno
  un'affiliazione di quell'istituto.

Per vedere anche i conteggi per riga-autore, opzionali e meno robusti perche'
possono sovrappesare paper con molti coautori, aggiungi:

```bash
--include-author-counts
```

## Esecuzione locale

Da root della repo:

```bash
cd Giulia
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m ipykernel install --user --name mapd-covid-giulia --display-name "Python (mapd-covid-giulia)"
```

Poi aprire:

```text
Giulia/notebooks/word_count_giulia.ipynb
```

e selezionare il kernel `Python (mapd-covid-giulia)`.

## Esecuzione su CloudVeneto

Da `/data/MAPD-Project` sulla VM scheduler:

```bash
bash Giulia/scripts/setup_mapd_env.sh
```

Se i worker non vedono `/data`, montare lo storage condiviso:

```bash
bash scripts/cluster_storage_up.sh 10.67.22.206 10.67.22.53
```

Poi aprire:

```text
Giulia/notebooks/word_count_giulia_cloudveneto.ipynb
```

Configurazione di default:

- scheduler/notebook: `10.67.22.118`;
- worker: `10.67.22.206`, `10.67.22.53`;
- `1` worker per VM;
- `1` thread per worker;
- `2500MiB` per worker;
- output in `Giulia/reports/giulia_word_count_cloudveneto`.

La VM scheduler non viene usata come worker di default, cosi' resta memoria per
notebook, scheduler Dask, SSH e sistema operativo.
