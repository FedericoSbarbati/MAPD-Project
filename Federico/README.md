# Federico — 2.3.2 Paesi e istituti più e meno rappresentati

Il testo chiede di *«identify the countries that have been the most and least active in
this research field»* a partire dalla **country of affiliation of the authors**, e la
stessa cosa per l'**affiliation institute**. Aggiunge che *«it's possible (not required) to
pass from an RDD/bag to a DataFrame structure for this task»*: qui si usa il **DataFrame**
di Dask, che è anche ciò che distingue questo task dal 2.3.1 a Bag.

| file | cos'è |
|---|---|
| `affiliations.py` | l'implementazione: funzioni pure + un `main()` per lanciarla da terminale |
| `bench_affiliations.py` | la campagna di benchmark: un comando, e scrive un CSV riga per riga |

Dipendenza condivisa col resto del progetto: `cluster.py` alla radice, che accende il
cluster e stampa com'è fatto. Nessun ambiente Python separato.

## L'algoritmo

```
silver/authors ──► DataFrame (cord_uid, country, institution_norm)
      │
      │  dropna            un autore senza affiliazione non vota
      │  drop_duplicates   (paper, entità): un paper conta UNA volta per entità
      ▼  value_counts      l'unico shuffle del job
  classifica  entità → paper        (+ entità → autori, la stessa cosa senza il dedup)
      │
      ▼  .compute() → pandas sul client (206 e 105.967 righe: piccole)
  CSV completi + barplot della cima e del fondo
```

Il conteggio **per paper** è la metrica primaria: senza il dedup un articolo con quaranta
co-autori italiani varrebbe quaranta volte uno con un autore solo. Il conteggio **per
autore** resta in tabella accanto, così l'inflazione da co-autori si legge come numero
invece di essere affermata — per gli Stati Uniti sono 50.351 paper contro 257.902 righe
autore, 5,1 autori per paper.

## Il risultato

Corpus completo (`data/silver/authors`, 2.943.737 righe, 192 file, 34 MB), Mac con
`LocalCluster` a 4 worker: **12,4 s**.

| paese | paper | autori |    | istituto | paper | autori |
|---|---:|---:|---|---|---:|---:|
| United States | 50.351 | 257.902 | | University of California | 3.505 | 13.047 |
| China | 28.398 | 180.235 | | University of Oxford | 2.159 | 7.748 |
| United Kingdom | 21.461 | 95.983 | | Harvard Medical School | 1.969 | 6.707 |
| Italy | 16.864 | 103.302 | | University of Toronto | 1.846 | 5.162 |
| Germany | 12.218 | 56.607 | | Chinese Academy of Sciences | 1.758 | 7.722 |

I primi 10 paesi si prendono il **62,0%** di tutte le coppie (paper, paese).

**In fondo alla classifica le due domande non hanno la stessa qualità di risposta.**

- **Paesi: il fondo è un risultato vero.** Su 206 paesi, **16 compaiono con un solo
  paper** — Kosovo, Moldova, Montserrat, St. Helena, Tonga, Trinidad and Tobago… È la
  disuguaglianza che il testo chiede di far vedere.
- **Istituti: il fondo è rumore di normalizzazione, e va detto.** Su 105.967 istituti
  distinti, **72.499 (68,4%) compaiono una volta sola**, e il fondo classifica è fatto di
  `the University of Hong Kong` accanto a `The University of Hong Kong`, di frammenti di
  indirizzo (`and Environmental Health`) e di stringhe con marcatori tipografici
  (`†Thailand Ministry of Public Health`). `DATA_DICTIONARY.md` lo dichiara: la
  normalizzazione degli istituti è **leggera**, e la disambiguazione vera (ROR/GRID) è un
  problema di ricerca lasciato fuori. Il fondo si stampa lo stesso — con questo numero
  accanto, che è ciò che lo rende leggibile.

**L'invariante che dice che il rollup è giusto.** Il task raggruppa per conto proprio, ma
il `silver` contiene già gli stessi rollup calcolati in fase di conversione. I due devono
coincidere, e coincidono: **284.042** coppie (paper, paese) come `silver/paper_countries`,
**517.911** coppie (paper, istituto) come `silver/paper_institutions`. Sono le due somme
della colonna `papers`, quindi il controllo è **gratis a ogni run** e lo script le stampa.

## Come si lancia

Dalla radice della repo. Dove gira lo decide `cluster.txt` (git-ignored), non il codice:
senza quel file parte un `LocalCluster`, con quel file un `SSHCluster` sui nodi elencati.

```bash
python Federico/affiliations.py                     # campione, cluster locale
python Federico/affiliations.py data/silver/authors  # corpus completo
python Federico/affiliations.py ~/mapd-data/silver/authors --out ~/mapd-out/2_3_2
```

| opzione | effetto |
|---|---|
| `--out DIR` | dove scrivere (default `~/mapd-out/2_3_2`, **fuori dalla repo**) |
| `--top N` | quante entità in cima e in fondo alla classifica (default 20) |
| `--partitions N` | raggruppa i 192 file in N partizioni (default: una per file) |

In uscita, per ciascuna delle due classifiche: il CSV completo (`*_ranking.csv`, con
`entity`, `papers`, `authors`) e i due grafici `*_top.png` / `*_bottom.png`.

## Le decisioni, e il perché

- **Si legge `silver/authors`, non i rollup già pronti** `silver/paper_countries` /
  `silver/paper_institutions`. Contare un paese una volta per paper invece che una volta
  per autore **è una decisione di analisi**, e il `silver` per regola non ne prende
  (`DECISIONI.md`): appartiene al task. In più i rollup pronti sono 2,7 e 8,2 MB già
  aggregati — leggerli vorrebbe dire consegnare un `value_counts` e nient'altro. Fatto
  così, invece, i rollup del `silver` diventano il **controllo** del risultato.
- **DataFrame e non Bag**, come il testo suggerisce per questo task. In pratica serve a
  qualcosa: `drop_duplicates` e `value_counts` hanno uno shuffle parallelo, mentre in Bag
  ogni operazione tipo-groupby collassa in **una sola partizione** (`CLAUDE.md`, regola 8).
- **`value_counts` riduce in una sola partizione, e qui va bene.** È l'opposto della scelta
  del 2.3.1, dove il reduce va spezzato in 16: là le chiavi distinte sono 6 milioni di
  parole, qui sono 206 paesi e 10⁵ istituti. La stessa operazione, due dimensionamenti
  diversi, perché è diverso lo spazio delle chiavi.
- **Le partizioni si ottengono raggruppando i file in lettura** (`dd.from_map` su gruppi),
  non con un `repartition` a valle: quello leggerebbe comunque alla granularità dei file e
  metterebbe la ricucitura dentro il cronometro.
- **Il risultato si scrive dal client**, non con `to_parquet` sui worker: sono 206 e
  105.967 righe. Questo elimina il `client.run(os.makedirs, ...)` che al 2.3.1 è servito
  perché a scrivere erano i worker sui propri dischi (`PROJECT_CONTEXT.md` §8.12).
- **Un solo `dask.compute` per tutte e quattro le Serie**, così `silver/authors` si legge
  **una volta**: quattro `compute` separati rileggerebbero i 192 file quattro volte.

## Benchmark

Le due curve obbligatorie — tempo vs numero di **partizioni** e tempo vs numero di
**worker** — più una terza riga che qui è indispensabile: **lo stesso lavoro in pandas su
un core solo**.

```bash
python Federico/bench_affiliations.py --out /tmp/bench-2_3_2               # prova generale
python Federico/bench_affiliations.py ~/mapd-data/silver/authors --ripetizioni 3
```

**Una scelta diversa da `bench_word_count.py`, e la sua ragione.** Là vale «una riga del
CSV = un cluster nuovo», perché un worker che ha macinato milioni di stringhe trattiene
RSS per frammentazione glibc. Qui il job dura secondi su 34 MB: quel logoramento non può
avvenire, mentre accendere un `SSHCluster` (~40 s) sarebbe la quasi totalità della
campagna. Quindi **un cluster per numero di worker**, e dentro quel cluster tutte le `k`.
Il punto «cluster pieno, una partizione per file» appartiene a tutte e due le curve: nei
grafici l'asse x si legge dalle **colonne di stato** (`partizioni`, `worker`), mai
dall'etichetta `curva`.

Si cronometra il calcolo delle quattro classifiche, cioè il lavoro distribuito. La
scrittura di CSV e grafici è pandas sul client ed è identica in ogni punto: dentro il
cronometro sarebbe una costante additiva che schiaccia le curve.

### I risultati sul Mac (`LocalCluster`, 4 worker × 3 thread, corpus completo)

Tre passate intere della campagna, dispersione fra 0,6% e 3,9%.

| partizioni | 1 | 2 | **4** | 8 | 16 | 32 | 64 | 128 | 192 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| secondi | 0,66 | 0,57 | **0,44** | 0,47 | 0,80 | 2,16 | 2,98 | 7,64 | 12,35 |

| worker (a k=192) | 1 | 2 | 3 | 4 |
|---|---:|---:|---:|---:|
| secondi | 24,75 | 17,83 | 14,31 | 12,35 |
| speedup | 1,00 | 1,39 | 1,73 | **2,00** |

**pandas, un core: 0,27 s.**

Tre cose da leggere insieme:

1. **La curva sulle partizioni non ha un minimo interno: cresce.** Da `k=4` a `k=192` il
   tempo fa **×28**. Non è memoria, non è squilibrio: è il costo per task, moltiplicato
   per quattro stadi di shuffle. Con 34 MB di dati, ogni partizione in più aggiunge più
   coordinamento di quanto calcolo tolga.
2. **Più worker aiutano — ma aiutano a smaltire l'overhead.** A `k=192` quattro worker
   fanno 2,00× rispetto a uno; e tutto quel lavoro si evita passando a `k=4`, che con gli
   stessi quattro worker costa 0,44 s.
3. **Il punto migliore di Dask è comunque 1,7× più lento di pandas su un core** (0,44
   contro 0,27), e il default «una partizione per file» è **46×** più lento. Questo task
   sta **sotto la soglia** in cui distribuire paga.

**E questo è il risultato, non un fallimento della misura.** Il 2.3.2 gira su 34 MB e
2,9 milioni di righe; il 2.3.1, sullo stesso cluster, gira su 3,56 GB e 12,4 milioni di
paragrafi e rende 2,74× su 4 worker. Due task, la stessa infrastruttura, lo stesso codice
di avvio: uno scala, l'altro no, e la differenza è la quantità di lavoro per riga di dati.
Le affiliazioni non hanno una versione più grande — esistono solo nel ramo `pdf_json`
(`DATA_DICTIONARY.md`), e sono il 45,70% delle righe autore per il paese, il 51,68% per
l'istituto.

## Limiti noti

- **Copertura**: le affiliazioni coprono **214.799 paper** (paese) e **237.184** (istituto)
  su 970.836. «Meno rappresentato» significa quindi *fra i paper di cui vediamo
  l'affiliazione*, e non nel corpus intero.
- **Istituti non disambiguati**: normalizzazione leggera, 68,4% di singleton. La cima della
  classifica è affidabile, il fondo va letto come rumore.
- **Un'affiliazione multi-paese si risolve sul primo paese riconosciuto** (scelta della
  fase di conversione, `DATA_DICTIONARY.md`).
- **Lo 0,67% dei `country_raw` non nulli resta non risolto** e non entra in classifica.

## Aperto

- **Il default di `--partitions` va deciso sul cluster vero, non sul Mac.** Qui il minimo è
  `k=4`, ma con quattro macchine e la rete di mezzo il compromesso è un altro: con meno
  partizioni che worker qualcuno resta fermo. Si misura là e poi si sceglie.
- Nessun notebook: se serve, importa `affiliations.py` come `word_count.ipynb` fa col suo
  modulo. Mai codice duplicato fra i due.
