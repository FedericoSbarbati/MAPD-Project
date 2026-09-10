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
      │  chiave            le grafie della stessa entità cadono sulla stessa chiave
      │  drop_duplicates   (paper, chiave): un paper conta UNA volta per entità
      ▼  value_counts      l'unico shuffle del job
  classifica  entità → paper        (+ entità → autori, la stessa cosa senza il dedup)
      │
      ▼  .compute() → pandas sul client (206 e 100.838 righe: piccole)
  CSV completi + barplot della cima e del fondo
```

Il conteggio **per paper** è la metrica primaria: senza il dedup un articolo con quaranta
co-autori italiani varrebbe quaranta volte uno con un autore solo. Il conteggio **per
autore** resta in tabella accanto, così l'inflazione da co-autori si legge come numero
invece di essere affermata — per gli Stati Uniti sono 50.351 paper contro 257.902 righe
autore, 5,1 autori per paper.

## La normalizzazione dei nomi, e il numero che la giustifica

Il `silver` normalizza gli istituti in modo **dichiaratamente leggero** (`norm_institution`
nella pipeline di conversione: NFKC, spazi collassati, punteggiatura tolta ai bordi) e
lascia la disambiguazione ai task — `DATA_DICTIONARY.md` avverte: *«group on
`institution_norm` with care»*. Senza una chiave di raggruppamento, in classifica finiscono
`‡ Rice University`, `e Jikei University School of Medicine` e `the University of Hong Kong`
accanto a `The University of Hong Kong`.

`chiave()` fa quattro cose, ognuna misurata sul corpus intero prima di essere aggiunta:

| regola (cumulativa) | istituti distinti | fusi |
|---|---:|---:|
| baseline, `institution_norm` del silver | 105.967 | — |
| minuscole | 103.570 | −2,3% |
| + via i caratteri non alfanumerici ai bordi (`‡ † △ ✉`) | 103.203 | −2,6% |
| + via l'articolo iniziale (`The`, `La`, …) | 102.008 | −3,7% |
| + accenti piegati (`université` = `universite`) | **100.838** | **−4,8%** |

**Non è cosmetica: cambia la risposta.** `The University of Hong Kong` era spezzata in
**sei grafie** (941 + 225 + 30 + 2 + 1 + 1) e passa dal **17° al 14° posto** con 1.188
paper; entrano nella top-20 The University of Sydney e University of Edinburgh. È
l'opposto di `is_reference_like` nel 2.3.1, dove la misura disse «non cambia niente» e la
regola fu tolta: qui la regola si paga da sola.

Due dettagli che vale la pena saper difendere:

- **L'etichetta consegnata è la grafia più frequente, non la chiave.** La chiave è
  minuscola e senza accenti, e serve solo a raggruppare; `The University of Hong Kong` esce
  scritto bene. Non costa nulla: `per_autore` si conta già sulla grafia, e le due Serie si
  ricuciono **sul client**, dove sono 10⁵ righe.
- **Il fold degli accenti** unisce da solo 1.062 gruppi, e i casi ispezionati sono tutti la
  stessa istituzione scritta due volte (`aix marseille université` / `… universite`,
  `åbo akademi` / `abo akademi`): l'estrazione dal PDF perde gli accenti a intermittenza.
  Nel repo c'è già lo stesso strumento — `accent_key()` nella conversione, per gli alias
  dei paesi.

**Sui paesi la chiave è un no-op, ed è misurato: 206 → 206.** Il `silver` li ha già
canonicalizzati con `country_converter`. Si applica lo stesso, perché un solo ramo di
codice è più facile da spiegare di due, e perché il no-op è esso stesso una misura.

## Il risultato

Corpus completo (`data/silver/authors`, 2.943.737 righe, 192 file, 34 MB), Mac con
`LocalCluster` a 4 worker: **13,4 s**.

| paese | paper | autori |    | istituto | paper | autori |
|---|---:|---:|---|---|---:|---:|
| United States | 50.351 | 257.902 | | University of California | 3.521 | 13.081 |
| China | 28.398 | 180.235 | | University of Oxford | 2.169 | 7.794 |
| United Kingdom | 21.461 | 95.983 | | Harvard Medical School | 1.971 | 6.712 |
| Italy | 16.864 | 103.302 | | University of Toronto | 1.857 | 5.189 |
| Germany | 12.218 | 56.607 | | Chinese Academy of Sciences | 1.763 | 7.741 |

I primi 10 paesi si prendono il **62,0%** di tutte le coppie (paper, paese).

**In fondo alla classifica le due domande non hanno la stessa qualità di risposta.**

- **Paesi: il fondo è un risultato vero.** Su 206 paesi, **16 compaiono con un solo
  paper** — Kosovo, Moldova, Montserrat, St. Helena, Tonga, Trinidad and Tobago… È la
  disuguaglianza che il testo chiede di far vedere.
- **Istituti: il fondo resta rumore, anche dopo la normalizzazione.** Su 100.838 istituti
  distinti, **68.126 (67,6%) compaiono una volta sola** — erano il 68,4% prima della
  chiave. Quello che resta non sono grafie diverse: sono **sigle** (`CAS` contro
  `Chinese Academy of Sciences`), **frammenti di indirizzo** (`and Environmental Health`) e
  nomi non latini. Per unirli servirebbe un dizionario, cioè entity resolution vera, che
  `DATA_DICTIONARY.md` mette esplicitamente fuori scope. Il fondo si stampa lo stesso, con
  questo numero accanto che è ciò che lo rende leggibile.

**L'invariante che dice che il rollup è giusto.** Il task raggruppa per conto proprio, e
sulla colonna **grezza** ottiene esattamente i rollup del `silver`: **284.042** coppie
(paper, paese) come `silver/paper_countries` e **517.911** (paper, istituto) come
`silver/paper_institutions`. Con la chiave, le coppie istituto scendono a **517.058**: le
853 di differenza *sono* l'effetto misurato della normalizzazione. Il controllo contro il
`silver` si fa **una volta** e sta qui — stessa decisione già presa per l'invariante
Map/Reduce del 2.3.1. Lo script stampa a ogni run la somma della colonna `papers`, che è il
numero da confrontare.

## Come si lancia

Dalla radice della repo. Dove gira lo decide `cluster.txt` (git-ignored), non il codice:
senza quel file parte un `LocalCluster`, con quel file un `SSHCluster` sui nodi elencati.

```bash
python Federico/affiliations.py                      # campione, cluster locale
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
  100.838 righe. Questo elimina il `client.run(os.makedirs, ...)` che al 2.3.1 è servito
  perché a scrivere erano i worker sui propri dischi (`PROJECT_CONTEXT.md` §8.12).
- **Un solo `dask.compute` per tutte e quattro le Serie**, così `silver/authors` si legge
  **una volta**: quattro `compute` separati rileggerebbero i 192 file quattro volte.

## Benchmark

Le due curve obbligatorie — tempo vs numero di **partizioni** e tempo vs numero di
**worker** — più una terza riga che qui è indispensabile: **lo stesso lavoro in pandas su
un core solo**, `chiave()` compresa.

```bash
python Federico/bench_affiliations.py --out /tmp/bench-2_3_2               # prova generale
python Federico/bench_affiliations.py ~/mapd-data/silver/authors --ripetizioni 3
```

**Una scelta diversa da `bench_word_count.py`, e la sua ragione.** Là vale «una riga del
CSV = un cluster nuovo», perché un worker che ha macinato milioni di stringhe trattiene
RSS per frammentazione glibc. Qui il job dura secondi su 34 MB: quel logoramento non può
avvenire, mentre accendere un `SSHCluster` (~40 s) sarebbe la quasi totalità della
campagna. Quindi **un cluster per numero di worker**, e dentro quel cluster tutte le `k`,
con **dieci secondi di pausa fra un cluster e il successivo**: lo scheduler nasce sempre
sulla porta 8786, e senza pausa il cluster dopo la trova occupata e non parte
(`OSError: [Errno 98] Address already in use` — visto sul cluster vero, non ipotizzato).
Il punto «cluster pieno, una partizione per file» appartiene a tutte e due le curve: nei
grafici l'asse x si legge dalle **colonne di stato** (`partizioni`, `worker`), mai
dall'etichetta `curva`.

Si cronometra il calcolo delle quattro classifiche, cioè il lavoro distribuito. La
scrittura di CSV e grafici è pandas sul client ed è identica in ogni punto: dentro il
cronometro sarebbe una costante additiva che schiaccia le curve.

### I risultati sul Mac (`LocalCluster`, 4 worker × 3 thread, corpus completo)

Tre passate intere della campagna, dispersione fra 1,1% e 5,2%.

| partizioni | 1 | 2 | 4 | **8** | 16 | 32 | 64 | 128 | 192 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| secondi | 4,39 | 3,59 | 2,22 | **1,81** | 1,95 | 3,17 | 4,08 | 8,82 | 14,25 |

| worker (a k=192) | 1 | 2 | 3 | 4 |
|---|---:|---:|---:|---:|
| secondi | 29,82 | 20,03 | 16,08 | 14,25 |
| speedup | 1,00 | 1,49 | 1,85 | **2,09** |

**pandas, un core: 3,93 s.**

1. **La curva sulle partizioni ha un minimo, e non è agli estremi.** A sinistra mancano
   processi a cui dare lavoro (a `k=1` un core solo legge tutto); a destra ogni partizione
   in più aggiunge coordinamento — quattro stadi di shuffle moltiplicati per il numero di
   task. Il minimo è a `k=8`, e `k=192` costa **7,9 volte** tanto.
2. **Il default «una partizione per file» è il punto peggiore della curva.** È trasparente
   (ricalca i file su disco) ma non è quello giusto: va scelto con la misura, e sul cluster
   vero va rifatta perché lì di mezzo c'è la rete.
3. **Al punto giusto, distribuire paga: 1,81 s contro i 3,93 s di un core, cioè 2,2×.**
   Con 12 thread, quindi l'efficienza resta bassa e il job è ancora dominato dal
   coordinamento — ma non è più *sotto* il baseline.

**Il confronto col 2.3.1, che è il motivo per cui questi due benchmark stanno insieme.**
Prima di `chiave()` questo task non faceva quasi nulla per riga: il suo punto migliore era
**1,7× più lento** di un core, e il default 46× più lento. Aggiungendo il lavoro per riga
che la correttezza richiedeva, lo stesso codice sullo stesso hardware è passato a **2,2×
più veloce**. È la stessa lezione che il 2.3.1 dà dall'altro lato — là 3,56 GB e 2,74× su
4 worker: **quel che decide se distribuire conviene non è la dimensione del cluster, ma
quanto lavoro c'è per riga di dati.** Qui l'abbiamo visto cambiare in diretta.

## Limiti noti

- **Copertura**: le affiliazioni coprono **214.799 paper** (paese) e **237.184** (istituto)
  su 970.836. «Meno rappresentato» significa quindi *fra i paper di cui vediamo
  l'affiliazione*, e non nel corpus intero.
- **Sigle e frammenti**: `chiave()` unisce le grafie, non i significati. `CAS` resta
  separato da `Chinese Academy of Sciences`, `and Environmental Health` resta un finto
  istituto: servirebbe un dizionario, cioè entity resolution (ROR/GRID), esclusa da
  `DATA_DICTIONARY.md` perché è un problema di ricerca.
- **Un'affiliazione multi-paese si risolve sul primo paese riconosciuto** (scelta della
  fase di conversione, `DATA_DICTIONARY.md`).
- **Lo 0,67% dei `country_raw` non nulli resta non risolto** e non entra in classifica.

## Aperto

- **La campagna sul cluster vero** (5 × medium, 4 worker) non è ancora girata: un run
  singolo là costa 39,6 s contro i 13,4 s del Mac, perché il coordinamento passa dalla rete.
- **Il default di `--partitions` va deciso su quei numeri, non su questi.** Sul Mac il
  minimo è `k=8`; con quattro macchine il compromesso è un altro.
- **La curva sui worker è misurata a `k=192`**, cioè al default, che è il punto peggiore:
  lo speedup di 2,09× è quindi una stima per difetto. Quando il `k` giusto sarà scelto
  sulla misura del cluster, la curva sui worker va rifatta lì.
- Nessun notebook: se serve, importa `affiliations.py` come `word_count.ipynb` fa col suo
  modulo. Mai codice duplicato fra i due.
