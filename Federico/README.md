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

Corpus completo (`data/silver/authors`, 2.943.737 righe, 192 file, 34 MB). Sul cluster
Cloud Veneto, nella configurazione misurata come migliore (8 processi da 1 thread,
`k=8`): **3,9 s**. Con il default di partenza erano 41 s — vedi Benchmark.

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
| `--partitions N` | raggruppa i 192 file in N partizioni (default **8**, misurato — vedi Benchmark) |

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

| opzione | a cosa serve |
|---|---|
| `--ripetizioni N` | N passate **intere** della campagna, non N misure di fila dello stesso punto |
| `--k N` | le partizioni a cui si misura la curva sui worker (default: una per file) |
| `--thread N` | thread per worker. Con `--thread 1` la stessa campagna misura i **processi** |
| `--only CURVA` | `partizioni` o `worker` |
| `--worker N …` | solo questi numeri di worker, invece di tutta la curva |

### Processi contro thread, senza toccare `cluster.txt`

`SSHCluster` accende **un worker per voce** nella lista degli host, quindi ripetere la
lista mette due processi sulla stessa macchina; e `CORD19_HOSTS` scavalca `cluster.txt`
per la durata di un comando, senza lasciare niente da rimettere a posto.

```bash
W=ip_worker1,ip_worker2,ip_worker3,ip_worker4
CORD19_HOSTS="ip_scheduler,$W,$W" CORD19_WORKER_MEMORY_LIMIT=1.7GB python Federico/bench_affiliations.py ~/mapd-data/silver/authors     --only worker --worker 8 --thread 1 --k 16 --ripetizioni 3
```

Si scrive `"$W,$W"` e non `w1,w1,w2,w2,…`: così i primi *N* host sono *N* macchine
**diverse**, e i punti intermedi della curva non finiscono con i processi ammucchiati su
metà cluster. **`CORD19_WORKER_MEMORY_LIMIT` non è opzionale:** il default è una *frazione
della RAM di sistema per worker*, quindi due worker sulla stessa macchina si
impegnerebbero il 170% della sua memoria, e a fermarli sarebbe l'OOM killer del kernel —
non la nanny di Dask, che crede di avere la macchina tutta per sé. Su una *medium* da
4 GB, 1,7 GB è la metà dei 3,5 che prende un worker solo.

**L'ipotesi, scritta prima di misurare.** Il lavoro per riga è `re.sub` + `lower()` in
Python puro: il **GIL** — il lucchetto che lascia eseguire bytecode Python a un solo
thread per processo alla volta — non viene rilasciato da nessuno dei due. La lettura
Parquet invece sì, perché Arrow è C++. Quindi mi aspetto che i processi battano i thread,
ma **meno nettamente del 2.3.1**, dove il Map era interamente Python e i thread arrivarono
a *rallentare* (`T(4)/T(1) = 1,31`). Se `8×1` batte `4×2` a parità di otto core, la causa
è il GIL; se pareggiano, il collo di bottiglia è altrove.

### I risultati, sul cluster vero (5 × `cloudveneto.medium`: 4 worker da 2 core e 4 GB)

**165 misure, zero errori.** Le due curve obbligatorie hanno 6 passate intere, il
confronto processi/thread ne ha 3. `pandas` su un core della stessa VM: **14,15 s**
(dispersione 1,5% su 21 misure) — è il metro di paragone di tutto ciò che segue.

#### Curva 1 — tempo contro numero di partizioni

Tre configurazioni, e la terza è quella che risponde alla domanda sul default.

| k | 1 | 2 | 4 | **8** | 16 | 32 | 64 | 128 | 192 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 4 proc × 2 thread | 15,56 | 12,46 | 7,63 | 6,77 | 6,67 | 10,12 | 13,16 | 27,16 | 41,09 |
| 4 proc × 1 thread | 10,42 | 11,50 | 7,53 | **6,35** | 6,39 | 10,71 | 13,87 | 28,46 | 42,41 |
| 8 proc × 1 thread | 11,52 | 11,08 | 7,72 | **3,91** | 4,87 | 6,35 | 7,57 | 16,83 | 25,98 |

**La curva ha un minimo interno, e non è agli estremi.** A sinistra mancano processi a cui
dare lavoro: a `k=1` una partizione sola sta su un worker solo e gli altri guardano. A
destra ogni partizione aggiunge coordinamento — quattro riduzioni moltiplicate per il
numero di task — su 34 MB di dati che non ne hanno bisogno.

**`k=8` è il default scelto.** Vince o pareggia in tutte e tre le configurazioni: è il
minimo a `4×1`, è dentro il rumore del minimo a `4×2`, ed è il minimo **netto** a `8×1`,
dove `k=16` costa il 24% in più (3,91 contro 4,87 s, con dispersioni del 3–6%: sei
deviazioni standard, non rumore). A otto processi `k=8` è *una partizione per processo*,
ma la stessa regola a quattro processi darebbe `k=4`, che è peggio di `k=8` (7,53 contro
6,35): **è un numero misurato, non una formula**, ed è per questo che nel codice c'è una
costante e non un conto sul numero di worker.

**Il default di partenza era il punto peggiore della curva.** «Una partizione per file»
ricalca il layout su disco ed è la scelta trasparente, ma costa **6,5 volte** il minimo
sullo stesso cluster.

#### Curva 2 — tempo contro numero di worker

Misurata a `k=16`, cioè dentro il plateau, con 2 thread per worker:

| worker | 1 | 2 | 3 | 4 |
|---|---:|---:|---:|---:|
| secondi | 19,19 | 11,34 | 7,88 | 6,62 |
| speedup | 1,00 | 1,69 | 2,43 | **2,90** |
| efficienza | 1,00 | 0,84 | 0,81 | 0,72 |

La stessa curva misurata a `k=192` — il default di partenza — dà **2,48×**. Il punto in cui
si misura lo speedup non è neutro: dove il job è fatto quasi solo di coordinamento, il
parallelismo ha meno da mordere e la misura **sottostima**.

#### Processi contro thread, a parità di core

Tutte le configurazioni a `k=16`, per poterle confrontare:

| processi × thread | core del cluster | secondi | vs un core |
|---|---:|---:|---:|
| 1 × 2 | 2 | 19,19 | 0,73× |
| 2 × 2 | 4 | 11,34 | 1,24× |
| 3 × 2 | 6 | 7,88 | 1,79× |
| **4 × 1** | **4** | **6,67** | 2,12× |
| 4 × 2 | 8 | 6,62 | 2,13× |
| **8 × 1** | **8** | **4,97** | **2,84×** |

**Il secondo thread non vale niente; il secondo processo vale 1,34×.** Partendo da `4×1`,
raddoppiare i core dando un thread in più a ogni worker rende **1,01×** — zero. Raddoppiarli
dando processi in più rende **1,34×**. Stesso hardware aggiunto, due destini opposti.

**A parità di core i processi vincono sempre:** su 8 core `8×1` batte `4×2` di **1,33×**,
su 4 core `4×1` batte `2×2` di **1,70×**.

**Ed è il GIL.** Il GIL è il lucchetto che lascia eseguire bytecode Python a un solo thread
per processo alla volta; `re.sub` e `lower()`, che sono il lavoro per riga di questo task,
non lo rilasciano mai. Girando il conto di Amdahl al contrario, la frazione di lavoro che
due thread nello stesso processo riescono davvero a spartirsi è **~1,5%**: tutto il resto
è seriale dentro il processo. La lettura Parquet il lucchetto lo rilascerebbe, perché
Arrow è C++, ma a `k=8`–`16` è troppo poca cosa per vedersi.

L'ipotesi scritta prima di misurare diceva «i processi vincono, ma meno nettamente del
2.3.1»: là `8×1` batteva `4×2` di **2,03×**, qui di **1,33×**. Direzione e ordine di
grandezza giusti. La differenza è che nel 2.3.1 i thread **rallentavano** (`T(4)/T(1) =
1,31`), mentre qui sono **inerti** — e la conferma non è un punto solo: le curve `4×1` e
`4×2` si sovrappongono entro il ±6% su **tutti e nove** i valori di `k`, con metà dei core.
L'unica eccezione è `k=1`, dove il secondo thread fa danno (15,56 contro 10,42 s).

#### La conseguenza pratica, che è la parte scomoda

Il cluster ha 8 core. Con `4 worker × 2 thread`, che è il default di `SSHCluster` e la
configurazione con cui abbiamo misurato tutto all'inizio, **ne lavorano davvero 4**: metà
cluster sta a guardare. Per usarlo tutto servono **8 worker da 1 thread**, cioè un worker
per core — la ricetta con `CORD19_HOSTS` qui sopra.

Messi in fila i due pomelli: **8 processi × 1 thread a `k=8` → 3,91 s**, contro i **41,09 s**
della configurazione di partenza (4×2 thread, una partizione per file). Stesso hardware,
stesso codice: **10,5×**. E contro un core solo della stessa macchina, **3,62×**.

### Il confronto col 2.3.1, che è il motivo per cui questi benchmark stanno insieme

Prima che `chiave()` aggiungesse il lavoro per riga che la correttezza richiedeva, questo
task non faceva quasi nulla per riga: il suo punto migliore era **1,7× più lento** di un
core solo, e il default 46× più lento. Con quel lavoro in più, lo stesso codice sullo
stesso hardware è passato a **essere più veloce** di un core. Il 2.3.1 dice la stessa cosa
dall'altro lato: 3,56 GB, 12,4 milioni di paragrafi, 2,74× su 4 worker.

**Quel che decide se distribuire conviene non è la taglia del cluster, ma quanto lavoro c'è
per riga di dati** — e su questo task l'abbiamo visto cambiare in diretta.

I numeri del Mac (`LocalCluster`, 4 worker × 3 thread) restano nel CSV e raccontano la
stessa storia a un'altra scala: minimo a `k=8` (1,81 s), `k=192` a 14,25 s, pandas 3,93 s.
Non sono confrontabili con quelli della VM — i core delle *medium* sono molto più lenti —
ma il **rapporto** fra Dask al punto giusto e un core solo è lo stesso: 2,2× là, 2,1× qui.

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

- **Nessun notebook.** Se serve, importa `affiliations.py` come `word_count.ipynb` fa col
  suo modulo: mai codice duplicato fra i due.
- **`cluster.txt` elenca quattro macchine, quindi il default è `4 worker × 2 thread`** —
  cioè metà cluster fermo. Per i run veri conviene la lista raddoppiata con
  `CORD19_HOSTS`. Automatizzarlo in `cluster.py` sarebbe una modifica all'unico file
  condiviso fra i quattro task: **non fatta apposta**, va discussa col gruppo.
- **La curva sulle partizioni a 8 processi ha un minimo netto e non un plateau**, al
  contrario di quella a 4. Perché la forma cambi non è spiegato dai dati che abbiamo: è
  annotato, non capito.
- **Il fondo classifica degli istituti resta rumore** (67,6% di singleton): unirlo
  davvero vuol dire entity resolution, fuori scope per dichiarazione di
  `DATA_DICTIONARY.md`.
