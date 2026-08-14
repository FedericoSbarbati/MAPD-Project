# Giulia — 2.3.1 Word count distribuito

Conteggio delle parole sul full text di CORD-19, con Dask **Bag** (la struttura
raccomandata dal testo: «we recommend utilizing the RDD/Bag data structure»).

| file | cos'è |
|---|---|
| `word_count.py` | l'implementazione: funzioni pure + un `main()` per lanciarla da terminale |
| `word_count.ipynb` | il notebook, che **importa** il modulo — nessun codice duplicato, nessun sottoprocesso |
| `bench_word_count.py` | la campagna di benchmark: **un comando, e gira da sola** — vedi sotto |

Dipendenze condivise col resto del progetto: `cluster.py` alla root, che accende il
cluster e stampa com'è fatto. Nessun ambiente Python separato: si usa quello del progetto
(`mapd-covid` sul Mac, `~/pyvenv` sulla VM).

## L'algoritmo

```
silver/paragraphs ──► Bag di (cord_uid, text)
      │
      │  Map      per ogni documento D, le coppie (w, cp(w))
      │            → conteggio LOCALE dentro la partizione (combiner):
      ▼              nessuno shuffle, partizioni invariate
  doc_counts = ((cord_uid, word), cp)
      │
      │  Reduce   per ogni parola w, c(w) = Σ cp(w) sui documenti
      ▼            → foldby sulla parola: l'unico shuffle del job
  global_counts = (word, c)
```

## Il risultato

Run completo sul corpus scaricato dalla VM (12.445.234 paragrafi), su Mac con
`LocalCluster` a 3 worker × 4 thread: **1.128 s**, 785.753.529 occorrenze contate,
vocabolario di **6.037.808** parole distinte.

```
   4.403.513  patients
   3.911.578  covid-19      ← 2ª parola dell'intero corpus
   3.280.997  study
   2.980.002  data
   2.359.949  using
   ...
   1.488.035  sars-cov-2    ← 18ª
```

Vale la pena fermarsi su `covid-19` e `sars-cov-2`: con un pattern `[a-z]+` ingenuo
**nessuna delle due esisterebbe**, spezzate in `covid`+`19` e `sars`+`cov`+`2` con le
cifre buttate via. La seconda parola più frequente del corpus è quella che la
tokenizzazione più ovvia fa sparire.

Il vocabolario cresce come previsto dalla legge di Heaps, ed è il motivo per cui ridurre
sulla parola è sostenibile mentre ridurre su `(documento, parola)` non lo è:

| dati | vocabolario |
|---|---:|
| campione | 177.534 |
| 10% del corpus | 2.323.613 |
| corpus completo | 6.037.808 (70,7 MB di stringhe) |

Dieci volte i dati per 2,6 volte il vocabolario. Il 56,6% delle parole compare **una
volta sola**: la coda è fatta di errori di OCR, identificativi e composti unici.

## Come si lancia

Dalla root della repo. Dove gira lo decide `cluster.txt` (git-ignored), non il codice:
senza quel file parte un `LocalCluster`, con quel file un `SSHCluster` sui nodi elencati.

```bash
python Giulia/word_count.py                                   # campione, cluster locale
python Giulia/word_count.py data/silver/paragraphs             # corpus completo
```

| opzione | effetto |
|---|---|
| `--out DIR` | dove scrivere (default `~/mapd-out/word_count`, **fuori dalla repo**) |
| `--top N` | quante parole esportare e mettere nel grafico |
| `--partitions N` | usa solo le prime N partizioni — smoke test |
| `--split-out N` | in quante parti spezzare il reduce finale (default 16); `0` = `Bag.foldby`, più lento — vedi sotto |

Output: `top_words.csv`, `top_words.png` (il barplot che l'assignment chiede) e
`word_counts/` in Parquet col vocabolario completo.

### Su Cloud Veneto

`cluster.txt` va nella **root del repo** (viene cercato anche in `~` e nella cartella
corrente). Il **primo IP fa solo da scheduler**: con 4 worker servono 5 voci.

```bash
printf 'IP_SCHEDULER,IP_W1,IP_W2,IP_W3,IP_W4\n' > ~/MAPD-Project/cluster.txt
source ~/pyvenv/bin/activate
cd ~/MAPD-Project
python Giulia/word_count.py ~/mapd-data/silver/paragraphs --split-out 16
```

All'avvio lo script stampa su cosa sta girando davvero. **Controlla questa riga:**

```
workers   : 4 su 4 host ['10.67.22.51', ...]
```

Se leggi `su 1 host ['127.0.0.1']` non sei sul cluster: sta girando tutto sulla VM
scheduler, e `cluster.txt` non è stato trovato (lo script elenca dove ha cercato).

Le dimensioni dei worker non vanno scritte da nessuna parte: `memory_limit` è una
**frazione** della RAM del nodo (85%) e `nthreads` non è impostato, così ogni nodo usa i
propri core. Il cluster si ricostruisce da snapshot a ogni sessione e il flavour delle VM
cambia — le costanti assolute sono bug che aspettano. Per forzarle:
`CORD19_WORKER_MEMORY_FRACTION`, `CORD19_WORKER_MEMORY_LIMIT`,
`CORD19_THREADS_PER_WORKER`, `CORD19_WORKERS`, `CORD19_SSH_KEY`, `DASK_SCHEDULER`.

### Dove finisce il Reduce: il risultato più istruttivo del task

`Bag.foldby` e `Bag.frequencies` riducono **sempre a una partizione sola**: tutto lo
spazio delle chiavi deve stare in un singolo task, su un singolo worker. Con la parola
come chiave è fattibile — il vocabolario satura al crescere del corpus — ma resta **una
coda seriale**, e verso la fine i task intermedi accumulano dizionari che tendono al
vocabolario intero (6,04 M parole, ~1,5–2 GB).

Misurato sul corpus intero, stessa macchina, stesso codice, risultato identico:

| configurazione | tempo | worker uccisi (95% budget) |
|---|---:|---:|
| 7,0 GB/worker, `foldby` | 1.128 s | 0 |
| 3,4 GB/worker, `foldby` | 1.762 s | 3 |
| **3,4 GB/worker, `--split-out 16`** | **274 s** | **0** |

Due letture, entrambe utili:

- **memoria**: con 3,4 GB il `foldby` uccide 3 worker; il run finisce lo stesso perché
  Dask ricalcola, ma paga il 56% e i kill arrivano *a metà run*. Sul cluster vero, con la
  rete di mezzo, quei ricalcoli diventano una spirale;
- **tempo**: `--split-out` è **4 volte più veloce del miglior run con `foldby`**, su
  worker grandi la metà. Non è (solo) memoria: è che la coda seriale di `foldby` — un
  task che macina un dizionario Python da 6 M voci — diventa 16 task paralleli con
  aggregazione vettoriale.

Da qui il default. `--split-out 0` seleziona il `foldby` puro, ed è il modo di riprodurre
il confronto. La fase Map, dove sta il lavoro vero, resta Bag; il testo dell'assignment
permette esplicitamente di passare da Bag a DataFrame.

Verificato che le due strade danno lo stesso identico risultato sul corpus intero:
6.037.808 parole, 785.753.529 occorrenze, ogni conteggio uguale.

*Dettaglio che costa un run se non lo si sa*: lo shuffle P2P non riesce a ricostruire la
propria spec sullo scheduler quando il grafo nasce da un Bag e muore con
`P2P <id> failed during transfer phase`. Il codice forza lo shuffle task-based.

## Le decisioni, e il numero che le giustifica

Nessuna regola di pulizia è "di prassi": ognuna è stata aggiunta dopo averne misurato
la necessità su 1.108.771 paragrafi veri. Il diario completo delle misure è in
`local/NOTES.md` (non committato); qui il riassunto.

**Il pattern dei token tiene trattini, cifre e lettere greche.**
Con un `[a-z]+` ingenuo, in un word count del corpus COVID-19 la parola `covid-19`
**non esiste**: viene spezzata in `covid` + `19` e la cifra sparisce. Stessa sorte per
`sars-cov-2`, `tnf-α`, `nf-κb`, `il-1β`, `μg/ml`. Il 10,1% dei paragrafi contiene
lettere greche e il 7,7% latino accentato — e lì il danno è peggiore, perché non si
perde e basta: `müller` diventa i due token spazzatura `m` e `ller`.

**La punteggiatura tipografica va normalizzata.** `U+2013 EN DASH` è il carattere
non-ASCII più frequente dell'intero corpus (286.687 occorrenze): `covid–19` scritto così
non matcherebbe un pattern che conosce solo il trattino ASCII. `NFKC` unisce inoltre
`U+00B5 MICRO SIGN` e `U+03BC GREEK SMALL LETTER MU`, che sono la stessa lettera scritta
in due modi (40.964 + 74.308 occorrenze).

**Il preambolo LaTeX del PMC va rimosso a blocchi.** L'XML di PubMed Central allega a
ogni formula inline un fallback LaTeX col preambolo completo. Senza rimozione,
`usepackage` è la **decima parola del corpus** con 255.743 occorrenze. Vale l'1,00% di
tutti i caratteri, tocca 1.125 documenti (3,78%) e **solo** quelli con `source='pmc'`.
Togliere l'intero blocco `\documentclass … \end{document}` elimina in una riga tutta la
famiglia (`amsmath`, `wasysym`, `upgreek`, `setlength`, `pt`…) invece di rincorrerla a
colpi di stop-word.

**Due insiemi di parole ignorate, e la separazione è la giustificazione.**
`STOPWORDS` contiene solo parole funzione dell'inglese — proprietà della lingua.
`ARTIFACTS` contiene rumore di *questo* corpus: `et`/`al` (erano 3ª e 4ª parola con
347.632 e 343.098 occorrenze — i due conteggi quasi identici sono la firma di «et al.»)
e i rimandi a figure e tabelle. Regola che ci siamo dati: **non si tolgono parole di
contenuto**, nemmeno generiche come `study` o `data`, perché è una decisione di analisi,
non di pulizia — lo stesso principio del layer silver.

**Il filtro `is_reference_like` è stato tolto, dopo averlo misurato.**
Erano 235.914 paragrafi, l'**1,90%**, e per giunta corti — 304 caratteri di media contro
785 — quindi pesavano solo lo **0,74% del testo**: togliendo il filtro non entra né esce
nessuna parola dalla top-20, cambiano solo i conteggi. Costava tre punti nel codice (due
lettori più un'opzione da riga di comando) per una differenza che non si vede: la regola
del progetto è che la complessità si paga solo se la misura la giustifica, e qui non la
giustificava. La colonna resta nel `silver`, disponibile a chi la vuole.

## Il vincolo distribuito che decide tutto

`Bag.frequencies()` e `Bag.foldby()` riducono sempre a **una sola partizione**: l'intero
spazio delle chiavi deve entrare in un singolo task, su un singolo worker.

La prima versione riduceva su `(cord_uid, word)` ed è morta sul **10% del corpus**:

```
MemoryError: Task ('frequencies-aggregate-…', 0) has 7.25 GiB worth of input
dependencies, but worker has memory_limit set to 6.52 GiB
```

La chiave sbagliata cresce col numero di documenti (già sul campione è 10,5× il
vocabolario); la chiave giusta è la parola, il cui spazio satura al crescere del corpus.
La cura è il **combiner** del MapReduce classico: contare per documento *dentro* la
partizione e proiettare sulla parola prima dell'unico shuffle.

| | prima | dopo |
|---|---|---|
| partizioni di `doc_counts` | 1 (shuffle globale) | come l'input (**nessuno shuffle**) |
| cosa attraversa la rete | una entry per *occorrenza* | una entry per *(documento, parola)* |
| 200 partizioni su 1979 | `MemoryError` | 154 s |

Stesso risultato matematico, stesso numero di righe: ma la scelta della chiave di
riduzione decide se il lavoro è fattibile, indipendentemente da quanta RAM si compra.

**Caveat, quantificato.** Contando dentro la partizione, un documento con paragrafi a
cavallo di due partizioni produce più entry parziali: +1,30% di entry rispetto alle
coppie realmente distinte. Il Reduce le somma, quindi `c(w)` non cambia; cambia solo
che `doc_counts` non è esattamente "una riga per (documento, parola)".

## Benchmark

Obbligatorie per il corso: tempo di esecuzione contro **numero di partizioni** e contro
**numero di executor/processing unit**. Il testo dice «at least», e «processing units» si
legge sia come *macchine* sia come *thread*: si misurano entrambi. In tutto **18 misure**.

**Un comando, e la campagna gira da sola tutta la notte.** Ma prima si calibra.

```bash
# 1. calibrazione, ~30 min: 3 misure dello stesso punto
python Giulia/bench_word_count.py ~/mapd-data/silver/paragraphs --only riferimento

# 2. la notte, ~4,5 h
tmux new -s bench          # dura ore: non deve morire con la sessione SSH
python Giulia/bench_word_count.py ~/mapd-data/silver/paragraphs 2>&1 | tee ~/bench.log
```

La calibrazione non è un lusso: la stima delle 4,5 ore viene da **un solo dato del Mac**
(274 s) moltiplicato per un fattore per-core indovinato, e può sbagliare del doppio. E tre
accensioni di fila verificano i tre rischi noti — la porta 8786, i worker orfani, e dove
finiscono i file scritti.

| in ordine | cosa | perché lì |
|---|---|---|
| **1 · riferimento** ×3 | tutti i worker, `k=256`, `split_out=16` | è il punto **in comune ai tre assi**, e le sue 3 ripetizioni misurano il rumore una volta sola invece di pagarlo su ogni punto |
| **2 · worker** | *obbligatorio*: stessi dati, stesso taglio, meno macchine | costo prevedibile, nessun punto può fallire |
| **3 · partizioni** | *obbligatorio*: stessi dati, tagliati in modi diversi | dal centro ai bordi: la coda è sacrificabile |
| **4 · thread** | l'altra lettura di «processing units» | due punti, a valle di quelli obbligatori |
| **5 · foldby** | `split_out=0` sul cluster vero | ultimo apposta: se non completa, *quello* è il risultato |

**Prima di lasciarla andare, la prova generale** — la stessa identica campagna sul
campione, quattro minuti. Serve a scoprire un percorso sbagliato la sera invece che alle
sette del mattino:

```bash
python Giulia/bench_word_count.py --out /tmp/bench-prova --timeout 300
```

### Le scelte che cambiano il significato della misura

- **il lavoro cronometrato è quello che si consegna**: Map, Reduce e scrittura del
  vocabolario. Non la sola `topk`, che è più comoda da misurare — il crash dell'11 agosto
  stava proprio nella scrittura, cioè nel pezzo che la vecchia versione saltava;
- **a dati fissi** nello sweep sulle partizioni: cambia solo come sono tagliati. Misurare
  fette di corpus crescenti è un'altra domanda;
- **il numero di partizioni si controlla raggruppando i file in lettura**
  (`read_groups` + `split_evenly`), non con un `repartition` a valle: quello lascerebbe la
  lettura sempre alla stessa granularità e metterebbe nel cronometro il costo della
  ricucitura, che dipende da `k`. Vedi sotto, «le partizioni che non hai chiesto»;
- **una riga del CSV = una misura = un cluster nuovo.** Costa un minuto a punto, e non è
  pignoleria: un worker che ha già macinato milioni di stringhe trattiene RSS per
  frammentazione dell'allocatore (`PROJECT_CONTEXT.md` §7), quindi riusando un cluster per
  nove punti l'ultimo misurerebbe il partizionamento **più l'usura**. Come effetto
  collaterale sparisce anche il problema di `SSHCluster`, dove `scale()` può solo scendere
  e obbligherebbe a ricordarsi un ordine di esecuzione;
- **il rumore si misura una volta sola**: 3 ripetizioni sul punto di riferimento, 1
  altrove. La dispersione di quel punto è la scala con cui si giudicano tutte le
  differenze dei grafici — due punti che distano meno di quella barra non sono diversi;
- **ogni misura è appesa al CSV appena esiste**, e una configurazione che non completa
  viene registrata (`secondi` vuoto, l'errore accanto) senza fermare la campagna. Non è
  tolleranza ai guasti fine a sé stessa: «non ha completato» è una riga della tabella, non
  un buco — ed è il risultato che ci si aspetta dal `foldby`;
- **un `performance_report` per OGNI misura**, non quattro scelti in anticipo: costa due
  secondi e dà quello che nessuna curva dà (la linea del tempo dei task e la memoria di
  ogni worker). È il motivo per cui il picco di memoria **non sta nel CSV**: è già lì.
  Sempre con `mode="inline"`, altrimenti l'HTML scarica BokehJS da un CDN e resta bianco
  appena lo guardi senza internet — cioè dopo averlo copiato giù dal cluster.

Il benchmark va fatto **sul corpus vero**: su una fetta piccola i tempi sono dominati
dall'overhead di scheduling.

### Le partizioni che non hai chiesto

Per mesi lo script ha stampato `partitions: 990` su una cartella di **1979 file**, e per un
po' abbiamo creduto a una copia parziale dei dati. Non lo era: sono 1979 file e 12.445.234
righe, sia in locale sia sulla VM. Il 990 era dask.

`to_bag` forza `optimize()`, e l'ottimizzatore di dask-expr **fonde le partizioni piccole**
(mediana 2,6 MB) a due a due. `blocksize` non c'entra: da 4 a 64 MB, con e senza Client
distribuito, `read_parquet` resta sempre a 1979 — ogni Parquet del silver ha un solo
row-group e niente aggrega fra file.

**La sorpresa è arrivata togliendo il filtro `is_reference_like`**, che era in mezzo alla
lettura. Stessi file, stesso `to_bag`, misurato sul corpus intero:

| lettura | partizioni eseguite |
|---|---:|
| 3 colonne + filtro booleano + proiezione → `to_bag()` | **990** |
| 2 colonne → `to_bag()` *(quello che gira oggi)* | **1.979** |

Cioè: **il numero di partizioni non dipende solo dai file, dipende dalla forma della
query.** Un ramo condizionale in mezzo alla lettura ha cambiato la parallelizzazione di
tutto il job, e nessuno l'aveva chiesto. Oggi le partizioni tornano 1:1 con i file, il che
è più prevedibile — ma è un effetto collaterale, non una scelta: se domani rientrasse una
colonna in più, il numero potrebbe muoversi di nuovo.

Due conseguenze che valgono per tutti e quattro i task:

- **`.npartitions` prima di un `compute` non dice quante partizioni gireranno.** Il numero
  vero è quello dell'espressione *ottimizzata*;
- attraverso `dd.read_parquet` il numero di partizioni **non è una manopola**, è una
  proprietà che si scopre. Per un benchmark che ha il partizionamento come variabile
  indipendente è squalificante — da qui `read_groups`, che dà esattamente `k` (verificato:
  `k` chiesto ⇒ `k` task di Map nel grafo eseguito, perché un Bag non è una collezione
  dask-expr e nessun ottimizzatore lo tocca alle spalle).

## Limiti noti

- **Il corpus non è monolingue e noi sanifichiamo solo l'inglese.** La spia è `der`, che
  entra nelle prime 100 parole con 69.605 occorrenze. Sondaggio sulle parole funzione:
  tedesco 267.505, francese 41.311, spagnolo 40.408, portoghese 19.615. La direzione
  scelta è dare a quelle lingue le loro stop-word, non scartare i paper.
- **Gli script non latini sono esclusi di proposito.** Il CJK è lo 0,03% dei paragrafi
  e non è mai il corpo del documento (sono parole chiave cinesi citate dentro testo
  inglese); contarlo richiederebbe un segmentatore, perché il cinese non ha spazi.
- **Niente stemming.** `cell`/`cells`, `study`/`studies`, `use`/`used`/`using` occupano
  slot separati. Unirli cambierebbe la classifica, ma è una decisione di analisi da
  argomentare, non da dare per scontata.
