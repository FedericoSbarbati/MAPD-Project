# Giulia — 2.3.1 Word count distribuito

Conteggio delle parole sul full text di CORD-19, con Dask **Bag** (la struttura
raccomandata dal testo: «we recommend utilizing the RDD/Bag data structure»).

| file | cos'è |
|---|---|
| `word_count.py` | l'implementazione: funzioni pure + un `main()` per lanciarla da terminale |
| `word_count.ipynb` | il notebook, che **importa** il modulo — nessun codice duplicato, nessun sottoprocesso |

Dipendenze condivise col resto del progetto: `bench.py` alla root (creazione del Client
e strumenti di misura). Nessun ambiente Python separato: si usa quello del progetto
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
python Giulia/word_count.py data/silver/paragraphs --check     # + verifica dell'invariante
```

| opzione | effetto |
|---|---|
| `--out DIR` | dove scrivere (default `reports/word_count`, git-ignored) |
| `--top N` | quante parole esportare e mettere nel grafico |
| `--partitions N` | usa solo le prime N partizioni — smoke test |
| `--keep-references` | non scartare i paragrafi `is_reference_like` |
| `--check` | verifica l'invariante Map/Reduce (raddoppia il tempo) |

Output: `top_words.csv`, `top_words.png` (il barplot che l'assignment chiede) e
`word_counts/` in Parquet col vocabolario completo.

Variabili d'ambiente del cluster (le stesse della pipeline di conversione):
`CORD19_WORKERS`, `CORD19_THREADS_PER_WORKER`, `CORD19_WORKER_MEMORY_LIMIT`,
`CORD19_SSH_KEY`, `DASK_SCHEDULER`.

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

**Il filtro `is_reference_like` è attivo per definizione, non per risultato.**
Sono l'1,24% dei paragrafi e per giunta corti (477 caratteri contro 867): togliendoli,
nella top-30 non entra né esce nessuna parola, cambia lo 0,45% delle occorrenze. Lo
teniamo perché una dichiarazione di conflitto d'interessi non è corpo del paper, e
perché è gratis. Non perché migliori la classifica.

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

Obbligatori per il corso: tempo di esecuzione contro **numero di partizioni** e contro
**numero di worker**. Stanno nel notebook (§9), con l'impalcatura in `bench.py`.

Tre dettagli che cambiano il significato della misura:

- nel sweep sulle partizioni **i dati restano gli stessi** e cambia solo come sono
  suddivisi. Il vecchio benchmark misurava fette di corpus crescenti (3/10/25/50
  partizioni), cioè tempo vs *quantità di dati*: un'altra domanda;
- si usa `repartition(npartitions=k)`, **non** `repartition(partition_size=…)`: la
  seconda si impianta sotto un Client distribuito (`PROJECT_CONTEXT.md`, §8.4);
- su `SSHCluster` il pool di worker è la lista di host, quindi il sweep può solo
  **scendere**: si elencano tutti i worker in `cluster.txt` e si scala verso il basso.

`bench.measure` ripete ogni misura, chiama `sweep()` **tra** una ripetizione e l'altra
(mai dentro una regione cronometrata) e non usa `client.restart()`, che sporcherebbe i
tempi. Conserva tutte le ripetizioni, non solo la media: è la dispersione a dire se una
differenza è reale. Registra anche l'unmanaged del worker peggiore — se cresce
linearmente con le ripetizioni c'è un hotspot di churn nel task
(`docs/MEMORY_LEAK_REPORT.md`, §7.4).

Il benchmark va fatto **sul corpus vero**: su una fetta piccola i tempi sono dominati
dall'overhead di scheduling e più worker risultano più lenti di uno solo.

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
