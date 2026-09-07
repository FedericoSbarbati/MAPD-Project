# Task 2.3.3 — Title embeddings

Soluzione del punto **2.3.3 "Obtaining Embeddings for Paper Titles"**: il titolo di ogni
paper diventa un vettore numerico usando il modello pre-addestrato fastText
`crawl-300d-2M-subword.vec`.

| File | Cosa è |
|---|---|
| `title_embeddings.py` | **La fonte di verità**: le funzioni e il `main()`. È quello che gira |
| `bench_title_embeddings.py` | La campagna di benchmark: un comando, una riga di CSV per misura |
| `task_2_3_3_title_embeddings.ipynb` | Il notebook: **importa** il `.py`, spiega e disegna i grafici |
| `requirements.txt` | Le versioni usate — già tutte presenti in `~/pyvenv` sulle VM |

Convenzione del repo (`docs/DECISIONI.md`): **un `.py` con le funzioni, un notebook che
lo importa**, mai codice duplicato tra i due. Sul cluster si lancia il `.py` da terminale.

---

## 1 · Come si lancia

```bash
# sullo scheduler, dal repo clonato in ~/MAPD-Project
source ~/pyvenv/bin/activate

# il task
python daniele/title_embeddings.py ~/mapd-data/silver/papers

# la campagna di benchmark (prima la prova generale, poi la calibrazione, poi sul serio)
python daniele/bench_title_embeddings.py --out /tmp/bench-prova --timeout 300
python daniele/bench_title_embeddings.py ~/mapd-data/silver/papers --only riferimento
tmux new -s bench-emb
python daniele/bench_title_embeddings.py ~/mapd-data/silver/papers --ripetizioni 3 2>&1 | tee ~/bench-emb.log
```

**Prerequisiti sulle macchine** (non sono nel repo perché non ci possono stare):

1. `cluster.txt` nella radice del repo, sullo scheduler — è git-ignored, si scrive lì:
   ```
   10.67.22.144,10.67.22.162,10.67.22.225,10.67.22.237
   ```
   Il **primo host fa solo da scheduler**, gli altri tre sono worker.
2. Il modello in `~/mapd-model/crawl-300d-2M-subword.vec` **su ogni macchina che fa da
   worker** (vedi §3: non c'è più un file system condiviso).

Non serve modificare nessun altro file del repo.

## 2 · Struttura e flusso logico

```
Fase 1   silver/papers ──► filtro title_ok ──► findall + explode ──► (cord_uid, word)
         (2 colonne su 23)                     + stop-word                  │  [persist]
                                                                            │
                                        parole distinte = VOCABOLARIO ◄─────┘
                                                    │ (~10⁵ stringhe, sul driver)
                                                    ▼
Fase 2   model.vec (4,5 GB) ──► dd.read_csv a blocchi ──► tieni solo il vocabolario
                                                                            │
                        (cord_uid, word) ⋈ (word, v0..v299)  broadcast del modello
                                                                            │
                        groupby(cord_uid).sum() ──► ÷ n_words = MEDIA ──► Parquet
```

Le funzioni del `.py` seguono questo ordine: `read_titles` → `tokenize` →
`vocabulary_of` → `read_model` → `keep_vocabulary_words` → `mean_pooling`, e `build` le
mette in fila.

**Perché la media (mean pooling).** L'assignment permette sia la lista di vettori sia
«aggregated into a single vector». La media dà un vettore di **dimensione fissa** (300)
per ogni titolo, che è la forma che serve al 2.3.4: la cosine similarity si calcola tra
due vettori, non tra due liste di lunghezza diversa.

**Cosa contiene l'output.** `cord_uid | n_words | v0..v299`, in Parquet zstd.
`n_words` è quante parole del titolo hanno trovato un vettore (le altre sono saltate,
come suggerisce l'assignment). Il **titolo non c'è**: sta in `silver/papers` a un join di
distanza, e portarselo dietro avrebbe aggiunto uno shuffle a ogni run misurato senza
alcuna ragione di calcolo. Il 2.3.4 lo recupera con `dd.read_parquet(papers,
columns=["cord_uid", "title", "is_title_unique"])` — lettura colonnare, costa poco.
`is_title_unique` gli serve davvero: 122 mila paper hanno un titolo duplicato, e le
coppie con similarità 1 sarebbero un artefatto.

## 3 · La domanda importante: come fanno worker da 3,8 GB a usare un modello da 4,5 GB?

**Il modello non viene mai caricato.** Il cambio di prospettiva è tutto qui: non è un
oggetto da caricare in memoria, è **un dataset da leggere a pezzi**, esattamente come i
dati.

1. Il file `.vec` è **testo**: 2 milioni di righe `parola v1 v2 ... v300`.
2. `dd.read_csv(..., blocksize="64MB")` lo taglia in ~70 blocchi. Ogni blocco è una task:
   il worker che la esegue legge **solo quel blocco** dal proprio disco, lo parsa e —
   prima che arrivi il successivo — lo **filtra**, tenendo solo le righe la cui parola
   compare in qualche titolo. Il picco per task è la dimensione di un blocco, non del file.
3. Dei 2 000 000 di vettori sopravvivono solo quelli del vocabolario dei titoli (~10⁵):
   quella fetta (poche centinaia di MB, distribuita) è l'unica cosa che resta in memoria.

Il file `.bin` da 7,2 GB **non si usa**: quello sì andrebbe caricato tutto in RAM dalla
libreria `fasttext`, ed è impossibile sui nostri worker. Il `.vec`, essendo testo riga per
riga, si presta alla lettura distribuita.

> ⚠️ **Serve su ogni worker, allo stesso path.** Con l'architettura attuale non c'è più
> né volume né NFS: ogni macchina legge dal proprio disco. Se il modello sta solo sullo
> scheduler, i worker falliscono con `FileNotFoundError` — ed è per questo che il `.py`
> controlla che il file esista e lo dice esplicitamente.

### Il filtro, e il collegamento col MEMORY_LEAK_REPORT

Il filtro fa una domanda ripetuta ~70 volte: *"questa parola sta nel vocabolario?"*,
contro un insieme di ~10⁵ stringhe. Scritto nel modo ovvio —
`block["word"].isin(insieme_python)` — pandas **riconverte l'insieme in formato Arrow a
ogni blocco**: stesso risultato, ma secondi di lavoro GIL-bound per task e una montagna di
oggetti temporanei che l'allocatore poi trattiene. È **esattamente** l'hotspot
root-causato in `docs/MEMORY_LEAK_REPORT.md` (lì valeva ~170×).

La cura, adottata qui: il vocabolario è convertito **una volta sola sul driver** in
`pyarrow.Array`, e ogni blocco è filtrato col kernel vettoriale `pc.is_in`.

Dal report viene anche il resto della ricetta, che però sta già in `cluster.py` e vale per
tutti i task: `MALLOC_TRIM_THRESHOLD_=0` e `MALLOC_ARENA_MAX=2` impostate **prima** che
nasca un worker.

### L'altra scelta che riguarda la memoria: il broadcast del join

Dopo il join ogni riga `(paper, parola)` porta **300 float**. Un join normale rimescola
entrambi i lati per chiave `word`: sul corpus intero sono svariati GB che attraversano la
rete, e per giunta sparpaglia le parole di uno stesso paper su partizioni diverse, così il
`groupby` che segue non ha più niente da ridurre localmente.

Mandando invece il modello filtrato (piccolo) a tutti i worker, il join diventa locale, i
titoli restano partizionati come sono stati letti, e il `groupby` può sommare le parole di
un paper **dentro la partizione** prima che qualcosa si muova. È lo stesso identico trucco,
per la stessa ragione, del join prefer-pmc della conversione (`PROJECT_CONTEXT.md` §7,
Atto 1, dove un `merge` fu riscritto come broadcast). `--no-broadcast` resta come manopola
perché la differenza tra i due vale la pena di essere misurata.

## 4 · Benchmark

Stesso metodo della campagna del word count, così i risultati stanno nella stessa
relazione: **una riga del CSV = una misura = un cluster nuovo**, e ogni punto è il
riferimento con **una sola manopola cambiata**. Ogni misura parte da worker appena nati,
perché su questo cluster un worker che ha già macinato milioni di stringhe trattiene RSS
per frammentazione: riusandolo, la misura sommerebbe il partizionamento e l'usura.

Le manopole sono cinque — le prime due sono le due **larghezze** del grafo:

| manopola | cosa cambia | il confine |
|---|---|---|
| `partizioni` | in quante parti si tagliano i **titoli** | poche partizioni = task grosse = picco di RAM |
| `blocksize` | in che blocchi si legge il **modello** | idem, sul lato modello |
| `worker` | quanti **processi** | speedup ed efficienza |
| `thread` | quanti task **dentro** lo stesso processo | GIL + memoria condivisa nel worker |
| `split_out` | quanto è larga la **coda** del Reduce | `split_out=1` = una task tiene tutto |

più un punto singolo, `broadcast`, che confronta le due strategie di join.

Si misurano **due** cose per ogni punto: i **secondi** e il **picco di RAM del worker più
carico** (`picco_gb`, da `ru_maxrss`, lo stesso strumento di `Giulia/misura_ram.py`). Il
picco non è un extra: è quello che decide se una configurazione completa o muore con
`KilledWorker`, e sulle curve del word count è la metà della storia.

Le ripetizioni (`--ripetizioni 3`) sono **passate intere**, non tre misure di fila dello
stesso punto: fra due ripetizioni passano ore, quindi la dispersione comprende la
variabilità della macchina nella giornata. E se la campagna si interrompe, quello che resta
in mano è una campagna completa invece di mezza curva misurata tre volte.

**Dove finisce tutto** (fuori dalla repo, che sul cluster è usa-e-getta):

```
~/mapd-out/title_embeddings/embeddings/     il risultato (Parquet, sui dischi dei worker)
~/mapd-out/bench-embeddings/misure.csv      una riga per misura  <- QUESTO va scaricato
~/mapd-out/bench-embeddings/report_*.html   una dashboard Bokeh per ogni misura
~/mapd-out/title_embeddings/bench_*.png     i grafici, generati dal notebook
```

Il notebook (§8) legge `misure.csv` e disegna, per ogni manopola, tempo e picco di RAM
affiancati.

## 5 · Il muro della memoria, misurato sul cluster

Il primo run sul corpus intero è **morto** con `KilledWorker`, e vale la pena raccontarlo
perché è la stessa legge che governa la curva del word count.

Il colpevole è il join: attacca **300 float32 (1,2 kB) a ogni riga**. Le ~7,8 milioni di
coppie `(paper, parola)` del corpus diventano **~9,3 GB**, e il picco di **una** task è
quel totale diviso il numero di partizioni — con ogni thread del worker che ne tiene una:

| partizioni | picco per task | × 2 thread | su worker da 3,5 GB |
|---:|---:|---:|---|
| 3 | 3,11 GB | 6,21 GB | **morto** (il primo run) |
| 16 | 0,58 GB | 1,17 GB | ok |
| 64 | 0,15 GB | 0,29 GB | ok (default) |

Con `PARTITIONS = 0` (lascia al reader il suo partizionamento) i 9 file del silver
venivano aggregati dall'ottimizzatore in **tre** partizioni sole — è il fenomeno
«partizioni ≠ file» di `PROJECT_CONTEXT.md` §8.6 — e ogni task chiedeva 3,1 GB.
Il default è quindi **64**, e la manopola `partizioni` del benchmark serve proprio a
misurare dove sta il muro invece di indovinarlo.

Due leve in più, se un giorno il muro si riavvicina: `CORD19_THREADS_PER_WORKER=1`
(dimezza il picco per worker, e sul word count i thread peggioravano comunque i tempi) e
macchine più grandi.

### Secondo atto: il broadcast lo facciamo noi, non Dask

Sistemate le partizioni, il run è morto una seconda volta — ma con un messaggio molto più
utile:

```
MemoryError: Task ('broadcastjoin-...', 0) has 8.04 GiB worth of input dependencies,
but worker ... has memory_limit set to 3.25 GiB
```

Il numero però **non torna**: il modello filtrato sono ~162 mila parole a 1,2 kB l'una,
cioè **0,2 GB** (misurato). Dask ne dichiarava 8, quaranta volte tanto: con
`merge(..., broadcast=True)` ogni partizione in uscita risultava dipendere dal lato
modello *nel suo insieme*, e lo scheduler si rifiutava di partire.

La cura è smettere di far decidere a lui una cosa che sappiamo già:

```python
table = model.compute()                              # 0,2 GB: piccolo e motivato
handle = client.scatter(table, broadcast=True)       # una copia per worker, una volta
joined = tokens.map_partitions(_join_block, handle)  # da qui in poi tutto è locale
```

È **esattamente il broadcast della Lecture 2 di Dask** (`client.scatter(obj,
broadcast=True)`), ed è la stessa mossa dell'Atto 1 della conversione, dove un `merge` fu
riscritto come broadcast. Il costo diventa **noto**: 0,2 GB per worker, una volta sola,
e nessuno shuffle. `--no-broadcast` resta come alternativa da manuale (join distribuito
vero, con rimescolamento di entrambi i lati) ed è una delle manopole del benchmark.

Il `.py` stampa ora quanto pesa davvero la tabella broadcastata:
`model kept : 161,901 words, 0.20 GB to every worker`.

## 6 · Cose da sapere / limiti

- **L'output degli embedding è distribuito**: lo scrivono i worker, ognuno sul proprio
  disco, quindi la cartella sulla macchina scheduler resta **vuota**. È il motivo del
  `client.run(os.makedirs, ...)` nel `.py`. Quello che va scaricato sul portatile prima di
  spegnere le VM è il **CSV delle misure** e i grafici, che sono pochi MB; gli embedding
  veri servono al 2.3.4 e conviene tenerli dove sono finché quel task non gira.
- **La pipeline non è interamente pigra**: il vocabolario deve esistere sul driver prima
  che il modello si possa filtrare, quindi c'è una prima passata sui titoli che non si può
  fondere con la seconda. Il cronometro del benchmark le comprende entrambe, perché è
  quello che si consegna.
- **La copertura non è il 100%**: un titolo le cui parole non stanno nel modello (tipico
  dei titoli non inglesi) non produce un vettore. Il numero esatto lo stampa il `.py`.
- **Le stop-word sono solo inglesi.** È una scelta già registrata in `DECISIONI.md` come
  limite noto e comune ai task; la direzione decisa è dare a tedesco, francese, spagnolo e
  portoghese le loro liste, dopo i benchmark.
- **`title_embeddings.py` non importa niente del repo a livello top**, e non è un vezzo:
  viene spedito ai worker con `upload_file` e deve essere importabile **da solo**
  (`PROJECT_CONTEXT.md` §8.12a). `from cluster import get_client` sta dentro `main()`.
- La pipeline è stata **verificata numericamente in locale** su un dataset finto: la media
  distribuita coincide con quella calcolata a mano in pandas (`atol 1e-5`), e le due
  strategie di join danno lo stesso risultato riga per riga.
