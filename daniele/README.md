# daniele — 2.3.3 Title embeddings

Ogni titolo di paper diventa **un vettore di 300 numeri**. Il modello pre-addestrato
fastText associa un vettore di 300 numeri a una *parola*; un titolo è un insieme di parole,
quindi i vettori delle sue parole vengono mediati in un unico vettore per paper
(**mean pooling**).

| file | cos'è |
|---|---|
| `title_embeddings.py` | l'implementazione: le funzioni, più un `main()` per lanciarla da terminale |
| `bench_common.py` | la macchina condivisa dalle tre campagne di benchmark: accende un cluster, cronometra un punto, scrive la riga di CSV. Non si lancia da solo |
| `bench_scaling.py` | campagna: tempo contro numero di processi e numero di partizioni |
| `bench_threads.py` | campagna: cosa succede quando più task condividono un processo (il suo GIL e la sua RAM) |
| `bench_knobs.py` | campagna: le manopole di questo task — `blocksize`, `split_out`, `broadcast` |
| `task_2_3_3_title_embeddings.ipynb` | il notebook, che **legge** `measures.csv` e disegna le figure. Nessun codice duplicato |

Dipendenza condivisa col resto del progetto: `cluster.py` alla root, che accende il cluster.
Dove gira lo decide `cluster.txt`, mai il codice: nel codice non compare nessun IP.

---

## Il vettore di un titolo ha 300 numeri, non 300 × numero di parole

Vale la pena fissarlo subito, perché è la scelta che decide tutto il resto.

300 non è "quanto vale una parola": è la **dimensione dello spazio**. Come su una mappa ogni
città è 2 numeri e il centro di 10 città è ancora 2 numeri, così il baricentro di 9 parole è
ancora un punto dello stesso spazio a 300 assi.

L'alternativa sarebbe concatenare i vettori delle parole, ma allora un titolo di 5 parole
avrebbe 1.500 numeri e uno di 12 ne avrebbe 3.600: due vettori di lunghezza diversa non si
possono confrontare. La media è l'unica delle due rappresentazioni che dà a tutti i titoli
la **stessa larghezza**.

Il prezzo è dichiarato: la media perde l'ordine delle parole. *"man bites dog"* e *"dog bites
man"* ottengono lo stesso identico vettore.

---

## Il flusso dei dati

```
INPUT   silver/papers                        970.836 righe x 23 colonne
                                             ne legge 3: cord_uid, title_norm, title_ok
          |  tiene title_ok, ripartiziona in 64
          |  findall [a-z]{2,}  ->  explode  ->  toglie le stop-word
          v
 (1)    tokens        cord_uid | word        una riga per PAROLA (duplicati tenuti)
          |
          +--------> unique()  ->  vocabulary      161.901 parole distinte (sul driver)
          |                            |
          |                            |  filtra il modello, blocco per blocco
          |                            v
          |          model.vec  ->  model kept     87.774 parole x 300  =  0,11 GB
          |        2.000.000 parole                una copia broadcastata a ogni worker
          |        letto in blocchi da 64 MB        |
          |                                         |
          +-------------> JOIN su word <------------+
                              |
                              v
 (2)    merged      cord_uid | word | v0..v299     9.020.035 righe, 1,2 kB l'una
                              |
                              |  groupby(cord_uid).sum()  ->  diviso n
                              v
OUTPUT  embeddings  cord_uid | n_words | v0..v299  969.021 righe x 302 colonne
                                                   8 file parquet, 1,07 GB
```

Tre cose da leggere in quello schema:

* **il vocabolario è un ramo laterale, non la linea principale.** `tokens` tiene ogni
  occorrenza, perché la media di un titolo deve sapere a quale paper appartiene ogni parola.
  Le parole distinte servono solo a filtrare il modello, e dopo si buttano;
* **lo stato (2) è il punto più largo della pipeline.** Il join aggiunge 300 float32 a ogni
  riga, quindi 9 M di righe pesano ~11 GB — molto più di quanto il cluster abbia. Ci sta solo
  perché è tagliato in 64 partizioni e non esiste mai tutto insieme;
* **l'output è a larghezza fissa.** 969.021 titoli su 970.836 paper hanno ottenuto un
  vettore; i 1.815 rimasti fuori non hanno nemmeno una parola nota al modello, e vengono
  esclusi invece che scritti come vettori di zeri (un vettore di zeri ha coseno indefinito).

---

## Cosa succede quando lanci lo script

```bash
python daniele/title_embeddings.py ~/mapd-data/silver/papers
```

`main()` in ordine:

1. **`parse_args()`** legge gli argomenti. I percorsi relativi vengono risolti rispetto alla
   **root della repo**, non alla cartella corrente, così `python daniele/title_embeddings.py`
   e `cd daniele && python title_embeddings.py` significano la stessa cosa.
2. **Controlla input e modello** *prima* di accendere il cluster. Il `.vec` deve esistere
   allo stesso percorso su **ogni** macchina: non c'è un filesystem condiviso, ogni worker
   legge dal proprio disco.
3. **`get_client()`** accende il cluster. Cosa trova, cosa fa: `DASK_SCHEDULER` → si attacca a
   uno scheduler già acceso; `cluster.txt` → `SSHCluster`; niente → `LocalCluster`.
4. **`client.upload_file(__file__)`** spedisce questo file a scheduler e worker. Serve perché
   nel grafo le funzioni viaggiano **per nome**: i worker devono poter importare il modulo,
   e il codice esiste solo sulla macchina da cui lanci. Senza, l'errore che si vede è
   `Error during deserialization of the task graph`, e il `ModuleNotFoundError` vero si legge
   solo nel log dello scheduler.
5. **`client.run(os.makedirs, ...)`** crea la cartella di output **su ogni macchina**:
   `to_parquet` la crea solo qui sul client, ma a scrivere sono i worker, ognuno sul proprio
   disco.
6. **`build(...)`** monta la pipeline e restituisce un grafo da calcolare.
7. **`dask.compute(write, count, sum)`** è il calcolo vero.
8. **`finally:`** chiude client e cluster comunque sia andata.

---

## Le funzioni, una per una

| funzione | riceve | restituisce | calcola qualcosa? | dove gira |
|---|---|---|---|---|
| `read_titles` | percorso del parquet | dask.DataFrame | no, monta il grafo | driver |
| `tokenize` | dask.DataFrame | dask.DataFrame | no, monta il grafo | driver |
| `vocabulary_of` | dask.DataFrame | `pa.Array`, 161.901 stringhe | **sì** — `.compute()` | worker, risultato al driver |
| `read_model` | percorso del `.vec` | dask.DataFrame 1+300 colonne | no, monta il grafo | driver |
| `keep_vocabulary_words` | un blocco pandas + il vocabolario | lo stesso blocco filtrato | sì, subito | worker, dentro un task |
| `join_vectors` | i due dask.DataFrame | dask.DataFrame | **sì** — calcola il modello filtrato | driver, poi `scatter` |
| `_join_block` | 2 pandas DataFrame | 1 pandas DataFrame | sì, subito | worker, dentro un task |
| `mean_pooling` | dask.DataFrame | dask.DataFrame | no, monta il grafo | driver |
| `_to_mean` | un blocco pandas | un blocco pandas | sì, subito | worker (+ 1 volta sul driver, sul `meta`) |
| `build` | tutto | `(grafo, n_vocabolario, n_partizioni)` | sì, le due di sopra | driver |

La distinzione che conta è fra le due famiglie:

* le funzioni che **montano il grafo** (`read_titles`, `tokenize`, `read_model`,
  `mean_pooling`) tornano in millisecondi con un oggetto piccolo che descrive *cosa* fare.
  I dati restano sul disco;
* le funzioni che **fanno il lavoro** (`keep_vocabulary_words`, `_join_block`, `_to_mean`)
  ricevono veri `pandas.DataFrame` e girano dentro un task, su un worker. Sono passate a
  `map_partitions`, non chiamate direttamente.

`_to_mean` viene chiamata anche **una volta sul driver**, su un frame vuoto, per ricavare il
`meta`: lo schema dell'output derivato eseguendo la funzione stessa, così non può essere in
disaccordo con quello che la funzione fa davvero.

---

## Le tre computazioni

La pipeline **non è tutta lazy**, e sapere dove non lo è spiega perché il task ha delle fasi.

| # | dove | perché non si può rimandare |
|---|---|---|
| 1 | `vocabulary_of` | il vocabolario deve esistere **sul driver** prima che il modello possa essere filtrato: è l'argomento del filtro |
| 2 | `join_vectors`, `model.compute()` | con `broadcast=True` il modello filtrato deve esistere prima di poter essere consegnato ai worker |
| 3 | `main`, `dask.compute(...)` | il calcolo vero: scrittura + le due statistiche di copertura |

La (1) e la (2) sono due passate reali sui dati. Per questo `build` fa
`tokenize(papers).persist()`: `tokens` viene letto due volte, una per il vocabolario e una
per il join, e senza `persist` Dask rifarebbe da capo lettura del parquet e tokenizzazione.

La (3) chiede **scrittura e statistiche insieme**, in un solo `dask.compute`: Dask fonde i
due grafi e la pipeline gira una volta sola. Non è solo un'ottimizzazione. L'alternativa
ovvia — scrivere, poi rileggere l'output per contare — **non può funzionare**: a scrivere
sono i worker sui propri dischi, e sulla macchina da cui hai lanciato la cartella resta
vuota, perché lì non gira nessun worker. Rileggerla dal driver dà
`No files satisfy the parquet_file_extension criteria` dopo un run perfettamente riuscito.

---

## Come si legge l'output

Le 8 parti finiscono **sparse sui dischi dei worker**, con numerazione globale (`part.0`,
`part.1`, …), quindi si possono raccogliere in una cartella sola senza che si pestino:

```bash
mkdir -p ~/raccolta/embeddings
for w in IP_W1 IP_W2 IP_W3; do
    rsync -a ubuntu@$w:~/mapd-out/title_embeddings/embeddings/ ~/raccolta/embeddings/
done
```

I titoli **non** sono nell'output: restano in `silver/papers`, a un join di distanza.
Portarseli dietro lungo la pipeline avrebbe aggiunto uno shuffle a ogni run misurato senza
nessuna ragione di calcolo.

---

## Come si lancia

Dalla root della repo.

```bash
python daniele/title_embeddings.py                             # campione, cluster locale
python daniele/title_embeddings.py ~/mapd-data/silver/papers   # corpus completo
```

| opzione | effetto |
|---|---|
| `--model PATH` | il `.vec` di fastText (default `~/mapd-model/crawl-300d-2M-subword.vec`) |
| `--out DIR` | dove scrivere (default `~/mapd-out/title_embeddings`, **fuori dalla repo**) |
| `--partitions N` | in quante partizioni tagliare i titoli (default 64, `0` = quelle del lettore parquet) |
| `--blocksize S` | in che blocchi leggere il modello (default `64MB`) |
| `--split-out N` | in quante parti esce il Reduce finale (default 8, `0` = una sola) |
| `--no-broadcast` | join distribuito vero, con shuffle di entrambi i lati, invece del broadcast |

All'avvio lo script stampa su cosa sta girando davvero. **Controlla questa riga:**

```
worker    : 3 su 3 host ['10.67.22.162', '10.67.22.225', '10.67.22.237']
```

Se leggi `su 1 host ['127.0.0.1']` non sei sul cluster: `cluster.txt` non è stato trovato e
sta girando tutto sulla macchina scheduler.

### Il default `--partitions 64`, e perché non è un numero a caso

Il join aggiunge 300 float32 a ogni riga (1,2 kB), quindi le coppie (paper, parola) del
corpus completo pesano ~9,3 GB in tutto. Il picco di **un** task è quel totale diviso il
numero di partizioni, e ogni thread di un worker ne tiene uno alla volta.

Misurato sul cluster, 3 worker × 2 thread da 3,5 GB l'uno:

| partizioni | GB per task | GB per worker (2 thread) | esito |
|---:|---:|---:|---|
| 3 (quelle del lettore parquet) | 3,1 | 6,2 | tutti i worker uccisi |
| 64 | 0,15 | 0,3 | 52,3 s |

È la stessa legge che si vede nella figura 1 del notebook: il picco di un task va come 1/k.

### Il join: perché il broadcast è scritto a mano

Il modello filtrato è **piccolo** — 87.774 parole, 0,11 GB — quindi lo si calcola una volta e
se ne manda una copia a ogni worker con `client.scatter(..., broadcast=True)`. Da lì in poi
ogni partizione fa il suo join in locale: niente shuffle.

Non si usa `merge(..., broadcast=True)` lasciando decidere a Dask. Misurato sul cluster: Dask
faceva dipendere ogni partizione di output dall'intero lato modello e si rifiutava di partire
con `8.04 GiB worth of input dependencies` contro un worker da 3,25 GiB — circa trenta volte
la dimensione reale di quella tabella. Facendo il broadcast a mano il costo è noto: una copia
da 0,11 GB per worker, una volta sola.

`--no-broadcast` seleziona il join distribuito vero, ed è il modo di riprodurre il confronto.
