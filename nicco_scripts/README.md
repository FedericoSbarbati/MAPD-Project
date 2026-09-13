# Niccolò — 2.3.4 Similarità coseno fra i titoli

Il testo chiede di *«compute the cosine similarity between each pair of titles»* a partire
dai vettori del 2.3.3, e di usarla per *«identify some of the most similar (and most
dissimilar) papers»*. Quel **«some»** è la chiave di lettura del task: non si chiede di
*conservare* le similarità, si chiede di *attraversarle per trovarne gli estremi*.

| file | cos'è |
|---|---|
| `cosine.py` | l'implementazione: funzioni pure + un `main()` per lanciarla da terminale |
| `bench_cosine.py` | la campagna di benchmark: un comando, e scrive un CSV riga per riga |
| `embeddings/` | l'output del 2.3.3 di Daniele (git-ignored): 8 Parquet, 969.021 titoli × 300 |
| `notte_2_3_4.sh` | la campagna notturna del 2026-09-13: l'elenco dei comandi, non logica |

Dipendenza condivisa col resto del progetto: `cluster.py` alla radice, che accende il
cluster e stampa com'è fatto. Nessun ambiente Python separato.

```bash
python nicco_scripts/cosine.py nicco_scripts/embeddings --titoli 100000 --out ~/mapd-out/2_3_4
python nicco_scripts/bench_cosine.py nicco_scripts/embeddings --ripetizioni 3
```

## Sul cluster

**Gli embedding servono su UNA macchina sola**, quella da cui si lancia — cioè la prima riga
di `cluster.txt`, che fa da scheduler. È la conseguenza pratica della scelta di replicare i
dati con `scatter`: i worker li ricevono dal driver, non li leggono da disco. Al contrario
del modello FastText del 2.3.3, che va copiato su ogni VM.

```bash
# 1 · dal Mac, una volta: 1,1 GB verso lo SCHEDULER (rsync e non scp, cosi' un
#     trasferimento interrotto riprende da dove era)
rsync -av --progress \
    -e "ssh -J UTENTE_CV@gate.cloudveneto.it -i ~/.ssh/id_ed25519" \
    nicco_scripts/embeddings/ ubuntu@10.67.22.XYZ:mapd-data/embeddings/

# 2 · sullo scheduler: la calibrazione, PRIMA della campagna (regola del repo).
#     Cinque minuti, e dice quanto costa davvero una misura su queste VM
source ~/pyvenv/bin/activate
python nicco_scripts/bench_cosine.py ~/mapd-data/embeddings \
    --only partizioni --ripetizioni 1 --out ~/mapd-out/bench_2_3_4

# 3 · la campagna, dentro tmux perche' sopravviva alla connessione che cade
tmux new -s bench
python nicco_scripts/bench_cosine.py ~/mapd-data/embeddings \
    --ripetizioni 3 2>&1 | tee ~/bench-cosine.log
#     ctrl-b d per staccarsi, `tmux attach -t bench` per rientrare

# 4 · il task stesso, per avere le classifiche da mostrare. --papers e' OBBLIGATORIO qui:
#     sul cluster i dati non stanno dentro la repo
python nicco_scripts/cosine.py ~/mapd-data/embeddings \
    --papers ~/mapd-data/silver/papers --out ~/mapd-out/2_3_4
```

**Prima di aspettare**, leggere il blocco di configurazione: deve dire `SSHCluster`, quattro
worker, indirizzi `10.67.22.x` e **non** `127.0.0.1` (§2e di `SETUP_CLOUDVENETO.md`).

Quanto dura, su 5 × `cloudveneto.large` (1 scheduler + 4 worker da 4 vCPU): **~30 minuti a
passata**, quindi ~1,5 ore per tre. È una stima ricavata dai tempi del Mac e dal rapporto
fra i core (3,6× misurato sul 2.3.2), non una misura: la calibrazione del passo 2 serve
esattamente a correggerla.

## L'algoritmo

```
embeddings/ ──► campione di N titoli ──► X normalizzata (N × 300)     [fuori dal cronometro]
                                              │
                                   X tagliata in k BLOCCHI
                                              │  scatter: una copia per worker
                                              ▼
                    piastrella (i,j) = X[i] @ X[j].T     solo i ≤ j, S è simmetrica
                                              │  k(k+1)/2 task indipendenti
                                              ▼
                    RIDUZIONE SUL POSTO: top 20, bottom 20, istogramma
                                              │  40 righe invece di milioni di numeri
                                              ▼
                    merge sul client ──► le due classifiche + l'istogramma
```

**Tre mosse, e la terza è quella che rende il task fattibile.**

**1 · Normalizzare una volta sola.** La similarità coseno è il prodotto scalare diviso per
le due lunghezze; dividendo ogni vettore per la sua lunghezza — costo `N × 300`, cioè
niente — i denominatori diventano 1 e resta **il solo prodotto scalare**.

**2 · «Tutte le coppie» è una moltiplicazione di matrici.** Con i vettori normalizzati per
riga, `S = X @ Xᵀ` contiene in `(i,j)` esattamente la similarità fra il titolo `i` e il
titolo `j`. Scritto come doppio ciclo Python sarebbero `N²/2` giri di interprete; scritto
così, NumPy lo passa a **BLAS** (la libreria in C/Fortran che moltiplica matrici): sul Mac
**180-620 miliardi di operazioni al secondo**, contro il milione scarso di un ciclo Python.
Sono quattro-cinque ordini di grandezza, ed è tutta la differenza fra fattibile e no.

**3 · Il risultato è più grande dell'input, quindi ogni task riduce sul posto.** A 100.000
titoli i vettori sono 120 MB ma `S` sarebbe **40 GB**, e sul corpus intero 3,7 TB: non si
scrive, e non serve. Ogni piastrella calcola, tiene le sue 20 coppie migliori e le sue 20
peggiori, conta l'istogramma e **butta il resto**. È un Map/Reduce nella stessa forma del
word count, ed è **esatto, non approssimato**: una coppia che non è fra le prime 20 della
propria piastrella non può essere fra le prime 20 di tutte.

### Perché `delayed`, e non le altre collezioni di Dask

| | perché no |
|---|---|
| **DataFrame** | «tutte le coppie» in forma dataframe è un *cross join*: 470 miliardi di righe **materializzate**. È l'anti-pattern esatto di questo problema |
| **Bag** | gli elementi sarebbero coppie di indici, non dati: il Bag verrebbe usato come lista di istruzioni. E aggiungerebbe `npartitions`, una seconda manopola che nei benchmark si confonde con `k` |
| **`dask.array`** | ha già `X @ X.T` a blocchi, ma calcolerebbe tutte e `k²` le piastrelle — non sa che `S` è simmetrica — e per la top-k con gli indici globali servirebbe `map_blocks` con `block_info` |

`delayed` descrive il calcolo per com'è: una lista di piastrelle indipendenti. Il cuore del
file sono sei righe.

### Qui i dati si replicano e si distribuisce il calcolo

È **l'opposto del word count**: là tanti dati e poco lavoro per riga, qui pochi dati e
un'enormità di lavoro (*compute-bound*). A 100.000 titoli i vettori sono 120 MB, quindi
`scatter` ne manda una copia a ogni worker e da lì in poi viaggiano solo le 40 righe di
risposta. Conseguenza pratica: **gli embedding servono solo sulla macchina da cui si
lancia**, non replicati su ogni VM come il modello FastText del 2.3.3.

## Quanti titoli, e perché non tutti

Il costo va come `N² × 300`. Con tutti i 969.021 titoli sono **4,7 × 10¹¹ coppie**: una
volta si farebbero (circa mezz'ora sul cluster), decine di volte in una campagna no.

| titoli | coppie | un core (Mac) |
|---|---|---|
| 40.000 | 0,80 mld | 15,5 s |
| 100.000 | 5,00 mld | 69,6 s |
| 969.021 | 470 mld | ~45 min |

Il default è **100.000, circa N/10**: abbastanza grande da misurare calcolo vero e non
coordinamento — l'errore che il 2.3.2 ha pagato — e abbastanza piccolo perché il punto «un
worker solo» della curva non si porti via mezz'ora. È un parametro (`--titoli`), non una
costante nascosta, e **resta fisso per tutta la campagna**: è la scala del problema, non una
curva.

Il campione prende **una quota da ogni file** con un seed fisso, invece dei primi N: i primi
100.000 starebbero tutti dentro `part.0`, e l'ordine dei file è quello con cui il 2.3.3 li
ha scritti, non una garanzia di mescolamento.

## Il muro di memoria: `k` non è solo granularità

Una piastrella di lato `L = N/k` costa **~12-14 L² byte**: la matrice (4 byte a valore) più
gli indici che `argpartition` e `triu_indices` producono (8 byte l'uno). E va moltiplicato
per i **thread** del worker, che calcolano piastrelle diverse insieme.

| titoli | `k` | lato | per task | × 3 thread |
|---|---|---|---|---|
| 40.000 | 4 | 10.000 | 1,2 GB | **3,6 GB** |
| 100.000 | 4 | 25.000 | 7,5 GB | 22 GB |
| 100.000 | 16 | 6.250 | 0,47 GB | 1,4 GB |
| 100.000 | 32 | 3.125 | 0,12 GB | 0,35 GB |

Il modello è **verificato**: a 40.000 titoli e `k=4` il worker ha segnalato *«Unmanaged
memory: 3.81 GiB»* contro i 3,6 GB previsti. Quindi i `k` bassi non sono lenti, sono
**irrealizzabili** — stesso fenomeno misurato nel 2.3.1 a `k=32`, da cui lo sweep del
benchmark parte da 4 e non da 1.

Per la stessa ragione lo sweep non gira in ordine crescente ma **dal riferimento verso i
bordi** (`16, 32, 64, 128, 8, 4`): dentro una forma di cluster le misure si susseguono, e un `k`
che sfonda fa intervenire la nanny sul worker. Se venisse per primo, le misure successive
girerebbero su un cluster appena riavviato — i fragili in fondo si portano via al massimo
se stessi. È la regola d'ordine già adottata dalla campagna del 2.3.1.

E per la stessa aritmetica lo sweep arriva fino a **128**: su worker da 7,1 GB con quattro
thread, a 100.000 titoli `k=4` e `k=8` sfondano, quindi senza il 128 la curva avrebbe tre
punti validi su cinque. A destra il muro non esiste — il picco va come `1/k²` — e si misura
l'altro estremo, i task troppo piccoli perché il calcolo copra il costo di schedularli.

## Benchmark

Tre curve. Le due obbligatorie — tempo vs **partizioni** (qui: i blocchi `k`) e tempo vs
**worker** — più una terza che in questo task **non è un contorno**.

Campagna del **2026-09-12** su **5 × `cloudveneto.large`** (1 scheduler + 4 worker da 4 vCPU
e 8 GB), 100.000 titoli, **6 passate intere** più la calibrazione. Baseline NumPy su un core:
**103,13 ± 1,18 s**.

### L'ipotesi sui thread, scritta prima di misurare — e confermata

Nel 2.3.1 e nel 2.3.2 il lavoro è codice Python, il **GIL** lo mette in fila e i processi
vincono: a parità di core `8×1` batte `4×2` di 1,33× e il secondo thread rende **1,01×**,
cioè niente. Qui il lavoro non è Python: è una moltiplicazione dentro BLAS, che è C e
**rilascia il GIL**. I thread dovrebbero quindi lavorare davvero in parallelo.

| thread per worker | core | secondi | guadagno |
|---|---|---|---|
| 1 | 4 | 49,36 ± 0,44 | |
| 2 | 8 | 29,67 ± 0,34 | **1,66×** |
| 4 | 16 | 21,16 ± 0,50 | 1,40× |

**Il secondo thread rende 1,66×** dove nel 2.3.2 rendeva 1,01×: l'ipotesi regge, e con
dispersioni sotto il 2% non è un caso. Da 1 a 4 thread: **2,33×** con 4× i core — la
saturazione (1,66 → 1,40) è coerente con un limite di banda di memoria, non di GIL.

Questi tre punti sono i più puliti della campagna: ognuno è la **sola** misura del proprio
cluster, quindi nessuno eredita lo stato di quello precedente (vedi sotto).

> **BLAS va messo a un thread, o la misura non significa niente.** BLAS si auto-parallelizza
> e di default prende tutti i core: quattro worker che moltiplicano insieme sarebbero sedici
> thread su quattro core a pestarsi i piedi, e la curva misurerebbe quel caos invece di
> Dask. `cosine.single_thread_blas()` lo impone via `pre-spawn-environ` — stesso aggancio di
> `MALLOC_TRIM_THRESHOLD_`, e **senza toccare `cluster.py`**, che è condiviso fra i quattro
> task. Verificato interrogando i worker: `OPENBLAS_NUM_THREADS=1` ci arriva.

### Partizioni

| `k` | 16 | **32** | 64 | 128 |
|---|---|---|---|---|
| secondi | 21,70 ± 0,72 | **20,98 ± 0,44** | 23,38 ± 0,40 | 38,16 ± 0,69 |

Minimo a `k=32`, ma **il vantaggio su `k=16` è del 3,4% a 2,1 σ: da solo non deciderebbe
niente**. Quello che decide il default è l'altro conto: a `k=32` il picco per task è
**quattro volte più basso** (0,12 GB contro 0,47), e a 100.000 titoli `k=8` e `k=4`
*sfondano* — `k=8` con `KilledWorker` su tutti e quattro i worker, `k=4` con il cluster che
non si era ancora ripreso dal punto precedente (la sua riga registra `worker=3`). Il default
è `32` perché costa uguale e sta **due passi** dal muro, non perché sia più veloce.

A destra `k=128` costa **1,82×** il minimo: 8.256 task in cui il calcolo non copre più il
costo di schedularli. Le due curve hanno quindi due cause diverse, non una sola forma a U.

### Worker

| worker (×4 thread) | 1 | 2 | 3 | 4 |
|---|---|---|---|---|
| core | 4 | 8 | 12 | 16 |
| secondi | 76,84 ± 6,37 | 40,89 ± 0,97 | 26,88 ± 0,34 | 20,98 ± 0,44 |

**3,66×** su quattro worker, cioè **92% di efficienza** sui processi. Contro un core solo:
**103,13 / 20,98 = 4,92×** su 16 core (31% — la saturazione già vista nella curva thread).

Il punto a un worker ha una dispersione dell'8% contro lo 0,3-2% di tutti gli altri
(70,9 → 87,2 s): è sempre la stessa macchina, ed è l'unica misura in cui un solo nodo porta
tutto il carico. Annotato, non spiegato.

### Processi contro thread: il confronto pulito

I due confronti che vengono spontanei dalle curve — `4×1` contro `1×4` (1,56×) e `4×2`
contro `2×4` (1,38×) — **sono sporchi**, e vale la pena dire perché: su questo cluster ogni
worker sta su una macchina diversa, quindi `4×1` usa quattro macchine e `1×4` una sola. Sono
quattro bus di memoria contro uno, non processi contro thread.

Il confronto pulito ha richiesto un comando in più: **8 processi da 1 thread contro 4
processi da 2 thread**, sulle stesse quattro macchine, con gli stessi 8 core
(`CORD19_HOSTS` raddoppiata, `--worker 8 --thread 1`).

| 8 core, stesse 4 macchine | secondi |
|---|---|
| **8 processi × 1 thread** | **25,85 ± 0,16** |
| 4 processi × 2 thread | 29,67 ± 0,34 |

**I processi vincono 1,15×** (+3,82 s, 23 σ). Reale ma piccolo, e va confrontato col resto
del progetto:

| task | processi a core costanti | il secondo thread rende |
|---|---|---|
| 2.3.1 word count | 2,03× | *rallenta* |
| 2.3.2 affiliazioni | 1,33× | 1,01× (inerte) |
| **2.3.4 cosine** | **1,15×** | **1,66×** |

È la conferma dell'ipotesi, e nella forma più interessante: i thread qui **funzionano** (il
secondo rende 1,66× invece di niente), e di conseguenza il vantaggio residuo del processo si
riduce a un ottavo di quello del word count. Non spariscono del tutto, e c'è una ragione per
crederlo una **sottostima**: con 8 worker lo `scatter` replica i blocchi su otto destinazioni
invece di quattro, quindi l'`8×1` paga più trasferimento del `4×2` — e vince comunque.
Quantificare quella correzione richiederebbe una misura che non abbiamo (lo stesso punto
rimisurato a caldo su 8 worker): **resta un thread aperto**, non un aggiustamento a
posteriori.

### Il quadro: le macchine contano più dei core

| configurazione | core | secondi | vs 1 core | efficienza |
|---|---|---|---|---|
| 1 worker × 4 thread | 4 | 76,84 ± 6,37 | 1,33× | 33% |
| 4 worker × 1 thread | 4 | 49,36 ± 0,44 | 2,08× | 52% |
| 2 worker × 4 thread | 8 | 40,89 ± 0,97 | 2,51× | 31% |
| 4 worker × 2 thread | 8 | 29,67 ± 0,34 | 3,46× | 43% |
| **8 worker × 1 thread** | **8** | **25,85 ± 0,16** | **3,97×** | **50%** |
| 3 worker × 4 thread | 12 | 26,88 ± 0,34 | 3,82× | 32% |
| 4 worker × 4 thread | 16 | 20,98 ± 0,44 | 4,89× | 31% |

Due letture che si vedono solo mettendo in fila tutto:

**Otto core su quattro macchine battono dodici core su tre** (25,85 contro 26,88). Il core
aggiunto come *thread* in un processo che ne ha già quattro rende così poco che tre macchine
piene perdono contro due terzi di macchina in più. Quello che scala non è il core: è la
macchina — il suo bus di memoria e la sua cache.

**L'efficienza si divide in due gruppi netti:** ~50% dove i worker hanno 1 thread, ~31% dove
ne hanno 4. Il limite non è Dask e non è il GIL (BLAS lo rilascia): è la banda di memoria,
condivisa fra i thread dello stesso nodo. Coerente con la saturazione della curva thread
(1,66× poi 1,40×) e con il fatto che la riduzione della piastrella — scorrere `L²` valori per
la top-20 e l'istogramma — è **memory-bound**, non compute-bound: sul Mac costava 3,7× la
moltiplicazione.

### Il difetto che questa campagna ha trovato in sé stessa

Lo **stesso** punto — 4 worker, 16 thread, `k=32` — è stato misurato due volte dentro lo
stesso cluster, e dà due numeri diversi:

| | secondi |
|---|---|
| come punto della curva **partizioni** (2° del suo cluster, blocchi nuovi) | 20,98 ± 0,44 |
| come punto della curva **worker** (5° del suo cluster, `k=32` già girato) | 17,41 ± 0,71 |

**3,57 s di differenza, il 17%, a 10,5 σ.** Non è rumore.

> **La spiegazione che avevamo dato è stata falsificata dalla campagna del 2026-09-13.**
> Avevamo attribuito quei 3,57 s allo `scatter`: Dask nomina i dati scatterati con l'hash
> del contenuto, quindi alla seconda misura dello stesso `k` i blocchi sarebbero già sui
> worker e il trasferimento non avverrebbe. Il conto tornava (~480 MB su 1 Gb/s ≈ 3,8 s).
> **È sbagliata.** Misurando lo stesso punto tre volte di fila nello stesso cluster
> (`--ripeti-punto 3`), la prima misura **non** è più lenta delle altre, a nessun numero di
> worker:
>
> | worker | 1 | 2 | 3 | 4 | 8 |
> |---|---|---|---|---|---|
> | freddo − caldo (s) | −1,95 | −0,05 | +0,36 | −0,05 | +0,11 |
> | σ | 0,9 | 0,1 | 0,5 | 0,1 | 0,9 |
>
> Tutte compatibili con zero. **Lo `scatter` di 120 MB non costa niente di misurabile**, e
> la previsione «a 8 worker deve costare il doppio» è smentita dal dato che avrebbe dovuto
> confermarla. Il costo di distribuire i dati, in questo task, non è una voce di spesa.

Resta quindi da spiegare **perché ieri** quel punto fosse più veloce. L'unico effetto di
questo tipo che la notte ha trovato riguarda il **numero di task**, non i dati:

| `k` | 16 | 32 | 64 | 128 |
|---|---|---|---|---|
| 1ª misura | 21,83 | 20,49 | 23,95 | **39,05** |
| misure 2ª-3ª | 21,81 | 21,00 | 22,90 | **34,88** |

A `k=128` — 8.256 task — la prima esecuzione costa **il 10,7% in più**; a `k=16` e `k=32`
non succede nulla. Ieri il punto "veloce" girava **dopo** `k=64` e `k=128` nello stesso
cluster, cioè dopo ~10.000 task. L'ipotesi nuova è quindi che a scaldarsi non siano i
*dati* ma lo *scheduler* o l'allocatore dei worker, e che serva un volume di task per
vederlo. **Non è verificata**, e la misura che la deciderebbe è corta: nello stesso cluster,
`k=128` e subito dopo `k=32`, e si guarda se quel `k=32` vale 21 o 17.

**Conseguenza pratica invariata:** lo speedup sui worker resta **3,66×** e non 4,41×,
perché i due numeri di quel punto non sono confrontabili fra loro qualunque sia la causa.

### La campagna notturna del 2026-09-13

`notte_2_3_4.sh` è l'elenco dei comandi della seconda notte — nessuna logica, solo l'ordine
giusto — e trasforma in misure le tre domande che la prima campagna ha lasciato aperte:

| esperimento | domanda | come |
|---|---|---|
| **1 · filtri** | la classifica in cima è degenere: i filtri la rendono unica? | 5 configurazioni di `--min-parole`/`--solo-unici`, con `--top 5000` per **contare** i pari merito |
| **2 · scatter** | quanto costa distribuire i dati, e scala coi worker? | `--ripeti-punto 3`: la 1ª misura paga il trasferimento, le altre no. Ripetuto per 1, 2, 3, 4 e **8** worker, e per ogni `k` |
| **3 · un worker** | la dispersione dell'8% è la macchina o l'accensione? | lo stesso punto su **ciascuna** delle 4 macchine (`CORD19_HOSTS`), 8 misure a cluster fermo × 5 cluster |

L'opzione `--ripeti-punto N` serve a due e tre insieme, e la colonna `misura` del CSV è la
chiave di lettura: `misura=0` è la prima volta che quel `k` gira in quel cluster e paga lo
scatter, `misura>0` no. La dispersione fra le misure calde è **varianza a cluster fermo**,
quella fra `ripetizione` diverse include l'accensione: separarle è tutto il punto.

**La previsione da falsificare**, scritta prima: lo scatter deve costare ~2× a 8 worker che
a 4, perché il traffico è proporzionale alle destinazioni, e **non** deve dipendere da `k`,
perché i megabyte trasferiti sono gli stessi. Indizio a favore, già raccolto: sul Mac
`misura=0` **non** è più lenta delle altre (1,12 contro 1,15 s) — su localhost non c'è rete.

Durata stimata **5,1 ore** per **274 misure**, dai tempi del 2026-09-12. L'ordine va dal più
importante al più lungo: se la notte si interrompe, le risposte 1 e 2 sono già al sicuro.

### I risultati della notte (274 misure, 307 minuti)

**La dispersione a un worker è del calcolo, non dell'accensione.** Otto misure a cluster
fermo × 5 cluster, su ciascuna delle quattro macchine:

| nodo | media | σ | CV |
|---|---|---|---|
| 1 | 80,24 | 4,53 | 5,6% |
| 2 | 85,42 | 4,93 | 5,8% |
| 3 | 83,64 | 3,67 | 4,4% |
| 4 | 84,46 | 2,56 | 3,0% |

Le due domande hanno risposta netta. **Non è una macchina difettosa**: le quattro stanno
entro il 6,5% l'una dall'altra, e tutte disperdono. **Non è l'accensione del cluster**: la
dispersione *dentro* un cluster fermo (3,19 s in media) è **più grande** di quella *fra*
cluster diversi (2,21 s) — se fosse l'avvio sarebbe il contrario. E non c'è tendenza: dalla
1ª all'8ª misura si passa da 84,77 a 82,26 s, dentro il rumore.

Resta quindi che **il punto a un worker è intrinsecamente più rumoroso**: CV 3-5,6% contro
lo 0,3-2% di tutti gli altri. È l'unica configurazione in cui un solo nodo porta tutto il
carico, quindi il rumore del suo sistema operativo e della sua banda di memoria non viene
mediato su quattro macchine. Annotato **con la causa ristretta**, non più solo annotato.

## Correttezza: verificata una volta, non a ogni run

Con 2.000 titoli si calcola `S` **intera** in NumPy sul client e si confrontano le
classifiche con quelle prodotte dal grafo Dask. A `k = 3, 8, 16`: **top-20 e bottom-20
identiche**, 1.999.000 coppie contate su 1.999.000 attese. È la dimostrazione che la
divisione in piastrelle non perde né duplica coppie. Il controllo vive qui, non nel `.py`:
stessa regola dell'invariante Map/Reduce del 2.3.1.

A ogni run resta la versione a costo zero: lo script stampa **coppie contate contro coppie
attese**, e devono coincidere.

> Serve un dettaglio per farle coincidere: il coseno di due vettori normalizzati sta in
> `[-1, 1]` per definizione, ma **in float32 due vettori identici danno 1,0000001**. Senza
> il taglio a 1, quelle coppie cadono fuori dal range dell'istogramma e spariscono dal
> conteggio — è successo: 4 coppie su 12.497.500.

## Cosa dicono i numeri

Su 3.000 titoli veri (4,5 milioni di coppie):

| | |
|---|---|
| minimo | **−0,0037** |
| mediana | 0,688 |
| massimo | 1,0000 |
| coppie negative | **0,0001 %** |

**La scala teorica −1…+1 non è la scala reale.** Questi vettori sono medie di vettori di
parole di articoli scientifici tutti sullo stesso argomento: stanno in un cono stretto dello
spazio, e le similarità si ammassano fra 0,3 e 0,9. Il «perfect dissimilarity» a −1 del
testo non esiste in questo corpus, e i «più dissimili» stanno intorno a zero.

### Due artefatti, misurati

**Titoli duplicati.** Il 28,0 % dei paper ha un titolo non unico (`is_title_unique` in
`silver/papers`; 31,1 % nel campione), quindi la cima della classifica è fatta di coppie a
similarità 1,0000 che sono **lo stesso titolo depositato due volte**.

**Titoli ridotti a una parola.** 5.632 titoli (0,58 %) hanno `n_words = 1`, cioè una sola
parola trovata nel modello. Il vettore del titolo *è* il vettore di quella parola, e due
titoli che non c'entrano niente risultano identici al 100 %:

```
+1.0000  Mitteilungen der DGPPN 9/2020  ||  Pathophysiologie der Leberkrankheiten
+1.0000  Grundbegriffe der Immunologie  ||  Pathophysiologie der Leberkrankheiten
```

Tre titoli tedeschi di cui il modello inglese conosce **solo `"der"`**. Con `n_words ≤ 2`
sono il 2,24 % dei titoli, con `n_words ≤ 3` il 4,96 %.

**La classifica in cima non è unica, ed è il problema peggiore dei due.** Ci sono **4.851
coppie sopra 0,98** e le venti consegnate valgono **tutte esattamente 1,000000**: quali venti
escano è una scelta fra pari merito, e dipende dall'ordine in cui i task finiscono. Il run
sul cluster e quello sul Mac consegnano infatti **venti coppie diverse, tutte a 1,000000** —
non è un errore, è la domanda che è mal posta finché gli artefatti restano dentro.

> **Due run non sono bit-identici, e non devono esserlo.** Confrontando cluster e Mac sullo
> stesso campione (seed fisso), **30 bin su 100 dell'istogramma differiscono di ±1 o ±2
> coppie**, con differenza totale **esattamente zero**. La somma in virgola mobile non è
> associativa, e BLAS scambia l'ordine degli addendi secondo il blocking, la larghezza SIMD e
> la libreria: una coppia che vale 0,28 al bit può cadere da una parte o dall'altra del
> confine. Sono 2 coppie su 5 miliardi, ~4 × 10⁻⁹. La cosa da tenere: **l'invariante di
> conteggio coincide alla cifra** (4.999.950.000 su entrambe le macchine), mentre le *singole*
> classifiche degeneri no. Il calcolo distribuito è riproducibile entro l'errore di macchina,
> non bit a bit — affermare la seconda cosa sarebbe falso.

### Cosa succede filtrando: misurato, cinque configurazioni

| filtro | coppie a 1,000000 (su 5.000) | a pari merito con la 20ª | similarità minima |
|---|---|---|---|
| nessuno | 4.131 | 2.326 | −0,1475 |
| `--min-parole 2` | 3.357 | 1.758 | −0,1132 |
| `--min-parole 3` | 2.932 | 1.593 | −0,0542 |
| `--solo-unici` | 2.162 | 1.246 | −0,1661 |
| entrambi | **1.902** | **973** | −0,0827 |

I filtri dimezzano la degenerazione, **ma non la eliminano**: con entrambi attivi restano
1.902 coppie a 1,000000 e 973 a pari merito con la ventesima. La classifica resta una
scelta fra pari.

**E il motivo è il risultato più interessante della notte.** Guardando i titoli interi
delle coppie che sopravvivono a entrambi i filtri: **zero su duemila sono identici come
stringa**. Differiscono per un punto finale, un trattino tipografico, un apostrofo curvo:

```
"COVID-19 and the epistemology of epidemiological models…": comment from the editors.
"COVID-19 and the epistemology of epidemiological models…": comment from the editors
Learning and Leading: The Impact of COVID‐19 in Perioperative Areas     (trattino U+2010)
Learning and Leading: The Impact of COVID-19 in Perioperative Areas     (trattino ASCII)
```

Il filtro sui duplicati **ha funzionato**: i titoli ripetuti alla lettera sono spariti.
Quello che resta sono lo **stesso paper depositato due volte** da fonti diverse (PMC, WHO,
Elsevier) con differenze di composizione tipografica — che `is_title_unique` non vede,
perché `title_norm` non le unifica, e che l'embedding non può vedere, perché la
punteggiatura non entra nella media dei vettori di parola.

Quindi la lettura da dare al risultato **cambia di segno**: la cima della classifica non è
rumore da ripulire, è **il corpus che contiene migliaia di paper gemelli**, e la similarità
coseno li trova — che è esattamente il lavoro per cui la si usa. La domanda «quali sono i
titoli più simili» ha come risposta onesta *«questi ~1.900 sono lo stesso paper due
volte»*, e solo dopo averlo detto ha senso chiedersi quali siano i più simili **fra paper
diversi**.

**Nessuno dei due filtri è attivo per default.** Se quelle coppie siano *il risultato* o *il rumore*
è una decisione di analisi, e il layer `silver` segnala senza decidere
(`PROJECT_CONTEXT.md` §4): il filtro si aggiunge dopo averne discusso, non per abitudine.
Le due colonne che servono ci sono già (`is_title_unique`, `n_words`).

## Fuori scopo, deciso

- **La matrice `S` salvata**: 40 GB a 100.000 titoli, e la consegna non la chiede.
- **Il vicino più simile di *ogni* paper**: costerebbe poco (un massimo per riga invece che
  sull'intera piastrella) ed è ciò che farebbe un sistema di raccomandazione — ma il testo
  chiede *«some of the most similar»*, non una colonna per paper.
- **Clustering dei titoli.**
- **La curva tempo vs N**: mostrerebbe la `N²` disegnata, ma è una quarta curva.
