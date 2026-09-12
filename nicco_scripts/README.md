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

### L'ipotesi sui thread, scritta prima di misurare

Nel 2.3.1 e nel 2.3.2 il lavoro è codice Python, il **GIL** lo mette in fila e i processi
vincono: a parità di core `8×1` batte `4×2` di 1,33× e il secondo thread rende **1,01×**,
cioè niente. Qui il lavoro non è Python: è una moltiplicazione dentro BLAS, che è C e
**rilascia il GIL**. I thread dovrebbero quindi lavorare davvero in parallelo, e avere un
vantaggio in più — condividono la memoria, quindi i vettori esistono in una copia sola
invece che in una per processo.

**Prova locale (Mac, 4 worker, 40.000 titoli, `k=16`):**

| thread per worker | secondi | |
|---|---|---|
| 1 | 4,53 | |
| 2 | 2,55 | **1,78×** |
| 4 | 2,21 | 2,05× |

Il secondo thread rende **1,78×** dove nel 2.3.2 rendeva 1,01×. L'ipotesi regge sul Mac;
sul cluster va rimisurata.

> **BLAS va messo a un thread, o la misura non significa niente.** BLAS si auto-parallelizza
> e di default prende tutti i core: quattro worker che moltiplicano insieme sarebbero sedici
> thread su quattro core a pestarsi i piedi, e la curva misurerebbe quel caos invece di
> Dask. `cosine.single_thread_blas()` lo impone via `pre-spawn-environ` — stesso aggancio di
> `MALLOC_TRIM_THRESHOLD_`, e **senza toccare `cluster.py`**, che è condiviso fra i quattro
> task. Verificato interrogando i worker: `OPENBLAS_NUM_THREADS=1` ci arriva.

### Le altre due curve (stessa prova locale)

| `k` | 4 | 8 | **16** | 32 | 64 |
|---|---|---|---|---|---|
| secondi | 5,52 | 2,71 | **2,22** | 2,48 | 3,49 |

Minimo interno a `k=16`, che è il default del task. A `k=4` si paga il muro di memoria
(2,5× il minimo), a `k=64` la frammentazione in task troppo piccoli.

| worker | 1 | 2 | 3 | 4 |
|---|---|---|---|---|
| secondi | 6,01 | 3,36 | 2,53 | 2,47 |

**2,43×** su quattro worker. Il baseline NumPy su un core è **15,51 s**, quindi il punto
migliore (2,21 s) vale **7,0×** — ma sono numeri del Mac: valgono come prova generale, non
come risultato. La campagna sul cluster è ancora da lanciare.

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

**Nessuno dei due è filtrato, per ora.** Se quelle coppie siano *il risultato* o *il rumore*
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
