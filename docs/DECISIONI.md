# Registro delle decisioni

**A cosa serve.** Le scelte di questo progetto sono state prese una volta, discusse e
spesso **misurate**. Questo file esiste perché non vengano rimesse in discussione ogni
volta che qualcuno — persona o agente — riapre il repo con il contesto sbagliato.

**Come si usa.**

- **Parte A — regole in vigore.** Cosa vale *adesso*. Una riga per decisione, il perché in
  mezza riga, e dove sta la giustificazione completa. Si legge prima di proporre qualsiasi
  cosa. Si aggiorna **in luogo**: se una regola cambia, si riscrive quella riga.
- **Parte B — storico.** Cosa è stato deciso e quando. **Si appende in fondo e non si
  riscrive mai**, nemmeno quando una decisione viene superata: serve a ricostruire il
  ragionamento, non solo il risultato.

**Regola sopra le regole:** se una decisione qui dentro ti sembra sbagliata, può darsi che
lo sia — ma serve un **fatto nuovo** per cambiarla, non un'opinione. E se la cambi, la
riscrivi qui.

---

# Parte A — Regole in vigore

## Metodo di lavoro

- **La semplicità è un requisito, non un gusto.** È un esercizio universitario da
  discutere all'orale: ogni riga va saputa spiegare. La complessità che non si sa
  giustificare è un difetto. → *Precedente costoso: `bench.py`, vedi sotto.*
- **La documentazione si aggiorna nella stessa sessione in cui l'informazione nasce**, non
  "dopo". Sviluppo che avanza e documentazione ferma è la causa diretta del disastro
  `bench.py`.
- **Ogni regola di pulizia dei dati si aggiunge dopo averne misurato la necessità**, mai
  per abitudine o "perché si fa così". → `local/NOTES.md`
- **Un file `.py` con le funzioni, un notebook che lo importa.** Mai codice duplicato tra i
  due, mai `subprocess` che lancia script. Il `.py` è la fonte di verità, il notebook
  spiega. → `Giulia/old/README.md`
- **I notebook non si eseguono.** Si lavora da terminale, si lanciano `.py`. Niente VS Code
  remoto, niente Jupyter sulle macchine del cluster.
- **Il codice dev'essere spiegabile all'orale da chi lo consegna.** È il motivo per cui il
  primo word count è stato riscritto da zero: 2.466 righe in cui l'algoritmo (~30) era
  sepolto sotto flag, controlli difensivi e `try/except` che inghiottivano gli errori.
  → `Giulia/old/README.md`

## Dati

- **Niente database server: Parquet a due layer, `bronze/` e `silver/`,** tutto keyed su
  `cord_uid`. Un DB sarebbe un anti-pattern in un esercizio di calcolo distribuito.
  → `PROJECT_CONTEXT.md` §2
- **Il layer `silver` corregge errori oggettivi e AGGIUNGE FLAG; non prende decisioni di
  analisi.** I duplicati sono segnalati, non rimossi; le reference sono segnalate, non
  scartate; nessuna tokenizzazione. La decisione di analisi spetta al task.
  → `PROJECT_CONTEXT.md` §4, `DATA_DICTIONARY.md`
- **I dati non si rigenerano.** `data/` sul Mac e `~/mapd-data/silver/` sul cluster sono
  l'output validato del run completo. Per provare la pipeline: `CORD19_SAMPLE=N`.
- **La conversione JSON→Parquet è conclusa e non fa parte dell'assignment.** Non va
  rieseguita, e il dump grezzo (`archive/`, il volume da 200 GB) serve solo a lei.
- **Gli embedding precomputati di CORD-19 non si usano** — scelta esplicita: il task 2.3.3
  chiede di calcolarli con un modello FastText. → `PROJECT_CONTEXT.md` §3
- **Le affiliazioni si leggono solo dal ramo `pdf_json`**: nei `pmc_json` sono popolate
  ~0%. → `PROJECT_CONTEXT.md` §3
- **Niente conteggi assoluti negli assert.** Dump diversi danno numeri diversi; si
  verificano garanzie strutturali (unicità, integrità referenziale, invariante prefer-pmc).

## Cluster

- **Un cluster per persona, non uno condiviso.** Un volume OpenStack si attacca a una sola
  macchina, quindi condividere i dati via NFS obbligava tutti e quattro a lavorare sulla
  stessa installazione: in quattro non è praticabile. → `PROJECT_CONTEXT.md` §5
- **I dati si replicano su ogni macchina, non si condividono.** Il `silver/` sta sul disco
  di ogni VM, ci arriva dall'immagine snapshot. *Costo accettato:* N copie da tenere
  allineate; se il `silver` cambia, va rifatta l'immagine.
- **La prima macchina fa solo da scheduler.** Ci girano già il coordinatore e il processo
  da cui lanci; su 4 GB un worker che sfonda il tetto porterebbe giù il run intero; e i
  benchmark si interpretano solo se i worker sono intercambiabili.
- **Macchine tutte della stessa taglia** nello stesso cluster: 3 minimo, 4–5 di solito.
  Mescolarle rende il più piccolo il freno di tutti e i tempi inspiegabili.
- **Le macchine non si cancellano a fine sessione** (i risultati stanno sui loro dischi) —
  ma i risultati si **scaricano** sul portatile. *Regola ribaltata rispetto alla fase NFS.*
- **Niente Docker**, vincolo del corso: il cluster è reale, installato a mano.
- **Il security group `pod-students` non si tocca mai**: è condiviso con tutto il corso.
- **Mai una chiave privata su una VM o in chat.** Si condivide solo la parte pubblica, e
  sempre in append (`>>`). → `SETUP_CLOUDVENETO.md` §4
- **Niente costanti assolute di RAM o thread.** `memory_limit` è una *frazione* della RAM
  del nodo, `nthreads` non è imposto: le VM cambiano taglia tra una sessione e l'altra e un
  "7GB" scritto a mano su una VM da 3,8 GB viene ignorato in silenzio. → `local/NOTES.md` v6
- **Gli host stanno in `cluster.txt`, mai nel codice.** Il file è git-ignored e si cerca
  nella radice della repo. → `PROJECT_CONTEXT.md` §6
- **Niente fallback silenziosi:** la configurazione del cluster si stampa sempre. Un run su
  una macchina sola sembra un cluster lento e te ne accorgi a sessione bruciata.
- **I percorsi relativi si risolvono sulla radice della repo**, non sulla cartella da cui
  si lancia.
- **Niente di prezioso dentro la repo:** i risultati vanno in `~/mapd-out/`, perché sul
  cluster la repo si cancella e si riclona. *Decisa il 2026-08-13, non ancora applicata ai
  default del codice.*

## Analisi — task 2.3.1 (word count)

- **Dask Bag, non DataFrame:** è la struttura raccomandata dal testo dell'assignment.
- **Map/Reduce in due fasi, fedele allo spec, anche se costa 2,8×** rispetto alla riduzione
  diretta. È l'algoritmo che il testo *definisce*; il costo è stato misurato invece che
  subito. → `local/NOTES.md` v1
- **L'invariante Map/Reduce è verificato una volta, non a ogni run.** Le occorrenze prima
  e dopo la riduzione coincidono (785.753.529 sul corpus intero): il controllo vive nel
  notebook, che è il posto di una dimostrazione. Nello script era un'opzione `--check` che
  raddoppiava il tempo e non è mai stata la strada normale. *Deciso il 2026-08-13.*
- **In `word_count.py` c'è UNA sola fase Map**, quella dello spec. Le due varianti da
  esperimento (una entry per occorrenza, una per partizione) sono uscite dal file: sono i
  bracci di una misura, e vivranno nel file del benchmark se quella misura si farà.
  *Deciso il 2026-08-13.*
- **Si riduce sulla PAROLA, non su `(documento, parola)`**, contando prima dentro la
  partizione (il *combiner* del MapReduce classico). La chiave sbagliata cresce col numero
  di documenti e fa morire il job sul 10% del corpus. → `local/NOTES.md` v4
- **`split_out=16` è il default, non una via di fuga.** `foldby` ha una coda seriale: un
  solo task macina il vocabolario mentre gli altri worker stanno fermi. Con `split_out` il
  run è **4,1× più veloce su worker grandi la metà**, a parità di risultato.
  → `local/NOTES.md` v6
- **La tokenizzazione tiene trattini, cifre e lettere greche**, con normalizzazione NFKC e
  rimozione del preambolo LaTeX del PMC. Con un `[a-z]+` ingenuo `covid-19` — la **seconda
  parola del corpus** — semplicemente non esiste. → `local/NOTES.md` v2
- **Due insiemi separati di parole ignorate:** le parole funzione della lingua e gli
  artefatti di *questo* corpus (`et`/`al`, `fig`, `table`). **Non si tolgono parole di
  contenuto**, nemmeno generiche come `study` o `data`: sarebbe una decisione di analisi,
  non di pulizia. → `local/NOTES.md` v2
- **`is_reference_like` NON si filtra più: il word count conta tutti i paragrafi del
  `silver`.** Era l'1,90% dei paragrafi e lo 0,74% del testo, e non muoveva nessuna parola
  nella top-20 (verificato di nuovo dopo la rimozione: stesse parole, stesso ordine).
  Costava un'opzione da riga di comando e un ramo in due lettori diversi: una differenza
  che non si vede non paga quel codice. La colonna resta nel `silver`.
  *Deciso il 2026-08-13, sostituisce «si filtra per definizione» → `local/NOTES.md` v3.*
- **Il numero di partizioni dipende dalla forma della query, non solo dai file.** Tolto il
  filtro, la stessa cartella di 1979 file è passata da **990 a 1979** partizioni eseguite.
  Non è un parametro: si legge dal `print` dello script prima di interpretare un
  benchmark. → `Giulia/README.md`, `PROJECT_CONTEXT.md` §8.6
- **Direzione scelta e non ancora realizzata:** al testo non inglese si danno le sue
  stop-word (tedesco, francese, spagnolo, portoghese), invece di scartare quei paper.
  → `local/NOTES.md`, "Da vedere in futuro"

## Benchmark

- **I benchmark si fanno sul corpus vero, non sul campione.** Sul campione una macchina
  batte quattro, perché a quella scala il tempo è tutto overhead di coordinamento.
  → `local/NOTES.md` v5
- **Le curve obbligatorie sono DUE** — tempo vs numero di partizioni, tempo vs numero di
  worker. Il testo però dice «at least», e «executors/**processing units**» si legge anche
  come *thread*: si aggiunge quindi **una misura secca sui thread per worker** (tre punti,
  non una curva) e **una riga sola** per il `foldby` sul cluster vero. Niente altro.
  *Rifatti da zero il 2026-08-13, riscritti il 2026-08-14.*
- **Il cluster dei benchmark è 5 × `cloudveneto.large`** (1 scheduler + 4 worker da 4 vCPU
  e 8 GB, cioè 7,1 GB di `memory_limit`). **Il muro sulle partizioni lo mette la memoria
  PER SLOT, e la soglia è NETTA:** la nanny uccide sopra `0,95 × memory_limit` = 6,74 GB, e
  a `k=32` il worker arriva a 6,41 — ci passa sotto tre volte su tre, a `k=16` no.
  Dimezzare la memoria per worker sposta il muro di un passo in `k` (7,1 GB → `k=32`,
  3,4 → `128`, 1,7 → `256`), perché il picco di un task va come 1/k.
  *Misurato il 2026-08-16 col picco per worker nel CSV. Sostituisce «sotto `k=128` il job
  non è eseguibile, il picco è ~15× il testo, ×4 thread = 16,3 GB»: sbagliata tre volte —
  il muro è a `k=32` con un thread, l'espansione si ferma a ~13,8×, e quattro thread
  moltiplicano il picco per 1,57 e non per 4 perché non capitano insieme.*
  → `Giulia/misura_ram.py`, `SETUP_CLOUDVENETO.md` §6
- **Una riga del CSV = una misura = un cluster nuovo,** dove il logoramento esiste: un
  worker che ha già macinato milioni di stringhe trattiene RSS per frammentazione glibc,
  quindi riusando un cluster per nove punti l'ultimo misurerebbe partizionamento **più**
  usura. Su un job da secondi e 34 MB (2.3.2) quel logoramento non può avvenire e
  l'accensione dominerebbe la campagna: là si usa **un cluster per numero di worker**.
  *Qualificata il 2026-09-10.* → `PROJECT_CONTEXT.md` §7 Atto 3, `Federico/README.md`
- **Le ripetizioni sono PASSATE INTERE della campagna** (`--ripetizioni N`), non misure
  consecutive dello stesso punto. Costa uguale e dice di più: fra due ripetizioni dello
  stesso punto passano ore, quindi la dispersione comprende la variabilità della macchina
  nella giornata e non solo quella di due run attaccati; e una giornata interrotta lascia
  **una campagna completa** invece di mezza curva misurata tre volte. Tutto è ancorato a un
  unico punto di riferimento (tutti i worker, `k=256`, `split_out=16`), che appartiene a
  tutte le curve: nei grafici l'asse x si legge dalle colonne di **stato** (`partizioni`,
  `worker`, `thread`) e non da `valore`. **Ma quelle colonne si verificano, non si
  credono:** fino al 2026-08-16 le riempiva `client.scheduler_info()["workers"]`, che
  **sotto-conta quando più worker stanno sulla stessa macchina** — otto worker veri, ne
  dichiara cinque, e non si corregge né dopo un refresh né dopo `wait_for_workers`. Adesso
  si usa `client.nthreads()`; i CSV di `bench-8x1` e `bench-16x1` restano marchiati
  «5 worker» e vanno letti sapendolo.
  *2026-08-16, sostituisce «riferimento ripetuto 3 volte, 1 altrove».*
- **L'ordine della campagna è progettato per una giornata che può interrompersi:**
  riferimento → **partizioni dal centro verso i bordi** → worker → thread → foldby. Le
  partizioni prima dei worker perché sono la curva col minimo, e il punto a un worker solo
  costa da solo mezz'ora; i `k` bassi in fondo perché sono i più lenti e i più fragili.
  *2026-08-16, sostituisce l'ordine «riferimento → worker → partizioni».*
- **La campagna non si lancia mai senza averla prima calibrata** (`--only riferimento`,
  ~30 min): le stime dei tempi vengono da un solo dato del Mac e possono sbagliare del
  doppio.
- **Lo sweep sulle partizioni tiene i dati fissi**, e `k` si ottiene **raggruppando i file
  in lettura**, non con un `repartition` a valle: quello lascerebbe la lettura sempre alla
  stessa granularità e metterebbe nel cronometro il costo della ricucitura. Fette di corpus
  crescenti misurano la quantità di dati, non il partizionamento.
- **Il numero di worker si cambia accendendo un cluster nuovo** con i primi *N* host di
  `cluster.txt`, non con `scale()`: su `SSHCluster` si può solo scendere, il che obbliga a
  ricordarsi un ordine di esecuzione ed è già costato una campagna misurata su un worker
  solo. Costa un minuto a punto e ogni misura parte da uno stato pulito.
- **Si cronometra il lavoro che si consegna**, scrittura del vocabolario compresa. Il crash
  dell'11 agosto stava proprio nella scrittura, cioè nel pezzo che il vecchio benchmark
  saltava misurando la sola `topk`.
- **Lo script misura, il notebook disegna.** Il `.py` scrive un CSV riga per riga; i
  grafici stanno nel notebook. Così una campagna di ore non dipende da un notebook aperto,
  e per rifare un grafico non si rioccupa il cluster.
- **`performance_report` sempre con `mode="inline"`**: senza, l'HTML scarica BokehJS da un
  CDN e resta bianco appena lo si apre senza internet — cioè dopo averlo copiato giù dal
  cluster, che è l'unico momento in cui lo si guarda.

---

# Parte B — Storico

---
## 2026-08-13

**Decisioni + perché**
Architettura cluster rifatta: via NFS e volume condiviso (obbligava 4 persone su un solo
cluster), un cluster per persona con dati replicati da snapshot su ogni macchina; VM non
più cancellate a fine sessione perché i risultati vivono sui loro dischi; `~/mapd-out/`
come casa dei risultati, fuori dalla repo che è usa-e-getta. Documentazione riorganizzata
per causa dichiarata: `bench.py` è diventato ingiustificabile perché lavorava su documenti
vecchi e su decisioni mai scritte — da qui questo registro e la regola "la documentazione
si aggiorna nella stessa sessione".

**Collegamenti toccati**
`CLAUDE.md` (riscritto: mappa + regole + come parlare con Federico) → rimanda a
`docs/DECISIONI.md` (nuovo) · `docs/SETUP_CLOUDVENETO.md` (ora runbook operativo) ·
`docs/PROJECT_CONTEXT.md` (§5 riscritta, §8 estesa a 12 regole, §9 riscritta) ·
`DATA_DICTIONARY.md` rimisurato sui dati veri (picco nel 2021 non nel 2020; 272.191 titoli
non unici; `is_reference_like` 1,90%) → `Giulia/README.md` allineato · comando `/wrap`
ripuntato su questo file ·
memoria privata dell'agente ripulita (32 KB che duplicavano il repo → 5 voci che ci
puntano; regola: se memoria e repo divergono, vince il repo).

**Thread aperti**
Rifare i benchmark da zero (priorità 1) · riscrivere il 2.3.2, oggi non distribuito ·
applicare `~/mapd-out` ai default del codice · verificare i core per flavor sulla
dashboard · `daniele/SOLUZIONE_2_3_3.md` cita ancora 406.211 righe per `silver/papers`
(vere: 970.836) — **lasciato apposta**, è il documento di un compagno.

---
## 2026-08-14

**Decisioni + perché**
Impalcatura benchmark buttata (948 righe, **zero misure prodotte**) → `cluster.py` accende
il cluster e basta, `bench_word_count.py` è una campagna sola che gira una notte da sola;
lo script misura e scrive un CSV, il notebook disegna. `k` partizioni si ottiene
raggruppando i file (`repartition` metterebbe la ricucitura nel cronometro) e il numero di
worker accendendo un cluster nuovo coi primi *N* host (su `SSHCluster` `scale()` scende e
basta, e obbliga a ricordare un ordine). Da `word_count.py` tolti `--check`, il filtro
`is_reference_like` (top-20 identica) e i due Map da esperimento: erano bracci di misure
mai eseguite. **Regole sostituite in Parte A:** invariante opt-in, `is_reference_like` "si
filtra per definizione", "`bench.py` in revisione", sweep partizioni via `repartition`.

**Collegamenti toccati**
`cluster.py` (ex `bench.py`, +`available_workers`) ← `word_count.py` e
`bench_word_count.py` → CSV in `~/mapd-out/bench/` → `word_count.ipynb` §9, che ora
disegna (grafici usciti dall'impalcatura) · README, PROJECT_CONTEXT §2/§8.6/§9,
DATA_DICTIONARY, CLAUDE.md allineati.

**Thread aperti**
Lanciare la campagna sul cluster (mai misurato per davvero: solo campione) · verificare
che la porta 8786 si liberi fra un cluster e il successivo (pausa 10 s, non provata su
SSH) · misurato: `word_count.py` calcolava tutto **due volte**, risolto con `persist()` ·
partizioni passate da 990 a 1979 togliendo il filtro — dipendono dalla forma della query ·
2.3.2 ancora non distribuito.

---
## 2026-08-14 (sera) — il disegno della campagna, e il codice che lo esegue

**Decisioni + perché**
Letti i flavor veri dalla dashboard: **medium = 2 core, large = 4, xlarge = 8** (chiude la
domanda aperta in `SETUP` §6). Scelto **5 × `cloudveneto.large`**: 4 core danno tre punti
alla misura sui thread, 8 GB fanno completare anche `k=4`. Il testo del corso dice «at
least», e «processing units» si legge anche come *thread*: aggiunta una **misura secca sui
thread** con l'ipotesi scritta prima di misurare (Map = regex + Counter in Python puro →
tiene il GIL → `T(4)/T(1) ≈ 0,6`, non `0,25`), più **una riga** per il `foldby` sul cluster
vero. Ripetizioni: **3 su un solo punto di riferimento** per misurare il rumore una volta,
1 altrove. Ordine progettato per una notte interrompibile: riferimento → worker →
partizioni **dal centro ai bordi** → thread → foldby. Regola nuova che semplifica e
insieme corregge: **una riga del CSV = un cluster nuovo** (via i "blocchi", via
`client.cancel`, e ogni punto parte da worker non frammentati).

**Collegamenti toccati**
`Giulia/bench_word_count.py` riscritto (183 → **107 righe di codice**): la campagna è una
tabella di cinque righe, ogni punto è "il riferimento con una manopola cambiata"; un
`performance_report` per **ogni** misura, che è il motivo per cui il picco di memoria non
sta nel CSV ← `cluster.py` (+`n_threads`, simmetrico a `n_workers`; **e finalmente
tracciato da git: non lo era**) → CSV in `~/mapd-out/bench/` → `word_count.ipynb` §9,
riscritto e portato a cinque sezioni. `SETUP` §6 (tabella dei flavor) e §5 (via la regola
"bench in revisione") allineati.

**Trovato riallineando il notebook:** il punto di riferimento è etichettato
`curva="riferimento"`, quindi filtrare su `curva=="partizioni"` faceva **sparire da
entrambe le curve obbligatorie il loro punto migliore e le uniche barre d'errore**. I
grafici ora si costruiscono come "le righe della curva più il riferimento", con l'asse x
letto dalle colonne di stato.

**Thread aperti**
Calibrare sul cluster (`--only riferimento`) prima della notte · la scrittura del
vocabolario finisce sui dischi dei **worker**: la cartella sulla macchina scheduler
resterà vuota, e `SETUP` §3 dice ancora di scaricare i risultati da una macchina sola ·
disco delle VM locale o di rete? · se l'ipotesi sul GIL regge, il seguito naturale è un
worker per core (IP ripetuto in `cluster.txt`).

**Primo tentativo sul cluster: 3 misure su 3 fallite, e il cluster era perfetto**
(4 worker, 16 thread, 7,1 GB ciascuno — la configurazione voluta). Due trappole che
`LocalCluster` **non può** mostrare, entrambe riprodotte e verificate in locale:

1. **I dati sono replicati su ogni macchina, il codice no.** È l'asimmetria che
   l'architettura non copriva. Nel grafo le funzioni importate viaggiano *per nome*,
   quindi scheduler e worker devono poter importare `word_count`; via SSH nascono dalla
   home, senza il repo nel `sys.path`. Il messaggio (*«Scheduler and Client have different
   environments»*) manda a cercare versioni diverse: sotto c'era un `ModuleNotFoundError`,
   **visibile solo nel log dello scheduler**. **Spiega anche perché non era mai emerso**:
   `word_count.py` lanciato a mano ha le sue funzioni in `__main__`, che Dask spedisce per
   valore; il benchmark le importa, quindi no. → `client.upload_file`, e da `word_count.py`
   è uscito l'import di `cluster` (un modulo caricato così dev'essere autosufficiente).
2. **Le cartelle di output non esistono sui worker.** `to_parquet` fa `mkdirs` sul client,
   ma scrivono i worker sui propri dischi, e fsspec apre con `auto_mkdir=False`: sarebbe
   stato il `FileNotFoundError` successivo. → `client.run(os.makedirs, ...)`

Le due regole stanno in `PROJECT_CONTEXT.md` §8.12 perché **valgono per tutti e quattro i
task**.

**Vicolo cieco, annotato perché sembra la risposta giusta:**
`cloudpickle.register_pickle_by_value(modulo)` non risolve — Dask consulta quel registro
solo quando serializza *una funzione*, non quando serializza il grafo in blocco. Provato
sul cluster, fallito allo stesso modo.

**Lezione di metodo, che vale più delle due cure.** La prova generale sul campione valida
la logica ma **non può** validare la distribuzione: in locale client, scheduler e worker
condividono `sys.path` e file system. Non serve però il cluster per provarla — basta far
partire scheduler e worker da una cartella fuori dal repo e collegarsi con
`DASK_SCHEDULER` (ricetta in `PROJECT_CONTEXT.md` §8.12c). Dieci secondi, e riproduce
entrambe le trappole. Trovata dopo aver bruciato due tentativi sul cluster vero.

---
## 2026-08-14 (notte) — la campagna è girata, e il risultato non è quello previsto

**I numeri.** Riferimento `k=256`, cluster pieno: **497,6 s con dispersione 1,4%** su tre
ripetizioni — il metodo «rumore misurato una volta, poi una ripetizione» regge, e quella
barra è la scala di tutto il resto. Campagna completa in 128 minuti, 10 misure su 15.

- **worker** (obbligatoria, completa): speedup 2,74× su 4, efficienza 0,81 → 0,69;
- **partizioni** (obbligatoria, 5 punti su 10): minimo a `k=512`, +9,6% a `k=1979`,
  **e sotto `k=128` non completa**;
- **thread**: `T(4 thread)/T(1 thread) = 1,31`. Avevo previsto 0,60.

**Il risultato che ribalta un'assunzione.** I thread non danno poco: **fanno male**.
`4 worker × 1 thread` = 379,8 s usando 4 core su 16, contro 497,6 s usando tutti e 16.
E `1 worker × 4 thread` (1.365,7 s) contro `4 worker × 1 thread` (379,8 s) sono **3,6×** a
parità di thread nominali. Su questo carico l'unità di calcolo utile è **il processo**.

**Il perché, misurato e non ipotizzato** → `Giulia/misura_ram.py` (nuovo: esegue una sola
partizione fuori dal cluster e misura la RSS passo per passo). Il picco di un task è
**~15× il testo** che elabora, e la fase Map da sola vale il 44% con **~230 byte per
coppia `((cord_uid, parola), conteggio)`** — costo dell'oggetto Python, stabile a ogni
scala, campione compreso. Quindi **un thread è un moltiplicatore di memoria**: a `k=64`
servono 4,07 GB per task, ×4 thread = 16,3 GB contro un tetto di 7,1.

**Regola nuova: il ramo sinistro della curva sulle partizioni è un confine, non un buco.**
Sotto `k=128` il job non è eseguibile su questo hardware, e nemmeno un flavor da 16 GB
sposterebbe il limite di più di un punto. Si riporta come risultato misurato.

**Errore mio da non ripetere:** avevo previsto che `k=4` sarebbe passato, calcolando il
**testo in ingresso** per partizione invece dell'**uscita del Map**, che è ciò che riempie
il worker. E la prima analisi del fallimento l'ho data come fatto quando era
un'interpolazione fra due punti: Federico ha chiesto la misura, ed è stata la cosa giusta.

**Collegamenti toccati**
`Giulia/misura_ram.py` (nuovo) · `bench_word_count.py` (+`--thread N`, per rifare una
curva con meno thread e spostare il muro) · `word_count.ipynb` §9.1/§9.2/§9.3 riscritte:
le curve ora si costruiscono filtrando sullo **stato reale** (`worker`, `thread`,
`partizioni`) e non sull'etichetta `curva`, altrimenti due campagne a thread diversi
verrebbero **mediate insieme** · `Giulia/README.md` con i risultati.

**Thread aperti**
Rifare la curva sulle partizioni con `--thread 1` (~40 min: recupera `k=64` e la misura
nella configurazione migliore) · quanto del rallentamento dei thread è GIL e quanto è
pressione di memoria non è separato dai dati attuali · il seguito naturale è **un worker
per core** (IP ripetuto in `cluster.txt`), mai approvato · scaricare `~/mapd-out` dalla VM.

---
## 2026-08-14

**Decisioni + perché**
Campagna girata sul cluster: **i thread RALLENTANO** (`T(4)/T(1) = 1,31`, previsto 0,60) →
su questo carico l'unità di calcolo utile è **il processo**, non il thread; il perché è
misurato e non ipotizzato (`misura_ram.py`: ~230 B per coppia Map, picco = ~15× il testo,
quindi un thread è un **moltiplicatore di memoria**). Sotto `k=128` il job non è eseguibile
su 8 GB: **confine misurato, non buco** — *sostituita in Parte A* la riga che dava gli 8 GB
per sufficienti al punto estremo. Il codice va **spedito ai nodi** (`client.upload_file`):
i dati sono replicati su ogni macchina, il codice no, ed è ciò che ha bruciato due run.

**Collegamenti toccati**
`misura_ram.py` (nuovo, un processo per `k`) → `memoria.csv` · `bench_word_count.py`
(+`--thread N`) → `misure.csv` → `word_count.ipynb` §9, che ora costruisce le curve dallo
**stato reale** (`worker`/`thread`/`partizioni`) e non dall'etichetta `curva`, altrimenti
due campagne a thread diversi verrebbero mediate insieme · `cluster.py` finalmente
tracciato da git · `PROJECT_CONTEXT.md` §8.12 (trappole `SSHCluster` + banco di prova
locale con `DASK_SCHEDULER`) · `Giulia/README.md` coi risultati.

**Thread aperti**
Curva partizioni con `--thread 1` (previsione scritta: `k=64` passa, `k=32` no) · `SETUP`
§6 contiene ancora l'affermazione falsificata sugli 8 GB · GIL vs pressione di memoria non
separati dai dati attuali · un worker per core (IP ripetuti in `cluster.txt`) mai approvato
· scaricare `~/mapd-out` dalla VM · errore mio da non ripetere: previsioni date per fatti.

---
## 2026-08-16

**Decisioni + perché**
Campagna definitiva a **3 passate intere** (48 misure, 4 worker × 1 thread): il minimo sulle
partizioni è un **plateau** `k=128`–`256`, non un punto — 0,36% di distanza contro 1,3–2,7%
di rumore, e chiamarlo «minimo a 256» sarebbe leggere rumore.
`bench-8x1` e `bench-16x1` chiudono l'open thread **GIL o memoria**: a parità di 8 core e di
memoria per slot, 8 processi fanno 204,7 s contro i 416,2 di 4×2 thread e **nessuna delle due
è vicina al tetto** → è il GIL; 16 core rendono **11,59×** come processi (eff. 0,72) contro
**2,44×** come thread (0,15), cioè **4,7×** sullo stesso hardware.
Sostituite in Parte A tre regole: il muro (è la memoria per slot, soglia netta a
`0,95 × memory_limit`), le ripetizioni (passate intere), l'ordine (partizioni prima dei worker).

**Collegamenti toccati**
`bench_word_count.py` (+`picco_gb`/`picco_medio_gb` da `ru_maxrss`, +`--ripetizioni`,
`stato_cluster` ora su `client.nthreads()`) · `cluster.py:describe()` stessa cura ·
`word_count.py` (+`client.run(os.makedirs)`: senza, sul cluster non girava — e infatti non ci
era mai girato, il 2.3.1 non esisteva) → `risultati/` git-ignored (5 campagne, 6 log,
vocabolario 2.3.1 da 16 shard, 0 duplicati) · `SETUP_CLOUDVENETO.md` §3 e §6 corretti ·
banco di prova cieco con `--nworkers 6`, che ha preso due difetti prima del cluster.

**Thread aperti**
Vocabolario: 6.098.548 parole misurate contro 6.037.808 documentate in `word_count.py:245`,
sanificazione invariata dal commit precedente — un run locale da 15 min chiude · `README` e
`word_count.ipynb` §9 hanno ancora i numeri della prima campagna e le due tesi corrette
(tetto netto, thread ×1,57 e non ×4) · `bench-8x1`/`16x1` sono una passata sola e la loro
colonna `worker` dice 5 · perché `scheduler_info()` sotto-conti resta ignoto, curato non
capito.

---
## 2026-09-10

**Decisioni + perché**
2.3.2 riscritto distribuito in `Federico/` (DataFrame, come suggerisce il testo): legge
`silver/authors` e **rifà da sé il rollup per paper** — quel raggruppamento è una decisione
di analisi, e i rollup `paper_countries`/`paper_institutions` diventano il **controllo**
(284.042 / 517.911 coppie, identiche, gratis a ogni run perché sono la somma della colonna).
`value_counts` in **una** partizione: 206 paesi e 10⁵ istituti non sono i 6M di parole del
2.3.1, che lì avevano imposto `split_out=16`. **Misurato: questo task sta sotto la soglia in
cui distribuire paga** — 34 MB, e la curva sulle partizioni *cresce* (k=4: 0,44 s → k=192:
12,35 s, ×28), 4 worker rendono 2,00× ma sull'overhead, e il punto migliore di Dask è
comunque **1,7× più lento di pandas su un core** (0,27 s). Si consegna come risultato.

**Collegamenti toccati**
`Federico/affiliations.py` (nuovo) ← `cluster.py` → `Federico/bench_affiliations.py`
(nuovo) → `misure.csv` · `Federico/README.md` coi numeri · `DATA_DICTIONARY.md` (riga
2.3.2 e le due sezioni rollup: ora dicono che il task le ricalcola e ci si verifica) ·
`CLAUDE.md` mappa (+`Federico/`, `Giulia/old/` non è più l'unica versione del 2.3.2) ·
**qualificata in Parte A** la riga «una riga del CSV = un cluster nuovo»: vale dove il
logoramento glibc esiste, non su un job da secondi · banco di prova §8.12c passato
(scheduler+worker fuori dal repo: `upload_file` regge, nessun `ModuleNotFoundError`).

**Thread aperti**
Il default di `--partitions` va scelto **sul cluster vero**, non sul Mac (là il minimo è
k=4, ma con 4 macchine meno partizioni che worker lascia qualcuno fermo) · campagna mai
girata su Cloud Veneto · nessun notebook per il 2.3.2 · il fondo classifica degli istituti
resta rumore di normalizzazione (68,4% singleton), documentato e non curato.

---
## 2026-09-10 (sera) — la chiave di raggruppamento delle affiliazioni

**Decisioni + perché**
Il 2.3.2 raggruppava sulla colonna del `silver`, che è normalizzata **leggera per scelta
dichiarata** (`norm_institution`: NFKC, spazi, punteggiatura ai bordi) e lascia la
disambiguazione ai task. Aggiunta `chiave()` — minuscole, via i caratteri non alfanumerici
ai bordi (`‡ † △ ✉`), via l'articolo iniziale, accenti piegati — **perché la misura dice che
cambia la risposta**: `The University of Hong Kong` era spezzata in **sei grafie** e passa
dal 17° al 14° posto (1.188 paper); istituti distinti 105.967 → **100.838** (−4,8%), coppie
517.911 → **517.058**. È il caso opposto a `is_reference_like` nel 2.3.1, tolto perché non
muoveva la top-20. **L'etichetta consegnata è la grafia più frequente, non la chiave**, e non
costa uno shuffle: `per_autore` si conta già sulla grafia e le due Serie si ricuciono sul
client. Sui **paesi la chiave è un no-op misurato** (206 → 206: `country_converter` li ha già
canonicalizzati) e si applica lo stesso, un ramo solo di codice.
**E il benchmark si è ribaltato:** con il lavoro per riga che la correttezza richiedeva, il
punto migliore passa da **1,7× più lento** di un core a **2,2× più veloce** (1,81 s a `k=8`
contro 3,93 s di pandas), e la curva sulle partizioni acquista un **minimo interno** invece
di crescere sempre. Quel che decide se distribuire paga non è la taglia del cluster ma
**quanto lavoro c'è per riga**: qui lo abbiamo visto cambiare in diretta.

**Collegamenti toccati**
`Federico/affiliations.py` (+`chiave`, `ranking` raggruppa sulla chiave e conta la grafia,
`classifica` ricuce sul client) → `Federico/bench_affiliations.py` (`baseline_pandas` fa
**la stessa** cosa, chiave compresa: se saltasse un pezzo il rapporto Dask/pandas
confronterebbe due lavori diversi) → campagna locale rifatta da zero, 3 passate ·
`Federico/README.md` riscritto (sezione nuova sulla normalizzazione con la tabella regola
per regola; benchmark aggiornati) · `DATA_DICTIONARY.md`: la nota su `paper_institutions`
ora distingue le 517.911 coppie **grezze** dalle 517.058 **con la chiave**.

**Thread aperti**
Campagna sul cluster mai girata (un run singolo là: 39,6 s contro 13,4 s del Mac) · il
default di `--partitions` va scelto su quei numeri: sul Mac il minimo è `k=8`, il default
«una partizione per file» è il punto **peggiore** della curva · la curva sui worker è
misurata al default `k=192`, quindi il 2,09× è una stima per difetto e va rifatta al `k`
scelto · sigle (`CAS`/`NIH`) e frammenti di indirizzo restano fuori: serve un dizionario.

---
## 2026-09-10 (tarda sera) — la 8786 non si libera da sola

**Decisioni + perché**
Primo tentativo di campagna 2.3.2 sul cluster: **morta al primo punto**, `OSError: [Errno 98]
Address already in use` sulla 8786 subito dopo un run di `affiliations.py`. Chiude il thread
aperto il 2026-08-14 («verificare che la porta 8786 si liberi fra un cluster e il
successivo»): **non si libera da sola**, e la pausa di `bench_word_count.py` non era
pignoleria. `bench_affiliations.py` non ce l'aveva e accendeva **12 cluster di fila senza
respiro**: aggiunta `PAUSA_FRA_CLUSTER = 10`, stessa costante e stessa ragione. Secondo
difetto della stessa famiglia: `get_client` stava **fuori** dal `try/except` che protegge la
misura, quindi un cluster che non nasce si portava via l'intera campagna invece di lasciare
righe con l'errore — ora le sue misure diventano righe `errore` e la campagna prosegue.

**Collegamenti toccati**
`Federico/bench_affiliations.py` (+`PAUSA_FRA_CLUSTER`, `except` sul ciclo dei worker) ·
`Federico/README.md` §Benchmark (la pausa e il perché) · misurato di passaggio: il baseline
pandas su un core della VM è **14,18 s** contro i 3,93 s del Mac — i core delle *medium*
sono molto più lenti, da tenere presente leggendo i tempi del cluster.

**Thread aperti**
Campagna 2.3.2 sul cluster ancora da completare · resta da scegliere il `k` di default sui
numeri del cluster · la curva sui worker è misurata al default `k=192`, il punto peggiore.

---
## 2026-09-11 — i benchmark del 2.3.2, e cosa hanno deciso

**Decisioni + perché**
Campagna chiusa: **165 misure, zero errori**, 6 passate sulle due curve obbligatorie e 3 sul
confronto processi/thread. Tre decisioni, tutte con la misura sotto.
**(1) `DEFAULT_PARTITIONS = 8`** nel task, al posto di «una per file»: quello era il punto
**peggiore** della curva (6,5× il minimo), mentre 8 vince o pareggia in tutte e tre le
configurazioni provate — e a 8 processi `k=16` costa il 24% in più (3,91 contro 4,87 s,
sei deviazioni standard). È una **costante misurata, non una formula**: «una partizione per
processo» spiega il minimo a 8 processi ma a 4 darebbe `k=4`, che è peggio di `k=8`.
**(2) Su questo carico l'unità di calcolo utile è il PROCESSO.** A parità di core i processi
vincono sempre (`8×1` batte `4×2` di 1,33×, `4×1` batte `2×2` di 1,70×), e il secondo thread
per worker rende **1,01×**, cioè niente: la frazione che due thread si spartiscono davvero è
~1,5%, il resto è GIL. Confermato su **tutti e nove** i `k`, dove le curve `4×1` e `4×2` si
sovrappongono entro il ±6% **con metà dei core**. Ipotesi scritta prima di misurare
(«processi vincono, meno nettamente del 2.3.1»): rispettata, 1,33× contro 2,03×. Differenza
col 2.3.1: là i thread **rallentavano**, qui sono **inerti**.
**(3) La curva sui worker si misura al punto di lavoro, non al default**: a `k=16` dà 2,90×
su 4 worker, a `k=192` dava 2,48×. Dove il job è quasi solo coordinamento, la misura
sottostima. *Sostituisce la riga di ieri che riportava 2,51× come speedup del task.*
Sommando i due pomelli: **3,91 s** (`8×1`, `k=8`) contro i **41,09 s** della configurazione
di partenza — **10,5×** a parità di hardware e di codice.

**Collegamenti toccati**
`Federico/affiliations.py` (+`DEFAULT_PARTITIONS`) ← `Federico/bench_affiliations.py`, dove
`--k` ora eredita quel default, la campagna è diventata una **tabella di forme di cluster**
`{(worker, thread): [(curva, k)]}` e ci sono `--only`, `--worker`, `--thread`, `--k` ·
`CORD19_HOSTS` raddoppiata + `CORD19_WORKER_MEMORY_LIMIT=1.7GB` per gli 8 worker su 4
macchine, **senza toccare `cluster.txt`** (stessa ricetta di `bench-16x1` del 2.3.1) ·
`Federico/README.md` §Benchmark riscritta coi numeri del cluster ·
`risultati/affiliazioni/` (git-ignored): 165 misure + log + output del task.

**Thread aperti**
`cluster.txt` a quattro voci significa `4×2 thread` di default, cioè **metà cluster fermo**:
automatizzare il raddoppio in `cluster.py` tocca l'unico file condiviso fra i quattro task e
va discusso col gruppo · perché a 8 processi il minimo sia un punto e a 4 un plateau resta
**annotato e non capito** · il 2.3.2 non ha notebook.

---
## 2026-09-11 (sera) — il 2.3.4, e il primo task che non è GIL-bound

**Decisioni + perché**
Nasce `Niccolo/` (2.3.4, similarità coseno): `cosine.py` + `bench_cosine.py`, stessa interfaccia
di `Federico/`. **Normalizzare una volta** rende il coseno un puro prodotto scalare, quindi
«tutte le coppie» è `X @ Xᵀ` — BLAS, 180-620 GFLOPS sul Mac contro i ~10⁶ op/s di un ciclo
Python — e si taglia in **piastrelle** `(i,j)` con `i ≤ j`. **`delayed` e non DataFrame/Bag/
array**: il DataFrame farebbe un cross join da 470 mld di righe, il Bag userebbe come elementi
coppie di indici e aggiungerebbe una manopola che nei benchmark si confonde con `k`, `dask.array`
calcolerebbe tutte e `k²` le piastrelle perché non sa che `S` è simmetrica. **Il risultato è più
grande dell'input** (40 GB a 100.000 titoli, 3,7 TB sul corpus), quindi ogni piastrella **riduce
sul posto** (top-20, bottom-20, istogramma) — Map/Reduce **esatto**, non approssimato. Primo task
**compute-bound** del progetto: i dati si **replicano** (`scatter`, 120 MB) e si distribuisce il
calcolo, quindi **gli embedding servono solo sulla macchina da cui si lancia**, non su ogni VM.
**`--titoli 100000` (≈ N/10)** come default e **fisso** per tutta la campagna: il costo va come
`N²`, il corpus intero è ~45 min su un core e non sta in una campagna; sotto, si misurerebbe solo
coordinamento (l'errore pagato dal 2.3.2). Campione a **quota per file con seed**, non i primi N
(starebbero tutti in `part.0`).
**IPOTESI SCRITTA PRIMA DI MISURARE, e la prima misura la conferma:** qui il lavoro è in BLAS, che
**rilascia il GIL**, quindi i thread dovrebbero funzionare al contrario del 2.3.1/2.3.2. In locale
il **secondo thread rende 1,78×** dove nel 2.3.2 rendeva 1,01×. Perciò la **curva sui thread è di
prima classe**, non un contorno. Condizione perché quella misura significhi qualcosa: **BLAS a un
thread** (`single_thread_blas()` via `pre-spawn-environ`, stesso aggancio di `MALLOC_TRIM_THRESHOLD_`,
**senza toccare `cluster.py`**) — altrimenti 4 worker × 4 thread BLAS su 4 core misurano il proprio
thrashing. Verificato interrogando i worker.
**Il muro di memoria è su `k`, non sui dati:** una piastrella costa **~12-14 (N/k)² byte × thread
del worker**. Modello verificato (previsti 3,6 GB a 40.000 titoli e `k=4`, osservati 3,81 GiB dal
warning della nanny): a 100.000 titoli `k=4` chiederebbe 7,5 GB **per task**. I `k` bassi non sono
lenti, sono **irrealizzabili** — stesso fenomeno del `k=32` nel 2.3.1 — e lo sweep parte da 4 perché
il muro **si misura**, e un `k` che sfonda lascia una riga `errore` invece di uccidere la campagna.
**Correttezza verificata una volta, non a ogni run** (regola dell'invariante Map/Reduce del 2.3.1):
a `k = 3, 8, 16` su 2.000 titoli le top-20 e bottom-20 sono **identiche** al calcolo diretto. A ogni
run resta l'invariante a costo zero, coppie contate contro attese — che ha trovato un difetto vero:
in **float32 due vettori identici danno 1,0000001**, cadono fuori dal range dell'istogramma e
spariscono (4 coppie su 12.497.500). Curato con un `clip` in-place, che è anche la definizione.
**Cosa NON si consegna**, per scelta: la matrice, il vicino più simile di *ogni* paper, il
clustering, la curva tempo-vs-N.

**Collegamenti toccati**
`Niccolo/cosine.py` (nuovo) ← `Niccolo/bench_cosine.py` (nuovo, ricalca `bench_affiliations.py`:
CSV in append, `client.nthreads()` per lo stato, forme `{(worker, thread): [(curva, k)]}`,
`PAUSA_FRA_CLUSTER = 10`, `get_client` dentro il `try`) → `cluster.py` usato **senza modifiche**
(`single_thread_blas` sfrutta l'`update` che `configure_memory` fa già) · legge `Niccolo/embeddings/`
(output 2.3.3 di Daniele) e `data/silver/papers` per i titoli · `Niccolo/README.md` (nuovo, coi
numeri) · `CLAUDE.md` §4 (riga nella mappa) · `.gitignore` (la riga per gli embedding era un
**percorso assoluto**, che git ignora: 1,1 GB non erano davvero esclusi — corretta).

**Thread aperti**
Campagna sul cluster mai lanciata (numeri di oggi = Mac: minimo `k=16`, worker 2,43×, thread 1,78×,
7,0× su NumPy un core) · `--titoli` va ricalibrato sui tempi delle VM · **decisione rimandata**: se
filtrare i titoli duplicati (28,0 % del corpus) e quelli con `n_words ≤ 1-3` (0,58 % / 4,96 %) —
tre titoli tedeschi di cui il modello conosce solo `"der"` risultano **identici al 100 %** · il
2.3.4 non ha notebook · su `SSHCluster` resta da verificare che `pre-spawn-environ` porti davvero
le variabili BLAS sui nodi.

---
## 2026-09-12 — la campagna del 2.3.4: i thread funzionano, e le macchine contano più dei core

**Decisioni + perché**
Campagna chiusa sul cluster (5 × `large`, 100.000 titoli, **6 passate + calibrazione + un
comando a core costanti, 85 misure**). Tre decisioni e due difetti trovati misurando.
**(1) `DEFAULT_BLOCKS = 32`**, e *non* perché sia più veloce: su `k=16` guadagna il 3,4% a
**2,1 σ**, che da solo non deciderebbe niente. Decide il **picco per task, 4× più basso**
(0,12 GB contro 0,47), perché a 100.000 titoli `k=8` e `k=4` sfondano — `KilledWorker` su tutti
e quattro i worker, e la riga di `k=4` registra `worker=3` perché il cluster non si era ancora
ripreso. Il default costa uguale e sta **due passi** dal muro invece di uno. Curva: `k=16`
21,70 · **`k=32` 20,98** · `k=64` 23,38 · `k=128` 38,16 (1,82× il minimo: 8.256 task in cui il
calcolo non copre lo scheduling). Le due salite hanno **due cause diverse** — memoria a
sinistra, scheduling a destra — non una sola forma a U.
**(2) L'IPOTESI SUI THREAD È CONFERMATA, e cambia il quadro del progetto.** Il secondo thread
per worker rende **1,66×** (29,67 contro 49,36 s) dove nel 2.3.2 rendeva 1,01× e nel 2.3.1
rallentava: BLAS rilascia il GIL, e si vede. Di conseguenza il vantaggio residuo dei processi
a core costanti si riduce a **1,15×** — ma solo il confronto **8×1 contro 4×2 sulle stesse
quattro macchine** (25,85 ± 0,16 contro 29,67 ± 0,34, 23 σ) lo misura davvero: i due confronti
spontanei dalle curve (`4×1` vs `1×4` = 1,56×, `4×2` vs `2×4` = 1,38×) **sono sporchi**, perché
su questo cluster ogni worker è una macchina diversa e quei numeri confrontano quattro bus di
memoria contro uno. Sequenza dei tre task: **2,03× → 1,33× → 1,15×**.
**(3) Quello che scala è la MACCHINA, non il core.** Otto core su quattro macchine (25,85)
battono **dodici core su tre** (26,88). L'efficienza si divide in due gruppi netti: ~50% con
worker a 1 thread, ~31% con worker a 4. Il limite non è Dask né il GIL: è la **banda di
memoria** condivisa fra i thread del nodo — coerente con la saturazione della curva thread
(1,66× poi 1,40×) e col fatto che la *riduzione* della piastrella è memory-bound (sul Mac
costa 3,7× la moltiplicazione). Speedup finale **4,89×** su 16 core contro un core (102,55 s).
**PRIMO DIFETTO, trovato dalla campagna in sé stessa:** lo **stesso** punto (4 worker, 16
thread, `k=32`) dà **20,98 ± 0,44** come punto della curva partizioni e **17,41 ± 0,71** come
punto della curva worker — 17%, **10,5 σ**. Causa: Dask nomina i dati di `client.scatter` con
l'**hash del contenuto**, quindi alla seconda misura dello stesso `k` nello stesso cluster i
blocchi sono già sui worker e **il trasferimento non avviene**. Il conto torna (120 MB × 4
worker ≈ 480 MB ≈ 3,8 s su 1 Gb/s) ed è confermato due volte: il valore *freddo* coincide nelle
due curve che lo misurano su cluster appena nati (20,98 e 21,16), e `k=64` misurato *dopo*
`k=32` paga comunque il suo scatter perché i suoi blocchi sono altri. **Lo speedup sui worker è
quindi 3,66× e non 4,41×** (quest'ultimo confronta il valore caldo di 4 worker coi valori
freddi di 3, 2, 1). In cambio quei 3,57 s **sono il costo di distribuire i dati, isolato per
differenza**: il 17% del job a 4 worker.
**SECONDO DIFETTO, e riguarda il risultato, non la misura: la classifica in cima NON È UNICA.**
Ci sono **4.851 coppie sopra 0,98** e le venti consegnate valgono **tutte esattamente
1,000000**: quali venti escano dipende dall'ordine in cui finiscono i task, e cluster e Mac
infatti consegnano **venti coppie diverse tutte a 1,000000**. Finché duplicati e `n_words=1`
restano dentro, «le più simili» è una domanda mal posta. Aggiunto: **due run non sono
bit-identici** — 30 bin su 100 dell'istogramma differiscono di ±1-2 coppie con differenza
totale **zero**, perché la somma float non è associativa e BLAS cambia l'ordine degli addendi
con blocking, SIMD e libreria. L'**invariante di conteggio coincide alla cifra** su entrambe le
macchine (4.999.950.000): riproducibile entro l'errore di macchina, non bit a bit.
**Misurato di passaggio:** il baseline su un core VM è **102,55 ± 0,67 s** contro i 69,6 s del
Mac, cioè **1,51×** e non il 3,6× del 2.3.2. Il rapporto fra due macchine non è un numero:
dipende da cosa stanno facendo — là stringhe e regex, qui BLAS.

**Collegamenti toccati**
`nicco_scripts/` (la cartella si chiama così da oggi: i file sono stati pushati da un compagno
e `Niccolo/` è rimasta come doppione git-ignored, **non si lancia**) · `cosine.py`
(`DEFAULT_BLOCKS` 16 → 32, `--papers` controllato **prima** del calcolo perché sul cluster i
dati non stanno nella repo) ← `bench_cosine.py` (`BLOCCHI` = 16, 32, 64, 128 **in
quest'ordine**, dal riferimento verso i bordi: i `k` fragili per ultimi, altrimenti un worker
ucciso contamina le misure successive dello stesso cluster — regola d'ordine già in vigore dal
2.3.1, non applicata alla prima stesura; `k=8` e `k=4` **fuori** dallo sweep, li ha misurati la
calibrazione e il muro è un fatto binario, non una misura con dispersione) ·
`nicco_scripts/README.md` §Benchmark riscritta coi numeri del cluster · `CLAUDE.md` §4 ·
`risultati/cosine/` (git-ignored): 85 misure, classifiche, istogramma.

**Thread aperti**
Il costo dello `scatter` a 8 worker non è misurato, quindi l'1,15× dei processi è una
**sottostima** di entità ignota: servirebbe lo stesso punto rimisurato a caldo su 8 worker ·
**decisione rimandata, ora urgente**: filtrare i titoli duplicati (28,0 %) e `n_words ≤ 1-3`
(0,58 % / 4,96 %) — senza, la classifica in cima è degenere e non riproducibile · perché il
punto a **un worker** abbia dispersione dell'8 % (70,9 → 87,2 s) contro lo 0,3-2 % di tutti gli
altri resta **annotato e non capito** · il 2.3.4 non ha notebook · i due `.log` della campagna
non sono stati scaricati dalla VM (il secondo `rsync` non è passato).

---
## 2026-09-13 — la notte del 2.3.4: una previsione falsificata e un risultato che cambia segno

**Decisioni + perché**
Campagna notturna chiusa: **274 misure in 307 minuti**, contro i 308 stimati. Rispondeva alle
tre domande lasciate aperte ieri; due hanno risposta netta, una ha **smentito noi**.
**(1) IL COSTO DELLO `scatter` NON ESISTE, e la nostra spiegazione di ieri era sbagliata.**
Avevamo attribuito i 3,57 s di differenza fra due misure dello stesso punto al trasferimento
dei blocchi (Dask nomina i dati scatterati con l'hash del contenuto → la seconda misura non
ritrasferisce), con un conto che tornava: ~480 MB su 1 Gb/s ≈ 3,8 s. Misurando lo **stesso
punto tre volte di fila nello stesso cluster** (`--ripeti-punto 3`), la prima misura **non è
più lenta**: freddo − caldo vale −1,95 · −0,05 · +0,36 · −0,05 · +0,11 s a 1, 2, 3, 4, 8
worker, **tutte entro 1 σ da zero**. La previsione «a 8 worker deve costare il doppio» è
smentita dal dato che doveva confermarla. **Distribuire 120 MB non costa niente di
misurabile**, e va tolto dalla lista delle voci di spesa del task.
L'unico effetto di quel tipo che esiste riguarda il **numero di task, non i dati**: a
`k=128` (8.256 task) la prima esecuzione costa il **10,7%** in più (39,05 → 34,88), a `k=16`
e `k=32` nulla. Ieri il punto "veloce" girava dopo `k=64` e `k=128`, cioè dopo ~10.000 task:
**ipotesi nuova, non verificata** — a scaldarsi sarebbero scheduler o allocatore, non i dati.
La misura che la decide è corta (nello stesso cluster `k=128` e subito dopo `k=32`: vale 21
o 17?). Invariato: **lo speedup sui worker resta 3,66×**, perché quei due numeri non sono
confrontabili qualunque sia la causa.
**(2) LA DISPERSIONE A UN WORKER È DEL CALCOLO, NON DELL'ACCENSIONE.** Otto misure a cluster
fermo × 5 cluster × 4 macchine: la σ **dentro** un cluster (3,19 s) è **maggiore** di quella
**fra** cluster (2,21 s) — se fosse l'avvio sarebbe il contrario — e non c'è tendenza dalla
1ª all'8ª misura (84,77 → 82,26 s, dentro il rumore). **Non è una macchina difettosa:** le
quattro stanno entro il **6,5%** (80,24 · 85,42 · 83,64 · 84,46 s) e disperdono tutte
(CV 3,0-5,8%). Resta che il punto a un worker è **intrinsecamente** più rumoroso degli altri
(CV 0,3-2%): è l'unica configurazione in cui un solo nodo porta tutto il carico, quindi il
rumore del suo sistema operativo non viene mediato su quattro macchine. Causa **ristretta**,
non più solo annotata.
**(3) I FILTRI: il risultato cambia di segno.** Cinque configurazioni a 100.000 titoli con
`--top 5000` per contare i pari merito. Coppie a 1,000000: **4.131** (nessun filtro) · 3.357
(`--min-parole 2`) · 2.932 (`--min-parole 3`) · 2.162 (`--solo-unici`) · **1.902** (entrambi).
I filtri **dimezzano la degenerazione ma non la eliminano**, e il perché è la scoperta della
notte: fra le 2.000 coppie che sopravvivono a entrambi i filtri, **ZERO hanno i due titoli
identici come stringa**. Differiscono per un punto finale, un trattino U+2010 contro ASCII,
un apostrofo curvo — cioè sono **lo stesso paper depositato due volte** da fonti diverse, che
`is_title_unique` non vede (`title_norm` non unifica quei caratteri) e che l'embedding non
**può** vedere (la punteggiatura non entra nella media dei vettori di parola). Quindi la cima
della classifica **non è rumore da ripulire**: è il corpus che contiene migliaia di paper
gemelli, e il coseno li trova — che è il lavoro per cui lo si usa. La risposta onesta a
«quali sono i titoli più simili» è *«questi ~1.900 sono lo stesso paper due volte»*, e solo
dopo ha senso chiedersi quali siano i più simili **fra paper diversi**. **I filtri restano
opzioni spente per default:** ora ci sono i numeri per decidere, e la decisione è di chi
consegna.

**Collegamenti toccati**
`nicco_scripts/notte_2_3_4.sh` (nuovo: l'elenco dei comandi della notte, nessuna logica;
ordine dal più importante al più lungo perché una notte può interrompersi) →
`bench_cosine.py` (+`--ripeti-punto N` e colonna **`misura`**: `misura=0` è la prima volta che
quel `k` gira in quel cluster — serviva a isolare lo scatter e a separare la varianza a
cluster fermo da quella fra accensioni; +`--no-baseline`, perché 102 s a passata sarebbero
stati la maggior parte di una campagna che ripete un punto solo) · `cosine.py`
(+`--min-parole`, +`--solo-unici`, +`eligible_uids` che converte il vocabolario **una volta**
in `pyarrow.Array` per `pc.is_in` — l'hotspot di `MEMORY_LEAK_REPORT.md`) ·
`nicco_scripts/README.md` (la sezione sullo scatter **corretta**, non cancellata: la
spiegazione sbagliata resta scritta accanto alla misura che la smentisce) ·
`risultati/cosine/notte/` (git-ignored).

**Thread aperti**
Perché la prima esecuzione a `k=128` costi il 10,7% in più: ipotesi scheduler/allocatore,
misura decisiva da 6 minuti mai fatta · il 2.3.4 non ha notebook (deciso: non si fa) · la
scelta se filtrare resta **aperta e ora informata**: senza filtri si consegna «il corpus ha
migliaia di gemelli», con i filtri «fra paper diversi i più simili sono questi» — sono due
risposte diverse alla stessa domanda, ed entrambe sono difendibili.
