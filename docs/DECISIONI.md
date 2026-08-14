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
  worker — e non ce ne sono altre. Tutto il resto è un run che si fa una volta e di cui si
  scrive il numero. *Rifatti da zero il 2026-08-13: `bench.py` (497 righe) è diventato
  `cluster.py`, che accende il cluster e basta; `Giulia/bench_word_count.py` (451 righe,
  dieci blocchi) è una campagna sola da ~130 righe.*
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
