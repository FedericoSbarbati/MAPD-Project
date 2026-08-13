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
  subito. La verifica dell'invariante è opt-in (`--check`) perché raddoppia il tempo.
  → `local/NOTES.md` v1
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
- **`is_reference_like` si filtra per definizione, non per risultato:** sul corpus intero è
  l'1,90% dei paragrafi e lo 0,74% del testo, e non muove nessuna parola nella top-30 — ma
  una dichiarazione di conflitto d'interessi non è corpo del paper. → `local/NOTES.md` v3
- **Direzione scelta e non ancora realizzata:** al testo non inglese si danno le sue
  stop-word (tedesco, francese, spagnolo, portoghese), invece di scartare quei paper.
  → `local/NOTES.md`, "Da vedere in futuro"

## Benchmark

- **I benchmark si fanno sul corpus vero, non sul campione.** Sul campione una macchina
  batte quattro, perché a quella scala il tempo è tutto overhead di coordinamento.
  → `local/NOTES.md` v5
- **Il sweep sulle partizioni tiene i dati fissi:** `repartition(npartitions=k)` su una
  fetta fissa. Fette di corpus crescenti misurano la quantità di dati, non il
  partizionamento — è l'errore del vecchio benchmark. E `partition_size=…` si impianta
  sotto un cluster distribuito.
- **`bench.py` e `Giulia/bench_word_count.py` sono in revisione: non si estendono e non si
  usano come base.** Sono cresciuti oltre lo scopo del corso fino a diventare
  ingiustificabili all'orale. Vanno rifatti da zero, decidendo prima la forma della misura.

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
