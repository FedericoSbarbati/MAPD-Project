# Cloud Veneto — come si accende e si usa il cluster

Guida **operativa**: i passi da fare, nell'ordine in cui si fanno. Il *perché*
l'architettura è questa sta in [`PROJECT_CONTEXT.md`](PROJECT_CONTEXT.md) §5; le regole
già decise stanno in [`DECISIONI.md`](DECISIONI.md).

**Fonte autorevole** per tutto ciò che riguarda la piattaforma (dashboard, flavor, gate,
volumi, snapshot): <https://userguide.cloudveneto.it/en/latest/index.html>. In caso di
dubbio si controlla lì, non si tira a indovinare.

> **Stato: 2026-08-13.** Architettura attuale: **un cluster per persona, dati replicati
> su ogni macchina**. La versione precedente (un solo cluster condiviso, volume da 200 GB
> montato su `node0` ed esportato via NFS) è stata abbandonata: obbligava tutti e quattro
> a lavorare sulla stessa macchina e in quattro non era praticabile. Se trovi un documento
> o uno script che parla di NFS o di montare `/data`, è materiale vecchio.

---

## 0 · Il cluster in mezza pagina

```
   il tuo portatile
        │  ssh -J <utente_cv>@gate.cloudveneto.it
        ▼
   macchina 1  ──►  SOLO scheduler + il tuo terminale         rete interna 10.67.22.x
   │                (nessun worker gira qui)
   ├── macchina 2   worker
   ├── macchina 3   worker
   ├── macchina 4   worker
   └── macchina 5   worker
```

| | com'è |
|---|---|
| Quante macchine | **3 minimo**, di solito **4–5**. Si decide sessione per sessione |
| Taglia | tutte **uguali** nello stesso cluster: *medium* = 4 GB di RAM, *large* = 8 GB; ~25 GB di disco a testa |
| Chi fa cosa | la **prima** macchina fa solo da scheduler; tutte le altre sono worker |
| I dati | una copia **su ogni macchina**, in `~/mapd-data/silver/` — ci arrivano dall'immagine snapshot, non si copiano a mano |
| Il codice | clonato da GitHub a ogni sessione, in `~/MAPD-Project` |
| Chi accende i worker | il codice stesso, via SSH, leggendo `cluster.txt` |

**Cosa NON c'è (più):** niente NFS e niente volume condiviso · niente Docker (il cluster
è vero, non simulato) · niente VS Code remoto e niente Jupyter sulle VM: **si lavora da
terminale e si lanciano file `.py`**; i notebook servono a spiegare, non a eseguire.

Il volume di rete da 200 GB **esiste ancora** e contiene i dati grezzi CORD-19 e i Parquet
completi (bronze + silver), ma **non lo usa più nessuno**: serviva alla conversione
JSON→Parquet, che è finita e che non fa nemmeno parte dell'assignment. Lo si tiene come
archivio, staccato.

---

## 1 · Le cartelle di una macchina

Regola che regge tutto il resto: **quello che è prezioso vive nella home, mai dentro la
repo.** La repo è usa-e-getta — la cancelli e la ricloni quando vuoi senza perdere niente.

| Percorso | Cos'è | Sopravvive a `rm -rf` della repo? |
|---|---|---|
| `~/mapd-data/silver/` | **i dati** — `papers`, `paragraphs`, `authors`, `paper_countries`, `paper_institutions`. Arrivano dallo snapshot | sì |
| `~/mapd-out/` | **i risultati** dei run: CSV, grafici, Parquet prodotti | sì |
| `~/MAPD-Project/` | il codice, clonato da GitHub. Contiene `cluster.txt` | no, ed è voluto |
| `~/pyvenv/` | l'ambiente Python con Dask | sì |
| `~/dask-scratch/` | file temporanei di Dask | sì |

> ⚠️ **`~/mapd-out` è una decisione presa ma non ancora applicata nel codice.** Oggi il
> word count scrive di default in `reports/word_count` *dentro la repo*: con il flusso
> "cancello e riclono" questo significa perdere i risultati di un run senza accorgersene.
> Finché il default non è cambiato, **passa il percorso a mano**:
> `--out ~/mapd-out/word_count`. Il cambio si fa quando si rifà l'impalcatura dei
> benchmark (vedi §5).

---

## 2 · Accendere una sessione

### 2a. Creare le macchine (dashboard di Cloud Veneto)

1. Crea N istanze (3 minimo, 4–5 tipico) **dalla nostra immagine snapshot**, quella che
   ha già i dati dentro — è riconoscibile dal nome ed è visibile a tutto il gruppo.
   *Source: Image*, `Create New Volume: No`.
2. **Flavor uguale per tutte** le macchine dello stesso cluster. Mescolare taglie diverse
   è la cosa da non fare: Dask distribuisce il lavoro dando per scontato che i worker
   siano intercambiabili, quindi il più piccolo diventa il freno di tutti e i tempi dei
   benchmark diventano inspiegabili.
3. **Security group: `pod-students`** — quello del corso, che permette già il traffico
   tra le macchine. Si seleziona e basta: **non si modifica mai**, è condiviso con tutti
   gli studenti.
4. **Key Pair:** seleziona la tua.
5. Annota gli **IP interni** (`10.67.22.x`) di tutte le macchine.

### 2b. Collegarsi alla prima macchina

Le macchine stanno su una rete interna: ci si arriva **passando dal gate**, che è il
computer d'ingresso del data center (`-J` vuol dire esattamente "passa da lì").

```bash
ssh -J UTENTE_CV@gate.cloudveneto.it ubuntu@10.67.22.XYZ -i ~/.ssh/id_ed25519
```

`UTENTE_CV` è il tuo account Cloud Veneto personale (§4). Da qui in avanti si lavora solo
su questa macchina: **non serve collegarsi alle altre**, ci pensa il codice.

### 2c. Codice e lista dei nodi

```bash
git clone https://github.com/FedericoSbarbati/MAPD-Project.git
cd MAPD-Project
```

Poi crea `cluster.txt` nella radice della repo: **una riga sola**, gli IP separati da
virgola, **il primo è lo scheduler**.

```bash
echo '10.67.22.101,10.67.22.102,10.67.22.103,10.67.22.104,10.67.22.105' > cluster.txt
```

Il file è escluso da git apposta: gli IP cambiano a ogni cluster e non devono mai finire
dentro il codice. Righe che iniziano con `#` vengono ignorate, così puoi tenere annotata
una configurazione alternativa.

> **La prima voce fa SOLO da scheduler**, non lavora. Con 5 IP hai quindi 4 worker.
> Se volessi far lavorare anche la prima macchina basterebbe ripetere il suo IP come
> seconda voce — ma **non farlo**: il perché è in `PROJECT_CONTEXT.md` §5.

### 2d. Lanciare

```bash
source ~/pyvenv/bin/activate
python Giulia/word_count.py ~/mapd-data/silver/paragraphs --out ~/mapd-out/word_count
```

Il percorso dei dati si passa **assoluto**: i dati non stanno dentro la repo.

### 2e. Il controllo da fare SEMPRE, prima di aspettare venti minuti

All'avvio il codice stampa un blocco con la configurazione. Leggilo:

- deve dire **`SSHCluster`**, non `LocalCluster`;
- il numero di worker deve essere quello che ti aspetti (numero di IP meno uno);
- gli indirizzi dei worker devono essere `10.67.22.x`, **non `127.0.0.1`**.

Se vedi `127.0.0.1` sta girando tutto sulla macchina scheduler e le altre stanno a
guardare. È già successo: `cluster.txt` era finito nel posto sbagliato e il run sembrava
"un cluster lento" invece che una configurazione sbagliata — te ne accorgi a sessione
bruciata. Il file viene cercato, in quest'ordine, nella radice della repo, nella cartella
da cui lanci, e nella home.

---

## 3 · Chiudere una sessione

**Le macchine NON si cancellano più.** Regola cambiata rispetto al passato, e il motivo è
concreto: senza volume condiviso, i risultati stanno sui dischi delle macchine, quindi
distruggerle vuol dire buttare via il lavoro. Si lasciano accese.

**Scarica comunque i risultati sul portatile.** Sono piccoli (un CSV, un grafico, e il
Parquet del vocabolario: qualche decina di megabyte) e finché stanno su una sola macchina
sono a rischio:

```bash
scp -J UTENTE_CV@gate.cloudveneto.it -r ubuntu@10.67.22.XYZ:~/mapd-out ./
```

> **Tensione da tenere presente, e da decidere insieme nel gruppo.** La guida del corso
> raccomanda esplicitamente di essere parsimoniosi perché il pool di risorse è condiviso
> con tutti gli studenti: *«we are all sharing the same pool of resources, so be
> considerate»*. Tenere N macchine accese a tempo indeterminato non è vietato, ma è una
> scelta da fare consapevolmente. Se i risultati sono già scaricati, il motivo per
> tenerle accese viene meno.

---

## 4 · Dare accesso a un compagno (chiavi SSH)

Serve meno di prima — adesso ognuno ha il suo cluster — ma resta utile per farsi aiutare
a guardare un problema. **Il concetto in una frase: non si condivide mai una chiave
privata.**

Una coppia di chiavi SSH è fatta di due file:

| File | Nome tipico | Si condivide? | Dove vive |
|---|---|---|---|
| chiave **privata** | `id_ed25519`, `*.pem` | **mai, segreto assoluto** | solo sul portatile del proprietario |
| chiave **pubblica** | `id_ed25519.pub` | **sì, è sicura** | sulla VM, in `authorized_keys` |

La pubblica è *matematicamente* fatta per essere pubblica: da lì non si risale alla
privata. Puoi mandarla in chat senza problemi.

### 4a. Ogni persona, una volta sola, sul proprio portatile

Serve un account Cloud Veneto personale (registrazione al progetto
`PhysicsOfData-students` con l'SSO UniPD) per passare dal gate, e una coppia di chiavi:

```bash
ssh-keygen -t ed25519 -a 100 -C "mario.rossi@mapd"   # metti una passphrase, non lasciarla vuota
chmod 700 ~/.ssh && chmod 600 ~/.ssh/id_ed25519 && chmod 644 ~/.ssh/id_ed25519.pub
cat ~/.ssh/id_ed25519.pub                             # UNA riga: è questa che mandi
```

> Per entrare in una VM **già accesa** non serve importare una keypair dalla dashboard:
> quella viene iniettata solo al momento della creazione. Su una macchina esistente conta
> solo `authorized_keys`.

### 4b. Chi possiede la macchina autorizza la chiave

⚠️ Attenzione al singolo carattere: `>>` **aggiunge**, `>` **sovrascrive** e chiude fuori
tutti, te compreso.

```bash
install -d -m 700 -o ubuntu -g ubuntu ~/.ssh
echo 'ssh-ed25519 AAAAC3...mario mario.rossi@mapd' >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys && chown ubuntu:ubuntu ~/.ssh/authorized_keys
ssh-keygen -lf ~/.ssh/authorized_keys    # elenca cosa hai autorizzato
```

SSH **ignora in silenzio** `authorized_keys` se i permessi sono troppo aperti: è una
protezione, non un bug. Sintomo tipico: "continua a chiedermi la password" oppure
`Permission denied (publickey)`. La home non dev'essere scrivibile da gruppo/altri,
`~/.ssh` dev'essere `700`, `authorized_keys` `600`, tutto di proprietà dell'utente.

### 4c. Revocare

Cancella la riga di quella persona da `~/.ssh/authorized_keys`. Se qualcuno perde o
espone la propria chiave privata: si toglie la sua **pubblica** da tutte le macchine e
quella chiave è morta; lui ne genera una nuova. Siccome le private non sono mai state
condivise, il danno resta confinato al portatile compromesso.

### 4d. Se vi servisse isolare gli utenti

Il metodo sopra fa entrare tutti come `ubuntu`, quindi chiunque può leggere e modificare i
file — e le chiavi — di chiunque altro. Tra persone che si fidano va benissimo. Se
servisse davvero isolare, si crea un utente Linux per persona
(`sudo adduser --disabled-password --gecos "" mrossi`, poi la sua chiave nel *suo*
`~/.ssh/authorized_keys`) e una cartella comune col gruppo per collaborare. Nota che chi
ha `sudo` è root di fatto e può leggere tutto lo stesso.

### 4e. La chiave tra le macchine è un'altra cosa

Perché lo scheduler possa accendere i worker senza password serve una chiave
**macchina→macchina**, che non appartiene a nessuna persona. **È già dentro l'immagine
snapshot**: non c'è niente da fare a ogni sessione, e non va sostituita con la chiave
personale di qualcuno.

---

## 5 · Le regole assolute

- **Mai modificare il security group `pod-students`** — è condiviso con tutto il corso.
- **Mai mettere IP nel codice.** Gli indirizzi vivono solo in `cluster.txt`.
- **Mai mescolare taglie di macchine** nello stesso cluster.
- **Mai mandare una chiave privata**, e mai metterne una personale su una VM.
- **Mai scrivere risultati dentro la repo** su una macchina dove fai `rm -rf` e ricloni.
- **Niente Docker**, né sulle VM né sul Mac: il cluster è reale.
- **`scripts/cluster_storage_up.sh` non si lancia più.** Montava il volume ed esportava
  l'NFS: appartiene all'architettura vecchia.
- **Mai lanciare la campagna di benchmark senza averla prima calibrata**
  (`--only riferimento`, ~30 minuti). Le stime dei tempi vengono da un solo dato del Mac.

---

## 6 · I flavor, e cosa scegliere

**Letti dalla dashboard il 2026-08-14** — chiude una domanda che era aperta da mesi. Sono
i flavor pubblici, cioè quelli che possiamo davvero creare:

| flavor | vCPU | RAM | RAM/core | disco |
|---|---:|---:|---:|---:|
| `cloudveneto.xlarge` | 8 | 16 GB | 2,0 | 25 GB |
| `cloudveneto.8cores8GB25GB` | 8 | 8 GB | 1,0 | 25 GB |
| **`cloudveneto.large`** | **4** | **8 GB** | **2,0** | 25 GB |
| `cloudveneto.medium` | 2 | 4 GB | 2,0 | 25 GB |
| `cloudveneto.1core4GB25GB` | 1 | 4 GB | 4,0 | 25 GB |

Esistono anche `cloudveneto.30cores180GB…`, `…15cores90GB…` (con GPU) e
`cloudveneto.16cores4GB25GB`, ma sono **`Pubblico: No`**: non li abbiamo.

**Scelta per i benchmark del word count: 5 × `cloudveneto.large`** → 1 scheduler + 4
worker da 4 vCPU / 8 GB (quota: 20 vCPU, 40 GB). Il perché sta in `DECISIONI.md`; in due
righe: 4 core danno tre punti allo sweep sui thread, e 8 GB fanno completare anche il
punto più estremo della curva sulle partizioni invece di lasciarci un buco.

> **Il core in più non è velocità in più, su questo carico.** `SSHCluster` accende **un
> worker per host**, quindi 8 core diventano *un processo con 8 thread* — e la fase Map
> del word count è Python puro che tiene il GIL. Prendere `xlarge` invece di `large`
> raddoppia la quota consumata senza raddoppiare niente. È l'ipotesi che il benchmark
> §9.3 misura; per usare davvero tutti i core servirebbe un worker per core, cioè
> ripetere l'IP in `cluster.txt`.

### Ancora da verificare

- **Il disco delle VM è locale o un volume di rete?** Decide quanto pesa la lettura di
  ~5 GB di Parquet nei tempi dei benchmark. Si scopre alla prima misura di calibrazione.
- **Se un giorno il `silver/` cambia**, va rifatta l'immagine snapshot: le macchine
  create da un'immagine vecchia continuerebbero a leggere dati vecchi **senza dare nessun
  errore**. Oggi non è un problema — i dati attuali sono quelli validati e definitivi.
- **Dove mettere i risultati per condividerli nel gruppo** non è ancora deciso: il volume
  di rete si attacca a una macchina sola, quindi non è la risposta giusta per file così
  piccoli.
