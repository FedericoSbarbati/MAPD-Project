"""Accendere il cluster. È l'unica cosa che questo file fa.

Prima si chiamava `bench.py` e conteneva anche l'impalcatura di misura: 500 righe che
non hanno mai prodotto un numero. Le misure adesso stanno nel benchmark del task che le
usa; qui resta la creazione del Client, che è l'unica cosa davvero condivisa fra i
quattro task.

    from cluster import get_client
    client, cluster = get_client()              # tutti i worker di cluster.txt
    client, cluster = get_client(n_workers=2)   # solo i primi 2 (curva sui worker)
    client, cluster = get_client(n_threads=1)   # un thread ciascuno (misura sui thread)
    ...
    client.close(); cluster.close()

Dove gira lo decide `cluster.txt`, mai il codice:

    DASK_SCHEDULER              uno scheduler già acceso -> ci si attacca e basta
    CORD19_HOSTS / cluster.txt  "ip_scheduler,ip_worker1,..." -> SSHCluster.
                                Il PRIMO host fa SOLO da scheduler; per farlo lavorare
                                anche come worker, ripetilo come secondo.
    nessuno dei due             -> LocalCluster, per sviluppare sul Mac

`cluster.txt` è git-ignored: le VM di Cloud Veneto cambiano IP a ogni sessione.
"""

import os

HOSTS_FILE = "cluster.txt"


def read_hosts(repo_root="."):
    """La lista di host e da dove viene: (hosts, provenienza).

    Quando non trova niente restituisce (None, elenco dei posti guardati), così chi
    chiama può dire DOVE ha cercato invece di scendere in silenzio su una macchina sola.
    """
    if os.environ.get("CORD19_HOSTS"):
        return os.environ["CORD19_HOSTS"], "$CORD19_HOSTS"

    cercati = []
    for directory in (repo_root, os.getcwd(), os.path.expanduser("~")):
        percorso = os.path.abspath(os.path.join(directory, HOSTS_FILE))
        if percorso in cercati:
            continue
        cercati.append(percorso)
        if os.path.exists(percorso):
            for riga in open(percorso):
                riga = riga.strip()
                if riga and not riga.startswith("#"):
                    return riga, percorso
    return None, cercati


def available_workers(repo_root="."):
    """Quanti worker si possono chiedere a `get_client(n_workers=...)`.

    Serve alla curva "tempo vs numero di worker", che deve sapere da dove partire per
    scendere. Con `cluster.txt` sono gli host meno il primo, che fa solo da scheduler.
    """
    hosts, _ = read_hosts(repo_root)
    if hosts:
        return len([h for h in hosts.split(",") if h.strip()]) - 1
    core = os.cpu_count() or 2
    return int(os.environ.get("CORD19_WORKERS", max(1, min(4, core // 2))))


def configure_memory():
    """Le impostazioni che tengono piatta la RAM dei worker su lavori lunghi di testo.

    `MALLOC_TRIM_THRESHOLD_` e `MALLOC_ARENA_MAX` devono arrivare al processo worker
    QUANDO NASCE, prima che glibc inizializzi l'allocatore: passarle da
    `worker_options={"env": ...}` non funziona, arrivano tardi. `pre-spawn-environ` è
    l'aggancio che gira abbastanza presto, e SSHCluster porta la configurazione del
    driver sui nodi da solo. → `docs/MEMORY_LEAK_REPORT.md` §7.2

    Va chiamata PRIMA di creare il cluster: è per questo che la creazione del Client sta
    in un file solo, invece di essere copiata in ogni task.
    """
    import dask

    environ = dict(dask.config.get("distributed.nanny.pre-spawn-environ"))
    environ.update({"MALLOC_TRIM_THRESHOLD_": 0, "MALLOC_ARENA_MAX": 2})
    dask.config.set({"distributed.nanny.pre-spawn-environ": environ})

    # Lo shuffle P2P non riesce a ricostruire la propria spec sullo scheduler quando il
    # grafo nasce da un Bag, e muore con "P2P <id> failed during transfer phase".
    # → PROJECT_CONTEXT.md §8.5
    dask.config.set({"dataframe.shuffle.method": "tasks"})


def worker_options(n_threads=None):
    """Dimensionamento dei worker, deliberatamente RELATIVO.

    Il cluster si ricostruisce da uno snapshot a ogni sessione e la taglia delle VM
    cambia: 4 GB oggi, 8 GB domani. Un "7GB" scritto qui dentro su una VM da 3,8 GB
    viene ignorato dalla nanny in silenzio, e il worker crede di avere memoria che non
    ha. → `local/NOTES.md` v6

    `n_threads` scritto a mano vince su `CORD19_THREADS_PER_WORKER`, che resta il
    default: chi chiede esplicitamente sta misurando, chi usa la variabile d'ambiente
    sta configurando.
    """
    opzioni = {"memory_limit": float(os.environ.get("CORD19_WORKER_MEMORY_FRACTION", "0.85"))}
    if os.environ.get("CORD19_WORKER_MEMORY_LIMIT"):
        opzioni["memory_limit"] = os.environ["CORD19_WORKER_MEMORY_LIMIT"]
    thread = n_threads or os.environ.get("CORD19_THREADS_PER_WORKER")
    if thread:
        opzioni["nthreads"] = int(thread)
    return opzioni


def describe(client, modo, dettaglio):
    """Dire ad alta voce su cosa stiamo girando davvero.

    Esiste perché il fallback silenzioso costa caro: un run che usa una macchina invece
    di quattro sembra un cluster lento, non una configurazione sbagliata, e te ne accorgi
    a sessione bruciata. La riga da guardare è `worker: N su 1 host ['127.0.0.1']`.
    """
    workers = client.scheduler_info()["workers"].values()
    host = {w["host"] for w in workers}
    memoria = sum(w.get("memory_limit", 0) for w in workers)
    print("=" * 70)
    print(f"cluster   : {modo}")
    print(f"            {dettaglio}")
    print(f"worker    : {len(workers)} su {len(host)} host {sorted(host)}")
    print(f"thread    : {sum(w.get('nthreads', 0) for w in workers)}")
    print(f"memoria   : {memoria / 1e9:.1f} GB totali "
          f"({memoria / max(len(workers), 1) / 1e9:.1f} GB per worker)")
    print(f"dashboard : {client.dashboard_link}")
    if modo == "LocalCluster":
        print("            ATTENZIONE: tutto su questa sola macchina. Per usare il")
        print("            cluster serve cluster.txt nella root del repo.")
    print("=" * 70)


def get_client(repo_root=".", n_workers=None, n_threads=None):
    """Connettersi al cluster per cui questa macchina è configurata.

    I due argomenti sono i pomelli dei benchmark, e insieme si leggono come «il cluster
    ha `n_workers` worker da `n_threads` thread»:

    `n_workers` limita quanti worker usare: con `cluster.txt` prende i PRIMI n della
    lista (lo scheduler resta sempre il primo host e non conta), in locale dimensiona il
    LocalCluster. È il pomello della curva "tempo vs numero di worker": invece di
    rimpicciolire un cluster già acceso — cosa che su SSHCluster si può fare in una sola
    direzione, e obbliga a ricordarsi un ordine — se ne accende uno nuovo per ogni punto.

    `n_threads` fissa i thread di OGNI worker. È l'altra lettura di "processing units":
    più thread nello stesso processo condividono il GIL, più worker no.

    Restituisce (client, cluster). `cluster` è None quando ci siamo solo attaccati a uno
    scheduler acceso da qualcun altro. Vanno chiusi tutti e due.
    """
    from dask.distributed import Client, LocalCluster

    configure_memory()  # prima che nasca qualunque processo worker

    scheduler = os.environ.get("DASK_SCHEDULER")
    hosts, provenienza = read_hosts(repo_root)

    if scheduler:
        client = Client(scheduler)
        describe(client, "scheduler esistente", scheduler)
        return client, None

    if hosts:
        from dask.distributed import SSHCluster

        indirizzi = [h.strip() for h in hosts.split(",") if h.strip()]
        if len(indirizzi) < 2:
            raise SystemExit(
                f"{provenienza} elenca un solo host ({indirizzi}). Il PRIMO host fa solo\n"
                "da scheduler: servono almeno due voci perché esista un worker. Ripeti\n"
                "lo scheduler come secondo host se vuoi che lavori anche lui."
            )
        if n_workers is not None:
            disponibili = len(indirizzi) - 1
            if not 1 <= n_workers <= disponibili:
                raise SystemExit(
                    f"Chiesti {n_workers} worker ma {provenienza} ne elenca "
                    f"{disponibili}. Aggiungi host a cluster.txt: da qui non si inventano."
                )
            indirizzi = indirizzi[:n_workers + 1]

        connessione = {"known_hosts": None}  # gli IP interni si riciclano fra sessioni
        if os.environ.get("CORD19_SSH_KEY"):
            connessione["client_keys"] = [os.environ["CORD19_SSH_KEY"]]
        cluster = SSHCluster(
            indirizzi,
            connect_options=connessione,
            worker_options=worker_options(n_threads),
            scheduler_options={"port": 8786, "dashboard_address": ":8787"},
            # None = "lo stesso interprete del driver". I worker sono cloni snapshot
            # dello scheduler, quindi ~/pyvenv sta nello stesso posto.
            remote_python=os.environ.get("CORD19_REMOTE_PYTHON"),
        )
        client = Client(cluster)
        client.wait_for_workers(len(indirizzi) - 1, timeout="180s")
        describe(client, "SSHCluster",
                 f"{provenienza} -> scheduler {indirizzi[0]}, worker {indirizzi[1:]}")
        return client, cluster

    # Nessuna lista di host: gira tutto qui. Il LocalCluster si dimensiona su QUESTA
    # macchina, non su un numero che aveva senso su un'altra.
    core = os.cpu_count() or 2
    quanti = n_workers or available_workers(repo_root)
    opzioni = worker_options(n_threads)
    # I thread per worker si dividono sul numero MASSIMO di worker, non su quelli
    # chiesti adesso: cosi' chiedere meno worker vuol dire meno potenza di calcolo in
    # tutto, come sul cluster vero, dove ogni nodo porta i propri core. Dividendo su
    # `quanti` un worker solo si prenderebbe tutti i core della macchina, e la prova
    # generale della curva sui worker misurerebbe processi-contro-thread.
    per_worker = max(1, core // available_workers(repo_root))
    cluster = LocalCluster(
        n_workers=quanti,
        threads_per_worker=opzioni.get("nthreads", per_worker),
        memory_limit=opzioni["memory_limit"] / quanti
        if isinstance(opzioni["memory_limit"], float)
        else opzioni["memory_limit"],
        processes=True,
    )
    client = Client(cluster)
    describe(client, "LocalCluster", f"nessun cluster.txt in: {', '.join(provenienza)}")
    return client, cluster
