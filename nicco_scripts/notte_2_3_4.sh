#!/usr/bin/env bash
# Campagna notturna del 2.3.4 — risponde alle tre domande lasciate aperte dalla campagna
# del 2026-09-12 (docs/DECISIONI.md). Si lancia dalla radice della repo, sullo scheduler:
#
#     bash nicco_scripts/notte_2_3_4.sh [cartella_embeddings]
#
# LE TRE DOMANDE, e come questo file le trasforma in misure:
#
#  1 · I FILTRI (duplicati e n_words) cambiano il risultato?  La classifica in cima e'
#      degenere: 4.851 coppie sopra 0,98 e venti consegnate tutte a 1,000000, quindi quali
#      venti escano dipende dall'ordine dei task. Qui si misura, con --top 5000, QUANTE
#      coppie sono a pari merito in cinque configurazioni di filtro. La decisione resta di
#      chi consegna: questo produce i numeri su cui prenderla.
#
#  2 · QUANTO COSTA LO `scatter`?  Con --ripeti-punto 3 lo stesso punto gira tre volte di
#      fila nello stesso cluster: la PRIMA paga il trasferimento dei blocchi, le altre due
#      no (Dask nomina i dati scatterati con l'hash del contenuto). La differenza lo isola,
#      per ogni numero di worker. La previsione da falsificare: a 8 worker deve costare
#      ~2x che a 4, perche' il traffico e' proporzionale alle destinazioni.
#
#  3 · PERCHE' IL PUNTO A UN WORKER DISPERDE L'8%?  (70,9 -> 87,2 s, contro lo 0,3-2% di
#      tutti gli altri.) Due cause possibili, e le misure le separano:
#        - e' QUELLA macchina -> si gira lo stesso punto su ciascuna delle quattro, una per
#          volta, con CORD19_HOSTS;
#        - e' l'ACCENSIONE del cluster e non il calcolo -> --ripeti-punto 8 da' otto misure
#          a cluster fermo, e la loro dispersione si confronta con quella fra passate
#          (--ripetizioni), che invece accende un cluster nuovo ogni volta.
#
# ORDINE: dal piu' importante al piu' lungo, perche' una notte puo' interrompersi (regola
# della campagna del 2.3.1). Se salta la corrente alle 3, le risposte 1 e 2 sono al sicuro.
# Nessun `set -e`: un esperimento che fallisce non deve portarsi via quelli dopo.

set -u

DATI="${1:-$HOME/mapd-data/embeddings}"
PAPERS="$HOME/mapd-data/silver/papers"
OUT="$HOME/mapd-out/notte"
LOG="$HOME/notte-2_3_4.log"

mkdir -p "$OUT"
exec > >(tee -a "$LOG") 2>&1

source ~/pyvenv/bin/activate
PY=python

# Gli IP dal file, mai scritti qui dentro (regola 5 di CLAUDE.md)
HOSTS=$(grep -v '^#' cluster.txt | head -1)
SCHED=$(echo "$HOSTS" | cut -d, -f1)
WORKERS=$(echo "$HOSTS" | cut -d, -f2-)

banner() { echo; echo "================================================================"; \
           echo "$(date '+%F %T')  $*"; echo "================================================================"; }

banner "CAMPAGNA NOTTURNA 2.3.4 — inizio"
echo "dati      : $DATI"
echo "scheduler : $SCHED"
echo "worker    : $WORKERS"
echo "output    : $OUT"
INIZIO=$(date +%s)

# ----------------------------------------------------------------------------------
banner "1/4 · I FILTRI — 5 configurazioni, ~6 min"
# --top 5000 serve a CONTARE i pari merito: con --top 20 non si vedrebbe che sono
# migliaia. Cartelle separate perche' ognuna consegna classifiche proprie.
for caso in "nessuno:" "parole2:--min-parole 2" "parole3:--min-parole 3" \
            "unici:--solo-unici" "entrambi:--min-parole 3 --solo-unici"; do
    nome="${caso%%:*}"; opzioni="${caso#*:}"
    echo; echo "--- filtro: $nome  ($opzioni)"
    $PY nicco_scripts/cosine.py "$DATI" --papers "$PAPERS" --blocchi 32 --top 5000 \
        --out "$OUT/filtro_$nome" $opzioni | grep -Ev "^  [+-]"
done

# ----------------------------------------------------------------------------------
banner "2/4 · LO SCATTER A 8 WORKER — il pezzo che manca, ~11 min"
# Due processi per macchina (CORD19_HOSTS raddoppiata) e un thread ciascuno: gli stessi 8
# core del 4x2, ma il trasferimento va a otto destinazioni invece di quattro.
# CORD19_WORKER_MEMORY_LIMIT non e' opzionale: due worker sulla stessa macchina
# chiederebbero il 170% della sua RAM e li fermerebbe l'OOM killer del kernel.
CORD19_HOSTS="$SCHED,$WORKERS,$WORKERS" CORD19_WORKER_MEMORY_LIMIT=3.5GB \
    $PY nicco_scripts/bench_cosine.py "$DATI" \
        --only worker --worker 8 --thread 1 --k 32 \
        --ripeti-punto 3 --ripetizioni 6 --no-baseline --out "$OUT/scatter_8w"

# ----------------------------------------------------------------------------------
banner "3/4 · LO SCATTER DA 1 A 4 WORKER — ~67 min"
$PY nicco_scripts/bench_cosine.py "$DATI" --only worker --k 32 \
    --ripeti-punto 3 --ripetizioni 6 --no-baseline --out "$OUT/scatter_worker"

banner "3b/4 · LO SCATTER DIPENDE DA k? — ~11 min"
# Il traffico e' sempre 120 MB, quindi la previsione e' che NON dipenda da k. Se invece
# cresce coi blocchi, il costo non e' il trasferimento ma il numero di chiavi da gestire.
$PY nicco_scripts/bench_cosine.py "$DATI" --only partizioni \
    --ripeti-punto 3 --ripetizioni 2 --no-baseline --out "$OUT/scatter_partizioni"

# ----------------------------------------------------------------------------------
banner "4/4 · LA DISPERSIONE A UN WORKER, MACCHINA PER MACCHINA — ~3,6 h"
# Otto misure a cluster fermo (--ripeti-punto 8) ripetute su cinque cluster diversi
# (--ripetizioni 5): la dispersione DENTRO un cluster e quella FRA cluster si leggono
# separate, e il ciclo dice se una macchina e' peggiore delle altre.
indice=1
for w in $(echo "$WORKERS" | tr ',' ' '); do
    banner "4/4 · worker unico = macchina $indice ($w)"
    CORD19_HOSTS="$SCHED,$w" $PY nicco_scripts/bench_cosine.py "$DATI" \
        --only worker --worker 1 --k 32 \
        --ripeti-punto 8 --ripetizioni 5 --no-baseline --out "$OUT/nodo_$indice"
    indice=$((indice + 1))
done

# ----------------------------------------------------------------------------------
banner "FINE — $(( ($(date +%s) - INIZIO) / 60 )) minuti"
echo "misure scritte:"
for f in "$OUT"/*/misure.csv; do
    [ -f "$f" ] && echo "  $(( $(wc -l < "$f") - 1 ))  $f"
done
echo
echo "classifiche dei filtri:"
for f in "$OUT"/filtro_*/most_similar.csv; do
    [ -f "$f" ] && echo "  $(dirname "$f" | xargs basename): $(( $(wc -l < "$f") - 1 )) coppie"
done
