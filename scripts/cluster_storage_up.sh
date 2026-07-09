#!/usr/bin/env bash
#
# Prepara lo storage condiviso del cluster Dask su Cloud Veneto con UN SOLO comando.
#
# Va eseguito SULLO SCHEDULER (la VM a cui hai attaccato il volume dalla dashboard).
# Da lì, sfruttando l'SSH senza-password scheduler->worker gia' presente nello
# snapshot (la chiave macchina del cluster), monta /data su TUTTI i worker via NFS.
# Cosi' non apri 10 terminali: ne basta uno.
#
# Cosa fa (idempotente: si puo' rilanciare senza danni):
#   1. [scheduler] monta il volume dati su /data          (MAI mkfs: cancellerebbe i dati)
#   2. [scheduler] installa/abilita il server NFS + esporta /data alla subnet interna
#   3. [worker*]   monta /data via NFS allo STESSO path /data e verifica che i dati si vedano
#
# Uso (sullo scheduler):
#   bash scripts/cluster_storage_up.sh                 # legge gli IP dei worker da cluster.txt
#   bash scripts/cluster_storage_up.sh 10.67.22.35 10.67.22.254   # oppure IP a mano
#   bash scripts/cluster_storage_up.sh --down          # teardown: smonta tutto (prima di detach volume)
#
# Prerequisiti (gia' soddisfatti nel tuo snapshot):
#   - volume dati attaccato allo scheduler dalla dashboard (device tipo /dev/vdb)
#   - SSH passwordless scheduler->worker come utente ubuntu (chiave del cluster)
#
set -euo pipefail

# ----------------------------- CONFIG -----------------------------
VOL_DEV="${VOL_DEV:-/dev/vdb}"          # device del volume sullo scheduler (verifica con: lsblk)
MOUNT="${MOUNT:-/data}"                 # STESSO path su scheduler e worker (non cambiarlo su un solo nodo!)
SUBNET="${SUBNET:-10.67.22.0/24}"       # subnet interna del progetto: a chi e' permesso montare l'NFS
CHECK_FILE="${CHECK_FILE:-$MOUNT/MAPD-Project/archive/metadata.csv}"  # file di prova per la verifica finale
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_TXT="$REPO_DIR/cluster.txt"
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes"

# IP interno dello scheduler (i worker useranno questo per montare l'NFS)
SCHED_IP="$(hostname -I | tr ' ' '\n' | grep -E '^10\.67\.22\.' | head -n1)"
[ -n "$SCHED_IP" ] || SCHED_IP="$(hostname -I | awk '{print $1}')"

# --------------------- elenco IP dei worker -----------------------
# Priorita': argomenti da riga di comando; altrimenti li ricava da cluster.txt
# (formato "ip_sched,ip_sched,ip_worker1,ip_worker2,..."), togliendo l'IP dello scheduler.
DOWN=0
WORKERS=()
for a in "$@"; do
  if [ "$a" = "--down" ]; then DOWN=1; else WORKERS+=("$a"); fi
done

if [ ${#WORKERS[@]} -eq 0 ] && [ -f "$CLUSTER_TXT" ]; then
  line="$(grep -vE '^\s*#' "$CLUSTER_TXT" | grep -vE '^\s*$' | head -n1)"
  IFS=',' read -ra all <<< "$line"
  for ip in "${all[@]}"; do
    ip="$(echo "$ip" | xargs)"                 # trim spazi
    [ -z "$ip" ] && continue
    [ "$ip" = "$SCHED_IP" ] && continue         # lo scheduler non e' un worker NFS remoto
    case " ${WORKERS[*]:-} " in *" $ip "*) ;; *) WORKERS+=("$ip");; esac  # dedup
  done
fi

if [ ${#WORKERS[@]} -eq 0 ]; then
  echo "!! Nessun worker indicato. Passa gli IP come argomenti, o popola cluster.txt." >&2
  exit 1
fi

echo ">> scheduler = $SCHED_IP    worker = ${WORKERS[*]}"

# ============================== TEARDOWN ==============================
if [ "$DOWN" -eq 1 ]; then
  echo ">> [teardown] smonto l'NFS sui worker"
  for w in "${WORKERS[@]}"; do
    echo "   -> $w"
    ssh $SSH_OPTS ubuntu@"$w" "sudo umount -f -l $MOUNT 2>/dev/null || true"
  done
  echo ">> [teardown] fermo l'export e smonto il volume sullo scheduler"
  sudo exportfs -ua || true
  sudo systemctl stop nfs-kernel-server || true
  sudo umount "$MOUNT" 2>/dev/null || echo "   (/data era gia' smontato o in uso: chiudi il notebook e riprova)"
  echo ">> teardown completato. Ora puoi detach del volume + delete delle VM dalla dashboard."
  exit 0
fi

# =============================== SETUP ================================
echo ">> [scheduler] monto il volume $VOL_DEV su $MOUNT"
sudo mkdir -p "$MOUNT"
if mountpoint -q "$MOUNT"; then
  echo "   $MOUNT gia' montato, ok"
else
  # NB: SOLO mount, MAI mkfs qui. Il volume e' gia' formattato (mkfs si fa una volta sola).
  sudo mount "$VOL_DEV" "$MOUNT"
fi
sudo chown ubuntu:ubuntu "$MOUNT" || true

echo ">> [scheduler] configuro il server NFS e l'export di $MOUNT"
if ! dpkg -s nfs-kernel-server >/dev/null 2>&1; then
  sudo apt-get update -qq && sudo apt-get install -y -qq nfs-kernel-server
fi
EXPORT_LINE="$MOUNT $SUBNET(rw,sync,no_subtree_check)"
if ! grep -qxF "$EXPORT_LINE" /etc/exports 2>/dev/null; then
  echo "$EXPORT_LINE" | sudo tee -a /etc/exports >/dev/null
fi
sudo exportfs -ra
sudo systemctl enable --now nfs-kernel-server
echo "   export attivi:"; sudo exportfs -v | sed 's/^/     /'

echo ">> [worker] monto $MOUNT via NFS su ogni worker"
fail=0
for w in "${WORKERS[@]}"; do
  echo "   -> $w"
  ssh $SSH_OPTS ubuntu@"$w" "
    set -e
    dpkg -s nfs-common >/dev/null 2>&1 || { sudo apt-get update -qq && sudo apt-get install -y -qq nfs-common; }
    sudo mkdir -p '$MOUNT'
    mountpoint -q '$MOUNT' || sudo mount -t nfs '$SCHED_IP:$MOUNT' '$MOUNT'
    if [ -e '$CHECK_FILE' ]; then echo '      OK: dati visibili sul worker'; else echo '      ATTENZIONE: dati NON visibili ($CHECK_FILE)'; exit 3; fi
  " || { echo "      !! problema sul worker $w"; fail=1; }
done

if [ "$fail" -eq 0 ]; then
  echo ">> Tutto pronto. Ora lancia il notebook: i worker vedono $MOUNT."
else
  echo ">> Finito CON ERRORI: controlla i worker segnalati sopra (SSH? security group porta 2049? volume montato?)." >&2
  exit 1
fi
