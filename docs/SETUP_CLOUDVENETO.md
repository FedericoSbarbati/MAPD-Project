# Setup Cloud Veneto — accesso SSH sicuro per il gruppo

Guida operativa per il progetto MAPD-B su Cloud Veneto (progetto condiviso
`PhysicsOfData-students`). Questo documento copre **come dare accesso ai
compagni a una VM in modo sicuro**.

> Sezioni in arrivo (prossime sessioni): creazione delle VM del cluster,
> installazione di Dask, upload del dataset silver, benchmark.

---

## 0. Il concetto in una frase (leggere PRIMA di tutto)

**Non condividi MAI una chiave privata.** Ogni compagno genera la propria coppia
di chiavi **sul proprio portatile**, ti manda **solo la parte pubblica** (una
riga di testo, innocua), e tu la registri sulla VM. Così ognuno entra con la
**propria** chiave, che non lascia mai il suo computer.

### Perché è sicuro: chiave privata vs chiave pubblica

Una coppia di chiavi SSH è fatta di due file:

| File | Nome tipico | Si condivide? | Dove vive |
|------|-------------|---------------|-----------|
| Chiave **privata** | `id_ed25519`, `*.pem` | **MAI. Segreto assoluto.** | Solo sul portatile del proprietario |
| Chiave **pubblica** | `id_ed25519.pub` | **Sì, è sicura da diffondere** | Sulla VM, in `authorized_keys` |

La chiave pubblica è *matematicamente* progettata per essere pubblica: da essa
**non** si può risalire alla privata. Puoi mandarla su WhatsApp, per email, o
metterla su GitHub: non succede niente. Il "segreto" è **solo** la chiave
privata, e con questa procedura non la tocca nessuno tranne il suo proprietario.

### Cosa finisce (e cosa NON finisce) sulla VM condivisa

- ✅ Sulla VM ci vanno **solo chiavi pubbliche** (`.pub`).
- ❌ Sulla VM **non** ci va **nessuna chiave privata** personale, **né** il file
  `.pem` scaricato dalla dashboard, **né** la chiave del gate.
- ⚠️ L'unica chiave privata che può stare su una VM è la **chiave macchina del
  cluster Dask** (nodo→nodo), che è una chiave *dedicata* e non appartiene a
  nessuna persona — vedi §7.

---

## 1. Prerequisiti per OGNI compagno (una tantum)

Ogni membro del gruppo, sul **proprio** portatile, deve avere due cose.

### 1a. Un account Cloud Veneto (per il gate)

La VM sta su una rete interna raggiungibile solo passando da
`gate.cloudveneto.it`. Per usarlo, ogni compagno deve avere il **proprio**
account Cloud Veneto: registrarsi al progetto `PhysicsOfData-students` con la
propria SSO UniPD (vedi la guida del corso), attivare l'account e cambiare la
password al primo login:

```bash
ssh IL_TUO_USERNAME_CLOUDVENETO@gate.cloudveneto.it
```

Questo account è **personale** e non si condivide con nessuno.

### 1b. Una coppia di chiavi SSH per entrare nella VM

Sempre sul **proprio** portatile (su Windows: dentro il terminale **WSL**, o con
l'OpenSSH di PowerShell), ognuno genera la propria coppia. Consigliato il tipo
`ed25519` (moderno, corto, sicuro) **con passphrase**:

```bash
ssh-keygen -t ed25519 -a 100 -C "mario.rossi@mapd"
# - Premi INVIO per accettare il path di default: ~/.ssh/id_ed25519
# - IMPOSTA una passphrase quando te la chiede (protegge la chiave se ti
#   rubano il portatile). Non lasciarla vuota.
```

Vengono creati due file:

- `~/.ssh/id_ed25519`     → **privata**, resta qui per sempre
- `~/.ssh/id_ed25519.pub` → **pubblica**, questa la manderai

Metti i permessi corretti sulla privata (SSH è schizzinoso):

```bash
chmod 700 ~/.ssh
chmod 600 ~/.ssh/id_ed25519
chmod 644 ~/.ssh/id_ed25519.pub
```

### 1c. Mandare SOLO la chiave pubblica al responsabile

Il responsabile della VM (chi la crea) raccoglie le pubbliche di tutti. Ogni
compagno stampa la propria e la incolla nel canale del gruppo:

```bash
cat ~/.ssh/id_ed25519.pub
# Output = UNA riga sola, tipo:
# ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI...9k mario.rossi@mapd
```

Manda **quella riga**. È sicuro mandarla dove vuoi (chat, email). **Non** mandare
mai il file *senza* `.pub`.

> **Nota importante:** per far entrare un compagno in una VM **già accesa** NON
> serve che lui "importi una keypair" dalla dashboard di Cloud Veneto. La
> keypair della dashboard viene iniettata solo al **momento della creazione**
> della VM. Per una VM esistente conta solo il file `authorized_keys` (§2).

### 1d. (Opzionale ma consigliato) Verificare che la chiave sia autentica

Per essere rigorosi ed evitare che qualcuno "sostituisca" una chiave, il
proprietario legge il **fingerprint** su un canale fidato e il responsabile
verifica che combaci:

```bash
# il proprietario, sul suo portatile:
ssh-keygen -lf ~/.ssh/id_ed25519.pub
# 256 SHA256:abcd...WXYZ mario.rossi@mapd (ED25519)

# il responsabile, sulla riga ricevuta salvata in mario.pub:
ssh-keygen -lf mario.pub
# deve uscire lo STESSO SHA256:abcd...WXYZ
```

---

## 2. Metodo consigliato: utente condiviso `ubuntu`

È il metodo più semplice, adatto a un gruppo di 4 persone che si fidano, e
combacia con la guida Dask del corso. Tutti entrano come utente `ubuntu`; tu
aggiungi le loro chiavi pubbliche al `authorized_keys` di `ubuntu`.

**Chi esegue:** il responsabile, già collegato alla VM come `ubuntu`.

### 2a. Assicurati che `~/.ssh` esista con i permessi giusti

```bash
install -d -m 700 -o ubuntu -g ubuntu ~/.ssh
```

### 2b. Aggiungi le chiavi pubbliche — SEMPRE in append (`>>`)

⚠️ **Attenzione al singolo carattere:** usa `>>` (aggiunge in fondo), **mai** `>`
(sovrascrive e cancella le chiavi esistenti, bloccando fuori tutti, te compreso).

```bash
# una riga per ciascun compagno (incolla la SUA riga pubblica tra apici singoli)
echo 'ssh-ed25519 AAAAC3...mario  mario.rossi@mapd'   >> ~/.ssh/authorized_keys
echo 'ssh-ed25519 AAAAC3...laura  laura.bianchi@mapd' >> ~/.ssh/authorized_keys
echo 'ssh-ed25519 AAAAC3...gino   gino.verdi@mapd'    >> ~/.ssh/authorized_keys
```

In alternativa, apri il file con un editor e incolla le righe a mano:

```bash
nano ~/.ssh/authorized_keys   # incolla le righe, una per chiave, poi salva
```

### 2c. Blinda i permessi

```bash
chmod 600 ~/.ssh/authorized_keys
chown ubuntu:ubuntu ~/.ssh/authorized_keys
```

### 2d. Verifica cosa hai autorizzato

```bash
# elenca i fingerprint di tutte le chiavi ora autorizzate (per controllo)
ssh-keygen -lf ~/.ssh/authorized_keys
# conta le righe = numero di chiavi autorizzate
wc -l ~/.ssh/authorized_keys
```

### 2e. Come si connettono i compagni

Dal **loro** portatile, un solo comando (jump attraverso il gate). `USERNAME_CV`
è il loro username Cloud Veneto (per il gate), l'utente della VM è `ubuntu`,
l'identità è la **loro** chiave privata:

```bash
ssh -J USERNAME_CV@gate.cloudveneto.it ubuntu@10.67.22.XYZ -i ~/.ssh/id_ed25519
```

**Pro:** semplicissimo, un solo `authorized_keys`, funziona subito con Dask.
**Contro:** tutti condividono lo stesso home `ubuntu`, quindi ognuno può vedere
e modificare i file degli altri **e** il file `authorized_keys` stesso. Tra 4
compagni fidati va benissimo. Se vuoi che nessuno possa toccare le chiavi degli
altri, usa il §3.

---

## 3. Metodo a massima isolazione (opzionale): un utente per persona

Con questo metodo ogni compagno ha il **proprio** utente Linux, con il **proprio**
`authorized_keys` **non modificabile né leggibile dagli altri** utenti normali.
È quello che soddisfa alla lettera "le loro chiavi non devono essere modificabili
o accessibili da altri".

**Chi esegue:** il responsabile come `ubuntu` (che ha i privilegi `sudo`). Per
ogni compagno, esempio con l'utente `mrossi`:

### 3a. Crea l'utente senza password (solo chiave)

```bash
sudo adduser --disabled-password --gecos "" mrossi
```

`--disabled-password` = non è possibile entrare con password, **solo** con chiave.

### 3b. Installa la SUA chiave pubblica nel SUO `authorized_keys`

```bash
sudo install -d -m 700 -o mrossi -g mrossi /home/mrossi/.ssh
printf '%s\n' 'ssh-ed25519 AAAAC3...mario  mario.rossi@mapd' \
  | sudo tee /home/mrossi/.ssh/authorized_keys >/dev/null
sudo chmod 600 /home/mrossi/.ssh/authorized_keys
sudo chown -R mrossi:mrossi /home/mrossi/.ssh
```

Risultato: `/home/mrossi/.ssh/authorized_keys` è di proprietà di `mrossi` con
permessi `600`. Gli **altri** utenti normali (laura, gino…) **non** possono
leggerlo né modificarlo. Ognuno può cambiare **solo** la propria chiave.

### 3c. Cartella condivisa per collaborare (senza toccare le `~/.ssh`)

Così i compagni lavorano insieme sui file del progetto pur restando isolati sulle
proprie home:

```bash
sudo groupadd mapd
sudo usermod -aG mapd mrossi        # ripeti per ogni compagno
# cartella condivisa, setgid (2xxx) => i nuovi file ereditano il gruppo 'mapd'
sudo install -d -m 2770 -o root -g mapd /srv/mapd
```

Mettete lì il repo, il dataset silver e gli output condivisi.

### 3d. Come si connettono (utente = il proprio, non `ubuntu`)

```bash
ssh -J USERNAME_CV@gate.cloudveneto.it mrossi@10.67.22.XYZ -i ~/.ssh/id_ed25519
```

> **Nota su `sudo`:** chi ha `sudo` può comunque leggere qualsiasi file (è root
> di fatto). Se vuoi isolazione vera, **non** dare `sudo` a tutti: tienilo solo
> al responsabile, e crea gli altri come utenti normali. Per Dask non serve
> `sudo` una volta installato l'ambiente.

---

## 4. Permessi & hardening (OBBLIGATORIO — altrimenti SSH rifiuta la chiave)

SSH **ignora silenziosamente** `authorized_keys` se i permessi sono troppo
aperti (è una protezione, non un bug). Sintomo tipico: "continua a chiedermi la
password" oppure "Permission denied (publickey)". Controlla sempre:

```bash
ls -ld ~ ~/.ssh                 # ~ NON deve essere scrivibile da gruppo/altri
ls -l  ~/.ssh/authorized_keys   # deve essere -rw------- (600), owner corretto
```

Regole:

- `~` (home): non scrivibile da group/other (es. `755` o `750`, mai `777`).
- `~/.ssh`: `700`.
- `~/.ssh/authorized_keys`: `600`.
- proprietario di tutto: l'utente stesso (`chown`).

### (Consigliato) Forzare solo-chiave, niente password

Sulla VM, come `sudo`, crea un drop-in dedicato (non rovini il file principale):

```bash
sudo tee /etc/ssh/sshd_config.d/10-mapd.conf >/dev/null <<'EOF'
PasswordAuthentication no
PubkeyAuthentication yes
PermitRootLogin no
EOF
sudo systemctl restart ssh
```

⚠️ **Prima di riavviare, tieni aperta un'altra sessione SSH già dentro la VM**,
così se sbagli qualcosa non ti chiudi fuori. Verifica di riuscire a entrare da
una **nuova** shell *prima* di chiudere quella di sicurezza.

---

## 5. Verifica che funzioni

Sul portatile del compagno, prova con output verboso per capire cosa succede:

```bash
ssh -v -J USERNAME_CV@gate.cloudveneto.it ubuntu@10.67.22.XYZ -i ~/.ssh/id_ed25519
# cerca righe tipo: "Authentication succeeded (publickey)"
```

Sulla VM, guarda i log di autenticazione in tempo reale mentre il compagno prova:

```bash
sudo journalctl -u ssh -f          # oppure: sudo tail -f /var/log/auth.log
```

---

## 6. Revocare l'accesso (quando serve)

**Metodo `ubuntu` condiviso (§2):** apri `~/.ssh/authorized_keys` ed elimina la
riga di quella persona.

```bash
nano ~/.ssh/authorized_keys        # cancella la riga, salva
```

**Metodo per-utente (§3):** disattiva o rimuovi l'utente.

```bash
sudo usermod -L mrossi             # blocca l'accesso (reversibile)
sudo deluser --remove-home mrossi  # rimuove utente e home (definitivo)
```

**Se un compagno perde/espone la sua chiave privata:** rimuovete la sua chiave
**pubblica** da tutte le VM (righe sopra) e quella chiave è morta. Lui ne genera
una nuova (§1b) e rimanda solo la nuova pubblica. Poiché le private non sono mai
state condivise, il danno è contenuto al singolo portatile compromesso.

---

## 7. Nota: le chiavi del cluster Dask sono un'altra cosa

L'accesso **umano** (questo documento) è diverso dalla comunicazione
**macchina→macchina** che serve a Dask per far parlare scheduler e worker senza
password. Quella usa una chiave **dedicata**, generata sullo **scheduler**, la
cui parte pubblica va negli `authorized_keys` dei **worker**:

- Non è la chiave personale di nessuno: è una "chiave di servizio" del cluster.
- La sua parte privata vive solo sullo scheduler e, anche se un compagno la
  leggesse, aprirebbe soltanto le altre VM del cluster (a cui ha già accesso):
  nessuna escalation.
- **Non riutilizzare** una chiave personale (né il `.pem` del gate) per questo
  scopo: tienila separata.

I comandi esatti per generarla e distribuirla saranno nella sezione
"Installazione Dask" (guida `Dask installation` del corso).

---

## Checklist finale — le regole d'oro

- ✅ Ogni persona genera la **propria** coppia di chiavi sul **proprio** portatile.
- ✅ Si condivide **solo** la chiave **pubblica** (`.pub`), una riga di testo.
- ✅ Sulla VM: `~/.ssh` a `700`, `authorized_keys` a `600`, owner corretto.
- ✅ Aggiungi chiavi **sempre in append** (`>>`), mai in overwrite (`>`).
- ❌ Non mandare/copiare **mai** una chiave **privata** o un `.pem`.
- ❌ Non mettere **mai** una chiave privata personale sulla VM condivisa.
- ❌ Non fare `chmod 777` su `~/.ssh` (SSH smette di funzionare).
