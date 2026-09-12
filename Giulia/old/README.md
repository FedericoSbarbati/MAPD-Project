# Versione precedente — conservata, non più in uso

Prima stesura dei task 2.3.1 e 2.3.2, tenuta qui come riferimento finché il rifacimento
non è completo. **Non va lanciata**: `word_count.py` e `word_count.ipynb` nella cartella
sopra sono la versione corrente del 2.3.1.

| file | stato |
|---|---|
| `scripts/word_count_dask.py` | sostituito da `../word_count.py` |
| `scripts/run_word_count_cloudveneto.py` | non serve più: il cluster si configura da `cluster.txt`, non da IP nel codice |
| `notebooks/word_count_giulia.ipynb` | sostituito da `../word_count.ipynb` |
| `notebooks/word_count_giulia_cloudveneto.ipynb` | non serve più: lo stesso notebook gira su Mac e VM |
| `scripts/task_2_3_2_affiliation_representation.py` | **ancora l'unica versione del 2.3.2** — da riscrivere |
| `requirements.txt`, `environment.yml`, `scripts/setup_mapd_env.sh` | ambiente separato, abbandonato in favore di quello del progetto |

## Perché è stato rifatto

- **Non spiegabile.** 2.466 righe in cui l'algoritmo (~30 righe) è sepolto sotto 20 flag
  argparse, controlli difensivi sui path, doppio JSON di riepilogo e `try/except` che
  inghiottono gli errori. Le linee guida del corso chiedono che ogni studente sappia
  descrivere il proprio contributo e rispondere a domande individuali.
- **Il 2.3.2 non è distribuito**: è uno scan pyarrow in un solo processo con
  `collections.Counter`. In un esame di calcolo distribuito è il problema più grave.
- **IP scritti nel codice** (`10.67.22.118`…), contro la convenzione `cluster.txt` del
  progetto (`docs/PROJECT_CONTEXT.md`, regola 8.6).
- **Notebook che lanciano script che lanciano script** via `subprocess.run`, e tokenizer
  duplicato copia-incolla tra notebook e modulo: due sorgenti di verità destinate a
  divergere.
- **Nessuna misura dietro le scelte.** La lista di stop-word filtrava `fig` e `table` ma
  lasciava passare `usepackage`, che con 255.743 occorrenze era la decima parola del
  corpus.
- **Il "benchmark" misurava la cosa sbagliata**: tempo su fette di dati crescenti
  (3/10/25/50 partizioni), cioè tempo vs *quantità di dati*, non i due benchmark
  richiesti — tempo vs numero di partizioni a dati costanti e vs numero di worker.

Cosa invece era giusto e abbiamo tenuto: leggere dal layer silver invece che dai JSON,
la forma a due fasi Map/Reduce, e l'idea di esportare top-N in CSV più un barplot.
