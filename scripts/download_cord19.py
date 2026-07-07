#!/usr/bin/env python3
"""
Scarica il dataset CORD-19 da Kaggle sul VOLUME persistente di Cloud Veneto.

Va eseguito SULLA VM scheduler, DOPO aver montato il volume su /data
(vedi docs/SETUP_CLOUDVENETO.md). Il dataset e' grande: lancialo dentro `tmux`
cosi' se cade la connessione SSH il download continua.

Prerequisiti (una tantum, sulla VM):
  1. Volume montato:   sudo mount /dev/vdb /data
  2. Credenziali Kaggle: metti il tuo token in ~/.kaggle/kaggle.json (chmod 600).
     Lo scarichi da kaggle.com -> Settings -> API -> "Create New API Token".
     ATTENZIONE: kaggle.json e' un SEGRETO -> e' gia' git-ignored, non committarlo mai.
  3. pip install kagglehub   (dentro il pyvenv)

Uso:
  python3 scripts/download_cord19.py               # scarica in /data/kagglehub
  python3 scripts/download_cord19.py --dest /data/kagglehub
"""
import argparse
import os

DATASET = "allen-institute-for-ai/CORD-19-research-challenge"


def main():
    ap = argparse.ArgumentParser(description="Download CORD-19 from Kaggle onto the persistent volume.")
    ap.add_argument(
        "--dest",
        default=os.environ.get("KAGGLEHUB_CACHE", "/data/kagglehub"),
        help="Cartella sul VOLUME persistente dove salvare il dataset (default: /data/kagglehub).",
    )
    args = ap.parse_args()

    # CRUCIALE: manda la cache di kagglehub sul volume, non su ~/.cache (disco root da 25 GB).
    os.environ["KAGGLEHUB_CACHE"] = args.dest
    os.makedirs(args.dest, exist_ok=True)

    import kagglehub

    print(f"Scarico {DATASET} -> {args.dest}")
    print("E' un download grosso: assicurati di essere dentro tmux/nohup.")
    path = kagglehub.dataset_download(DATASET)
    print("\nFatto. File del dataset in:")
    print(path)
    print("\nPunta qui la pipeline (build_parquet.py) come cartella di input.")


if __name__ == "__main__":
    main()
