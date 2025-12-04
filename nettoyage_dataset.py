# wipe_prefix.py
from google.cloud import datastore
import argparse

client = datastore.Client()
BATCH_SIZE = 500  # pour éviter d'envoyer trop de suppressions d'un coup


def delete_posts(prefix: str):
    print(f"Suppression des posts author={prefix}* ...")
    query = client.query(kind="Post")
    to_delete = []
    for entity in query.fetch():
        author = entity.get("author", "")
        if str(author).startswith(prefix):
            to_delete.append(entity.key)
            if len(to_delete) >= BATCH_SIZE:
                client.delete_multi(to_delete)
                to_delete = []
    if to_delete:
        client.delete_multi(to_delete)
    print("Posts supprimés (batchés).")


def delete_users(prefix: str):
    print(f"Suppression des users {prefix}* ...")
    query = client.query(kind="User")
    to_delete = []
    for entity in query.fetch():
        name = entity.key.name
        if name and name.startswith(prefix):
            to_delete.append(entity.key)
            if len(to_delete) >= BATCH_SIZE:
                client.delete_multi(to_delete)
                to_delete = []
    if to_delete:
        client.delete_multi(to_delete)
    print("Users supprimés (batchés).")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Supprime les Users et Posts dont le prefixe correspond."
    )
    parser.add_argument(
        "--prefix",
        required=True,
        help="Préfixe des utilisateurs/posts à supprimer (ex: conc, test, posts10)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    prefix = args.prefix
    print(f"Début de la suppression pour le préfixe: {prefix!r}")
    # Important: d'abord les posts, ensuite les users
    delete_posts(prefix)
    delete_users(prefix)
    print(f"Suppression des données {prefix}* terminée.")
