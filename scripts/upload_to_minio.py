import os
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

load_dotenv()


def ingest_to_minio(file_path, bucket_name, object_name):
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9002")
    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")
    
    clean_endpoint = endpoint.replace("http://", "").replace("https://", "").strip("/")

    client = Minio(
        clean_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )

    try:
        # 1. Vérification/Création du bucket
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"✅ Bucket '{bucket_name}' créé.")

        if not os.path.exists(file_path):
            print(f"❌ Erreur : Le fichier {file_path} est introuvable.")
            return

        client.fput_object(
            bucket_name, object_name, file_path, content_type="application/json"
        )
        print(f"🚀 Succès : '{file_path}' -> '{bucket_name}/{object_name}'")

    except S3Error as exc:
        print(f"❌ Erreur S3 : {exc}")
    except Exception as e:
        print(f"❌ Erreur inattendue : {e}")


if __name__ == "__main__":
    ingest_to_minio(
        file_path="data/demandes_services_publics.json",
        bucket_name=os.getenv("MINIO_BUCKET_RAW", "raw"),
        object_name="demandes_togo_raw.json",
    )
