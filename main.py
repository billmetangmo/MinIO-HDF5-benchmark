
#import utils_hdf5
import utils_minio
import os
import tempfile
from google.cloud import storage #pip install google-cloud-storage
from minio import Minio
from minio.error import ResponseError

#data = "Sasuke must die !"
filename="/home/ubuntu/labs/speed/trinet.pth"
with open(filename, 'rb') as fd:
     data = fd.read()
size = 5
threads = 50

# MinIO GCS: https://github.com/minio/minio/blob/master/docs/gateway/gcs.md
# docker run -p 9000:9000 \
#  -v /home/ubuntu/labs/speed/credentials.json:/credentials.json \
#  -e "GOOGLE_APPLICATION_CREDENTIALS=/credentials.json" \
#  -e "MINIO_ACCESS_KEY=minioadmin" \
#  -e "MINIO_SECRET_KEY=minioadmin" \
#  -e 'MINIO_CACHE="on"' \
#  -e "MINIO_CACHE_QUOTA=80" \
#  -e "MINIO_CACHE_AFTER=1" \
#  -e "MINIO_CACHE_WATERMARK_LOW=70" \
#  -e "MINIO_CACHE_WATERMARK_HIGH=90" \
#  -e "MINIO_CACHE_DRIVES=/data" \
#  minio/minio gateway gcs computingv3

if __name__ == '__main__':

    #utils_data.sequential_batch_rw(data, size)
    #sequential_random_rw(data, size)
    #utils_data.parallel_batch_rw(data,size,threads)
    #parallel_random_rw(data, size, threads)
    try:
        bucket_name = "b1000-qopius"
        profile = "minio"

        if profile =="gcs":
            # GCP Bucket
            client = storage.Client()
            client.create_bucket(bucket_name,location="europe-west1")
        elif profile == "minio":
            # MinIO Bucket
            minioClient = Minio('d23.dev.qopius.net:9000',
                                access_key='minioadmin',
                                secret_key='minioadmin',
                                secure=False)
            minioClient.make_bucket(bucket_name)

        #utils_minio.sequential_batch_rw(data, size, profile, bucket_name, filename)
        #utils_minio.sequential_random_rw(data, size, profile, bucket_name,filename)
        #utils_minio.parallel_batch_rw(data, size, profile, bucket_name,filename,threads)
        utils_minio.parallel_random_rw(data, size, profile, bucket_name, filename, threads)

    finally:

        if profile =="gcs":
            # GCS delete
            bucket = client.get_bucket(bucket_name)
            bucket.delete(force=True)
            pass
        elif profile == "minio":
            # Install mc : https://github.com/minio/mc & sudo apt-get purge mc
            # Configure mc : mc config host add minio http://104.155.76.35:9000 minioadmin minioadmin
            os.system("mc rm minio/{} -r --dangerous --force ".format(bucket_name))
            minioClient.remove_bucket(bucket_name)
            pass
