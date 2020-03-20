import tempfile
import time
import os
import random
import threading
import queue
import h5py
from google.cloud import storage
from minio import Minio


def write_with_profile(profile,temp_dir,i,data,gc_bucket,bucket_name,filename):
    if profile == "nfs":
        path = os.path.join(temp_dir, str(i))
        with open(path, 'wb') as fd:
            fd.write(data)
    elif profile == "gcs":
        blob = gc_bucket.blob(str(i))
        blob.upload_from_filename(filename)
    elif profile == "minio":
        gc_bucket.fput_object(bucket_name, str(i), filename)


def read_with_profile(profile,temp_dir,i,gc_bucket,bucket_name):
    if profile == "nfs":
        path = os.path.join(temp_dir, str(i))
        with open(path, 'rb') as fd:
            fd.read()
    elif profile == "gcs":
        bashCommand = "gsutil -m -D rsync -d -e -U gs://{src_path} {dst_path} ".format(
            src_path=bucket_name, dst_path=temp_dir + "/")
        os.system(bashCommand + " > /dev/null 2>&1")
    elif profile == "minio":
        # mc cp --recursive minio/booba test
        gc_bucket.get_object(bucket_name,str(i))
        #bashCommand = "mc mirror --overwrite minio/{src_path} {dst_path} ".format(
            #src_path=bucket_name, dst_path=temp_dir + "/")
        #os.system(bashCommand)



def sequential_batch_rw(data, size, profile, bucket_name,filename):
    with tempfile.TemporaryDirectory(prefix="format", suffix="-tmp") as temp_dir:

        if profile == "gcs":
            storage_client = storage.Client()
            gc_bucket = storage_client.get_bucket(bucket_name)
        elif profile == "minio":
            gc_bucket = Minio('d23.dev.qopius.net:9000',
                        access_key='minioadmin',
                        secret_key='minioadmin',
                        secure=False)


        # Time posix sequential (write)
        start = time.time()
        for i in range(0, size):
            write_with_profile(profile,temp_dir,i,data,gc_bucket,bucket_name,filename)
        end = time.time()
        print(profile+" time posix seq write =" + str(end - start) + "s")

        # Time posix sequential (read)
        start = time.time()
        for i in range(0, size):
            read_with_profile(profile,temp_dir,i,gc_bucket,bucket_name)
        end = time.time()
        print(profile+" time posix seq read =" + str(end - start) + "s")

   


def sequential_random_rw(data, size,profile, bucket_name,filename):
    with tempfile.TemporaryDirectory(prefix="format", suffix="-tmp") as temp_dir:

        if profile == "gcs":
            storage_client = storage.Client()
            gc_bucket = storage_client.get_bucket(bucket_name)

            # Prepare for data read and write(update precedent data)
            for i in range(0, size):
                blob = gc_bucket.blob(str(i))
                blob.upload_from_filename(filename)
        elif profile == "minio":
            gc_bucket = Minio('d23.dev.qopius.net:9000',
                              access_key='minioadmin',
                              secret_key='minioadmin',
                              secure=False)

            # Prepare for data read and write(update precedent data)
            for i in range(0, size):
                gc_bucket.fput_object(bucket_name, str(i), filename)

        start = time.time()
        for i in range(0, size):
            choice = random.randint(1, 2)
            #path = os.path.join(temp_dir, str(i))
            if choice == 1:
                read_with_profile(profile,temp_dir,i,gc_bucket,bucket_name)
            else:
                write_with_profile(profile,temp_dir,i,data,gc_bucket,bucket_name,filename)
        end = time.time()
        print("time posix seq read/write =" + str(end - start) + "s")


def parallel_batch_rw(data,size,profile,bucket_name,filename,threads):

    with tempfile.TemporaryDirectory(prefix="format", suffix="-tmp") as temp_dir:

        if profile == "gcs":
            storage_client = storage.Client()
            gc_bucket = storage_client.get_bucket(bucket_name)
        elif profile == "minio":
            gc_bucket = Minio('d23.dev.qopius.net:9000',
                              access_key='minioadmin',
                              secret_key='minioadmin',
                              secure=False)

        def write_file_from_q_seq():
            while True:
                index = q.get()
                write_with_profile(profile,temp_dir,index,data,gc_bucket,bucket_name,filename)
                q.task_done()

        def read_file_from_q_seq():
            while True:
                index = q.get()
                read_with_profile(profile,temp_dir,index,gc_bucket,bucket_name)
                q.task_done()

        # Time posix parallel (write)
        start = time.time()
        q = queue.Queue()
        for i in range(0, threads):
            thread = threading.Thread(target=write_file_from_q_seq)
            thread.setDaemon(True)
            thread.start()

        for i in range(0, size):
            q.put(i)

        q.join()
        end = time.time()
        print("time posix parallel write =" + str(end - start) + "s")

        # Time posix parallel (read)
        start = time.time()
        q = queue.Queue()
        for i in range(0, threads):
            thread = threading.Thread(target=read_file_from_q_seq)
            thread.setDaemon(True)
            thread.start()

        for i in range(0, size):
            q.put(i)

        q.join()
        end = time.time()
        print("time posix parallel read =" + str(end - start) + "s")


def parallel_random_rw(data, size,profile,bucket_name,filename, threads):
    with tempfile.TemporaryDirectory(prefix="format", suffix="-tmp") as temp_dir:

        if profile == "gcs":
            storage_client = storage.Client()
            gc_bucket = storage_client.get_bucket(bucket_name)
            # Prepare for data read and write(update precedent data)
            for i in range(0, size):
                blob = gc_bucket.blob(str(i))
                blob.upload_from_filename(filename)
        elif profile == "minio":
            gc_bucket = Minio('d23.dev.qopius.net:9000',
                              access_key='minioadmin',
                              secret_key='minioadmin',
                              secure=False)
            # Prepare for data read and write(update precedent data)
            for i in range(0, size):
                gc_bucket.fput_object(bucket_name, str(i), filename)

        def random_rw_file(index):
            choice = random.randint(1, 2)
            if choice == 1:
                write_with_profile(profile,temp_dir,index,data,gc_bucket,bucket_name,filename)
            else:
                read_with_profile(profile,temp_dir,index,gc_bucket,bucket_name)

        def random_rw_file_from_q_seq():
            while True:
                index = q.get()
                #path = os.path.join(temp_dir, str(index))
                random_rw_file(index)
                q.task_done()

        # Time posix parallel (read)
        start = time.time()
        q = queue.Queue()
        for i in range(0, threads):
            thread = threading.Thread(target=random_rw_file_from_q_seq)
            thread.setDaemon(True)
            thread.start()

        for i in range(0, size):
            q.put(i)

        q.join()
        end = time.time()
        print("time posix parallel read/write =" + str(end - start) + "s")
