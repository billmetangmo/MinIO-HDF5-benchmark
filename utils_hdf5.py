import tempfile
import time
import os
import random
import threading
import Queue
import h5py

def sequential_batch_rw(data, size):
    with tempfile.TemporaryDirectory(prefix="format", suffix="-tmp") as temp_dir:

        # Time posix sequential (write)
        start = time.time()
        for i in range(0, size):
            path = os.path.join(temp_dir, str(i))
            with open(path, 'wb') as fd:
                fd.write(data)
        end = time.time()
        print("time posix seq write =" + str(end - start) + "s")

        # Time posix sequential (read)
        start = time.time()
        for i in range(0, size):
            path = os.path.join(temp_dir, str(i))
            with open(path, 'rb') as fd:
                fd.read()
        end = time.time()
        print("time posix seq read =" + str(end - start) + "s")

    with tempfile.TemporaryDirectory(prefix="format", suffix="-tmp") as temp_dir:

        f = h5py.File(os.path.join(temp_dir, 'mydataset.hdf5'), 'a')
        grp = f.create_group("tmp")

        # Time hdf5 sequential (write)
        start = time.time()
        for i in range(0, size):
            grp.create_dataset(str(i), data=data)
        end = time.time()
        print("time hdf5 seq write =" + str(end - start) + "s")

        # Time hdf5 sequential (read)
        start = time.time()
        for i in range(0, size):
            data = grp[str(i)]
        end = time.time()
        print("time hdf5 seq read =" + str(end - start) + "s")


def sequential_random_rw(data, size):
    with tempfile.TemporaryDirectory(prefix="format", suffix="-tmp") as temp_dir:

        # Prepare for data read and write(update precedent data)
        for i in range(0, size):
            path = os.path.join(temp_dir, str(i))
            with open(path, 'wb') as fd:
                fd.write(data)

        start = time.time()
        for i in range(0, size):
            choice = random.randint(1, 2)
            path = os.path.join(temp_dir, str(i))
            if choice == 1:
                with open(path, 'rb') as fd:
                    fd.read()
            else:
                with open(path, 'wb') as fd:
                    fd.write(data)
        end = time.time()
        print("time posix seq read/write =" + str(end - start) + "s")

    with tempfile.TemporaryDirectory(prefix="format", suffix="-tmp") as temp_dir:

        # Prepare for data read and write(update precedent data)
        f = h5py.File(os.path.join(temp_dir, 'mydataset.hdf5'), 'a')
        grp = f.create_group("tmp")
        for i in range(0, size):
            grp.create_dataset(str(i), data=data)

        # Time hdf5 sequential (read)
        start = time.time()
        for i in range(0, size):
            choice = random.randint(1, 2)
            if choice == 1:
                data = grp[str(i)]
            else:
                grp.create_dataset("k" + str(i), data=data)
        end = time.time()
        print("time hdf5 seq read/write =" + str(end - start) + "s")


def parallel_batch_rw(data, size, threads):
    with tempfile.TemporaryDirectory(prefix="format", suffix="-tmp") as temp_dir:

        f = h5py.File(os.path.join(temp_dir, 'mydataset.hdf5'), 'a')
        grp = f.create_group("tmp")

        def write_file_from_q():
            while True:
                index = q.get()
                grp.create_dataset(str(index), data=data)
                q.task_done()

        def read_file_from_q():
            while True:
                index = q.get()
                data = grp[str(index)]
                q.task_done()

        # Time hdf5 parallel (write)
        start = time.time()
        q = queue.Queue()
        for i in range(0, threads):
            thread = threading.Thread(target=write_file_from_q)
            thread.setDaemon(True)
            thread.start()

        for i in range(0, size):
            q.put(i)

        q.join()
        end = time.time()
        print("time hdf5 parallel write =" + str(end - start) + "s")

        # Time hdf5 parallel (read)
        start = time.time()
        q = queue.Queue()
        for i in range(0, threads):
            thread = threading.Thread(target=read_file_from_q)
            thread.setDaemon(True)
            thread.start()

        for i in range(0, size):
            q.put(i)

        q.join()
        end = time.time()
        print("time hdf5 parallel read =" + str(end - start) + "s")

    with tempfile.TemporaryDirectory(prefix="format", suffix="-tmp") as temp_dir:

        def write_file(path):
            with open(path, 'wb') as fd:
                fd.write(data)

        def read_file(path):
            with open(path, 'rb') as fd:
                fd.read()

        def write_file_from_q_seq():
            while True:
                index = q.get()
                path = os.path.join(temp_dir, str(index))
                write_file(path)
                q.task_done()

        def read_file_from_q_seq():
            while True:
                index = q.get()
                path = os.path.join(temp_dir, str(index))
                read_file(path)
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


def parallel_random_rw(data, size, threads):
    with tempfile.TemporaryDirectory(prefix="format", suffix="-tmp") as temp_dir:

        def random_rw_file(path):
            choice = random.randint(1, 2)
            if choice == 1:
                with open(path, 'wb') as fd:
                    fd.write(data)
            else:
                with open(path, 'rb') as fd:
                    fd.read()

        def random_rw_file_from_q_seq():
            while True:
                index = q.get()
                path = os.path.join(temp_dir, str(index))
                random_rw_file(path)
                q.task_done()


        # Prepare for data read and write(update precedent data)
        for i in range(0, size):
            path = os.path.join(temp_dir, str(i))
            with open(path, 'wb') as fd:
                fd.write(data)

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

    with tempfile.TemporaryDirectory(prefix="format", suffix="-tmp") as temp_dir:

        # Prepare for data read and write(update precedent data)
        f = h5py.File(os.path.join(temp_dir, 'mydataset.hdf5'), 'a')
        grp = f.create_group("tmp")
        for i in range(0, size):
            grp.create_dataset(str(i),data=data)

        def random_rw_file_from_q():
            while True:
                index = q.get()
                choice = random.randint(1, 2)
                if choice == 1:
                    data2 = grp[str(index)]
                else:
                    grp.create_dataset("k"+str(index),data=data)
                q.task_done()

        # Time hdf5 parallel (write)
        start = time.time()
        q = queue.Queue()
        for i in range(0, threads):
            thread = threading.Thread(target=random_rw_file_from_q)
            thread.setDaemon(True)
            thread.start()

        for i in range(0, size):
            q.put(i)

        q.join()
        end = time.time()
        print("time hdf5 parallel read/write =" + str(end - start) + "s")