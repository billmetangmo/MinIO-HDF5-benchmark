# MinIO-HDF5-benchmark
Find alternative to direct up/download to Google Cloud Storage from POSIX .


## Why ?
In a deep learning application, you may want to use a system that allows to share intermediate data between machines but also make
this data available from outside ( HTTP) easily. You can first consider using Google Cloud Storage if you are using GCP but this
does not scale well especially for small/medium files. The aim of this benchmark is to test new solution.

## Solution

### Solution 1
We consider 3 types of files:
* small/medium files : that we 
