# IOIntegrity
Python docker container that can be used to perform IO tests. Uses FIO as backend tool.

# About the App

The IOIntegrity application is a python based wrapper tool that uses fio to perform read/writes in a infinite loop on a given file path.

## Init Container:
1. If there is an ENV variable called "INIT" - the application will assume it is a init container(for kubernetes)
2. It will check if there is a WAL file and last_written_pattern file - if yes - it will assume last time pod crashed during write - it will rewrite the last_written_pattern and exit
3. If there is only a last_written_pattern(and no WAL) - it will perform a verification of the file based on last written pattern


## Primary Container:
1. Start a infinite loop
2. Based on the parameter `io_write_step` step count :
- it will either just do verify op of `last_written_pattern` found from file `/mnt/data/last_written_pattern` 
- else it will perform a write from one of the pattern given in config option `io_patterns` list (app cycles through list of patterns

## Write OP:
1. For each write op - app will create a WAL file with intended pattern to write 
2. Perform the write on given file (controlled by `io_file`) for given size(`io_size`)
3. Remove the WAL and update `last_written_pattern`

## Verify OP:
1. Perform a verify of the file written by write op based on `last_written_pattern`


## Files Touched:
1. The primary file being written to (controlled by `io_file`)
2. The config controlling the write/verify loop 
3. The `WAL` file on same path as where the file is written (/mnt/data) which contains the pattern that fio will be writing
4. `last_written_pattern` which contains last written pattern


# Config Options :

Config options are controlled by config.json that should be mounted on `/mnt/config/config.json`

| Config        | Desc           | Default  |
| ------------- |:-------------:| -----:|
|io_mode    | perform read/write or just read | write |
| io_patterns      | list of hex io patterns  passed to fio verify_pattern flag      |   ["0x0123456789abcdef"] |
| io_write_step | How many times should a write be performed compared to read. With value 4 - every 4th time a re-write is performed      |    2|
| iteration_sleep | sleep in seconds between each iteration     |    1|
| io_file_size | size of file to write     |    5Gi|
| io_file_path | file path of where file is written/verified     |   /mnt/data/file1|





# Building the Image:
```
docker build .
```




# Deploying the image in Kubernetes Cluster:

You can use the sample_deployment.yaml as reference. We need to have three components:
1. The configmap referring to the IO Integrity Config
2. The PVC Request
3. The deployment yaml referring to the above two mounts



