# Call centre transcribation service
Additional mysql, mssql and postgresql databases and tables configure required.  
### Architecture
- `queue` watches the mounted record folders and publishes every finished recording as a task
- `redis` keeps the task queue
- `worker_5090` and `worker_1080` are worker pools, one per recognition backend; the pool size limits how many requests a backend receives at once
- `cleaner` drops outdated rows from the database
- `flower` serves the queue monitoring UI
### Installation
```
git clone https://github.com/a-iceberg/stt_server.git
```
Copy `docker-compose-default.yml` to `docker-compose.yml`, then fill in the database credentials, the mount paths and the recognition backend URLs.  
You have to mount folders after each restart of the server.  
Mount folders:
```
sh mount.sh
```
Run:
```
sh compose.sh
```
### Latency tuning
`FILE_STABLE_SECONDS` and `ENQUEUE_LOOKBEHIND_SECONDS` define how long a recording has to rest before it is queued, and together they set the delay between the end of a call and its transcription.
### Monitoring
Queue depth, task duration and failures: `http://<server>:5555`, credentials are taken from `FLOWER_BASIC_AUTH`.  
Logs and performance monitoring available in [Portainer](https://www.portainer.io)
### Queue maintenance
Recordings that failed every retry are kept in the folder mounted as `FAILED_FILES_PATH` instead of being deleted.  
Drop all pending tasks and clear the queue table:
```
sudo docker-compose -p call_centre_stt_server run --rm queue clean.py
```
