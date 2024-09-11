# Call centre transcribation service
Additional mysql, mssql and postgresql databases and tables configure required.  
- Works with [Kaldi Vosk](https://hub.docker.com/r/alphacep/kaldi-vosk-server)   
- Inside docker container   
- Scalable   
- Russian language   
- [GPU](https://github.com/sskorol/vosk-api-gpu) and CPU support   
### Installation
```
git clone https://github.com/a-iceberg/stt_server.git
```
You have to mount folders after each restart of the server.  
Mount folders:
```
sh mount.sh
```
Run:
```
sh compose.sh
```
Logs and performance monitoring available in [Portainer](https://www.portainer.io)
