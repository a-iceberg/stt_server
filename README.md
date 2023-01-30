# Call centre transcribation service
Additional mysql and mssql databases and tables configure required.  
- Works with [Kaldi Vosk](https://hub.docker.com/r/alphacep/kaldi-vosk-server)   
- Inside docker container   
- Scalable   
- Russian language   
- [GPU](https://github.com/sskorol/vosk-api-gpu) and CPU support   
### Installation
```
git clone https://github.com/format37/call_centre_stt_server.git
cd call_centre_stt_server/docker
```
Configure your server:
```
mv docker-compose-default.yml docker-compose.yml
nano docker-compose.yml
```
Run:
```
sh compose.sh
```
Logs and performance monitoring available in [Portainer](https://www.portainer.io)
