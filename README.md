### call centre speech to text server

#### Installation
inside repo   
```
cd ../
python3 -m pip install vosk   
git clone https://github.com/alphacep/vosk-api.git   
cd vosk-api/python/   
python3 setup.py install --user --single-version-externally-managed --root=/   
python3 -m pip install vosk
cd call_centre_stt_server
wget https://alphacephei.com/vosk/models/vosk-model-ru-0.10.zip   
unzip vosk-model-ru-0.10.zip   
mv vosk-model-ru-0.10 model  
pip3 install -r requirements.txt
```  
additional libraries:   
```
python3 -m pip install pymssql
```

#### Run

#### More info on   
https://alphacephei.com/vosk/install   
#### Models   
https://alphacephei.com/vosk/models
