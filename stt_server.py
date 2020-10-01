import os
import uuid

temp_storage_path = 'files/'
audio_path = '/mnt/share/audio_call/RXTX_2020-10/01/in_4953621465_2020-10-01-09-41-34rxtx.wav'
out_path = temp_storage_path+str(uuid.uuid4())

os.system('ffmpeg -i '+audio_path+' -ar 16000 -af "pan=mono|c0=FL" '+out_path+'_l.wav')
os.system('ffmpeg -i '+audio_path+' -ar 16000 -af "pan=mono|c0=FR" '+out_path+'_r.wav')

print(out_path)