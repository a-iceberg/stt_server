sudo mkdir /mnt/share/
sudo mkdir /mnt/share/audio_call
sudo mkdir /mnt/share/audio_master
sudo mount -t nfs 10.2.4.239:/opt/Call_TO_1C/ /mnt/share/audio_call/ # call
sudo mount -t nfs 10.2.4.254:/opt/Call_TO_1C/ /mnt/share/audio_master/ # mrm
