echo 'Uncomment install section if get error: bad option; for several filesystems (e.g. nfs, cifs) you might need a /sbin/mount.<type> helper program.\n'
#sudo apt-get install cifs-utils
#sudo apt-get install nfs-common
echo 'Uncomment mkdir section if directories are not exists'
#sudo mkdir /mnt/share/
#sudo mkdir /mnt/share/audio_call
#sudo mkdir /mnt/share/audio_master
sudo mount -t nfs 10.2.4.239:/opt/Call_TO_1C/ /mnt/share/audio_call/ # call
sudo mount -t nfs 10.2.4.254:/opt/Call_TO_1C/ /mnt/share/audio_master/ # mrm
echo 'Mounting complete'
