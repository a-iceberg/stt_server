import time
import os

i=0
for root, dirs, files in os.walk('audio/mono'):
	for filename in files:
		print(root, filename)
		if i>10:
			break
print('files')
#print('mssql')
#print('mysql')
#while True:
time.sleep(3)
print('normal exit')