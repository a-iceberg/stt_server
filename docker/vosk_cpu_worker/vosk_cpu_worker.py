import os
import socket

# master path 'audio/mono'
# call path 'audio/stereo'

def get_worker_id():

	workers_count = int(os.environ.get('WORKERS_COUNT', '0'))
	hostname = str(socket.gethostname())

	with open('id_garden/'+hostname, "w") as f:
		f.write('')

	files = []
	while len(files)<workers_count:
		for root, dirs, files in os.walk('id_garden'):
			filenames = sorted([filename for filename in files])
			break

	for i in range(0, len(filenames)):
		if filenames[i] == hostname:
			break

	return i


worker_id = get_worker_id()
print('worker_id:', worker_id)
