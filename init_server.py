import pymssql

class server_settings:

	def __init__(self):

		self.script_path		= '/home/alex/projects/call_centre_stt_server/'
		self.audio_storage_path	= '/mnt/share/audio_call/'
		self.audio_path_prefix	= 'RXTX_'
		self.db_name			= 'voice_ai'
		self.db_server			= '10.2.4.124'
		self.db_login			= 'ICECORP\\1c_sql'
		self.cpu_cores			= [i for i in range(0,3)]

		#store pass in file, to prevent pass publication on git
		with open(self.script_path+'sql.pass','r') as file:
			self.db_pass		= file.read().replace('\n', '')
			file.close()
			
def connect_sql(settings):
	
	return pymssql.connect(
		server		= settings.db_server, 
		user		= settings.db_login, 
		password	= settings.db_pass,
		database	= settings.db_name
	)