from init_server import stt_server
import os

transcribed = []
server_object = stt_server(0)
server_object.source_id = 1
cursor = server_object.conn.cursor()
sql_query = "select distinct audio_file_name as filename, date_y, date_m, date_d"
sql_query += " from transcribations where source_id=1 order by date_y, date_m, date_d;"
cursor.execute(sql_query)
count_removed = 0
count_not_found = 0
for row in cursor.fetchall():
    full_path = server_object.original_storage_path[1]
    full_path += 'RXTX_' + row[1]
    full_path += '-' + row[2] + '/'
    full_path += row[3] + '/'
    full_path += row[0]
    if os.path.isfile(full_path):
        try:
            os.remove(full_path)
            print(row[1], row[2], row[3], 'removed:', full_path)
            count_removed += 1
        except OSError as e:  ## if failed, report it back to the user ##
            print(row[1], row[2], row[3], "error: %s - %s." % (e.filename, e.strerror))
    else:
        print(row[1], row[2], row[3], 'not found:', full_path)
        count_not_found += 1

print('job complete')
print('removed:', count_removed)
print('not found:', count_not_found)
