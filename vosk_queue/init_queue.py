import pymysql as mysql
import psycopg
import datetime
import os
import wave
import contextlib
import re
import pandas as pd
import time
import shutil
import logging
import requests
import soundfile

from soundfile import SoundFile


class stt_server:
    def __init__(self):
        # Init self.logger with info level
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        cores_count = int(os.environ.get("WORKERS_COUNT", "0"))
        self.cpu_cores = [i for i in range(0, cores_count)]

        # postgre sql
        self.p_sql_name = "voice_ai"

        # mysql
        self.mysql_name = {
            1: "MICO_96",
            2: "asterisk",
        }

        self.source_id = 0
        self.sources = {
            "call": 1,
            "master": 2,
        }

        self.original_storage_path = {
            1: "audio/stereo/",  # call centre records path
            2: "audio/mono/",  # masters records path
        }
        self.saved_for_analysis_path = "audio/saved_for_analysis/"
        self.confidence_of_file = 0

        self.temp_file_path = ""
        self.temp_file_name = ""

        self.p_conn = self.connect_p_sql()
        self.mysql_conn = {
            1: self.connect_mysql(1),
            2: self.connect_mysql(2),
        }

    def send_to_telegram(self, message):
        try:
            current_date = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
            chat_id = os.environ.get("TELEGRAM_CHAT", "")
            session = requests.Session()
            get_request = "https://api.telegram.org/bot" + token
            get_request += "/sendMessage?chat_id=" + chat_id
            get_request += (
                "&parse_mode=Markdown&text=" + current_date + " vosk_queue: " + message
            )
            session.get(get_request)
        except Exception as e:
            self.logger.info("send_to_telegram error: " + str(e))

    def connect_p_sql(self):
        return psycopg.connect(
            dbname=self.p_sql_name,
            user=os.environ.get("POSTGRESQL_LOGIN", ""),
            password=os.environ.get("POSTGRESQL_PASSWORD", ""),
            host=os.environ.get("POSTGRESQL_SERVER", ""),
            port=os.environ.get("POSTGRESQL_PORT", "")
        )

    def connect_mysql(self, source_id):
        return mysql.connect(
            host=os.environ.get("MYSQL_SERVER", ""),
            user=os.environ.get("MYSQL_LOGIN", ""),
            passwd=os.environ.get("MYSQL_PASSWORD", ""),
            db=self.mysql_name[source_id]
        )

    def linkedid_by_filename(self, filename, date_y, date_m, date_d):
        original_filename = filename
        filename = filename.replace("rxtx-in.wav", ".wav")
        filename = filename.replace("rxtx-out.wav", ".wav")
        filename = filename.replace("in_", "")
        filename = filename.replace("out_", "")

        date_from = datetime.datetime(int(date_y), int(date_m), int(date_d))
        date_toto = date_from + datetime.timedelta(days=1)
        date_from = datetime.datetime.strptime(
            str(date_from), "%Y-%m-%d %H:%M:%S"
        ).strftime("%Y-%m-%dT%H:%M:%S")
        date_toto = datetime.datetime.strptime(
            str(date_toto), "%Y-%m-%d %H:%M:%S"
        ).strftime("%Y-%m-%dT%H:%M:%S")

        uniqueid_match = re.findall(r"\d*\.\d*", original_filename)
        if not uniqueid_match:
            uniqueid = ''
        else:
            uniqueid = uniqueid_match[0]

        mysql_conn = self.connect_mysql(self.source_id)
        with mysql_conn:
            query = f"""
            SELECT linkedid, SUBSTRING(dstchannel, 5, 4), src
            FROM PT1C_cdr_MICO
            WHERE  calldate > '{date_from}' AND calldate < '{date_toto}' AND (uniqueid = '{uniqueid}' OR recordingfile LIKE '%{filename}%')
            LIMIT 1;
            """

            cursor = mysql_conn.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                linkedid, dstchannel, src = row[0], row[1], row[2]
                return linkedid, dstchannel, src
        return "", "", ""

    def get_sql_complete_files(self):
        cursor = self.p_conn.cursor()

        sql_query = "select distinct filename from queue where"
        sql_query += " source_id='" + str(self.source_id) + "'"
        sql_query += " order by filename;"
        cursor.execute(sql_query)
        complete_files = []
        for row in cursor.fetchall():
            complete_files.append(row[0])

        return complete_files

    def copy_file(self, src, dst):
        if not os.path.exists(src):
            self.logger.info("copy_file error: source file not exist " + src)
            self.log("copy_file error: source file not exist " + src)
            return
        self.log("copying " + src + " to " + dst)
        shutil.copy(src, dst)

    def log(self, text):
        current_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        text = str(current_date) + " " + text
        self.logger.info(text)
        with open(self.saved_for_analysis_path + "debug/log.txt", "a") as f:
            f.write(text + "\n")

    def get_fs_files_list(self, queue):
        queue_set = set(queue)
        filepath = self.original_storage_path[self.source_id]

        if self.source_id == self.sources["master"]:
            files_extracted = 0
            files_withoud_cdr_data = 0

            os_walk = os.walk(filepath)
            self.logger.info(
                f"master folder {filepath} Files in folder: {len(queue_set)}"
            )
            for dirpath, dirnames, filenames in os_walk:
                for filename in sorted(filenames):
                    if not filename.endswith((".wav", ".WAV")):
                        continue
            
                    if filename in queue_set:
                        continue

                    # get record date
                    if os.environ.get("SAVE_FOR_ANALYSIS", "0") == "1":
                        dst_file = (
                            self.saved_for_analysis_path + "debug/master/" + filename
                        )
                        if not os.path.exists(dst_file):
                            self.copy_file(
                                filepath + filename,
                                self.saved_for_analysis_path + "debug/master/",
                            )
                    try:
                        file_stat = os.stat(
                            filepath + filename
                        )
                        file_age = time.time() - file_stat.st_mtime
                    except Exception as e:
                        self.logger.info(
                            "get_fs_files_list / file_stat Error: " + str(e)
                        )
                        file_age = 0
                    if "h.wav" in filename:
                        try:
                            if file_age > 3600:
                                os.remove(
                                    filepath
                                    + filename
                                )
                                self.logger.info(
                                    str(round(file_age / 60))
                                    + " min. get_fs_files_list. Removed: "
                                    + filename
                                )
                            else:
                                self.logger.info(
                                    str(round(file_age / 60))
                                    + " min. get_fs_files_list. Skipped: "
                                    + filename
                                )
                            continue
                        except (
                            OSError
                        ) as e:
                            self.logger.info(
                                "Error: %s - %s." % (e.filename, e.strerror)
                            )
                            self.send_to_telegram(
                                "get_fs_files_list file delete error:\n" + str(e)
                            )

                    rec_date = "Null"
                    version = 0
                    r_d = re.findall(r"a.*b", filename)
                    if len(r_d) and len(r_d[0]) == 21:
                        try:
                            rec_date = r_d[0][1:][:-1].replace("t", " ")
                            src = re.findall(r"c.*d", filename)[0][1:][:-1]
                            dst = re.findall(r"e.*f", filename)[0][1:][:-1]
                            linkedid = re.findall(r"g.*h", filename)[0][1:][:-1]
                            version = 1
                        except Exception as e:
                            self.logger.info("Error: " + str(e))

                    if version == 0:
                        rec_date = "Null"
                        uniqueid = re.findall(r"\d*\.\d*", filename)[0]
                        cursor = self.mysql_conn[self.source_id].cursor()
                        query = (
                            "select calldate, src, dst from cdr where uniqueid = '"
                            + uniqueid
                            + "' limit 1;"
                        )
                        cursor.execute(query)  # cycled query
                        src = ""
                        dst = ""
                        linkedid = uniqueid

                        for row in cursor.fetchall():
                            rec_date = str(row[0])
                            self.logger.info("v.0 date " + rec_date)
                            src = str(row[1])
                            dst = str(row[2])

                        if (
                            len(
                                re.findall(
                                    r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", rec_date
                                )
                            )
                            == 0
                        ):
                            self.logger.info(
                                "u: "
                                + uniqueid
                                + " r: "
                                + rec_date
                                + " Unable to extract date from filename: "
                                + filename
                            )
                            rec_date = "Null"
                            files_withoud_cdr_data += 1

                    if not rec_date == "Null":
                        file_stat = os.stat(filepath + filename)
                        f_size = file_stat.st_size
                        yield (
                            filepath,
                            filename,
                            rec_date,
                            src,
                            dst,
                            linkedid,
                            version,
                            f_size
                        )
                        files_extracted += 1

            self.logger.info(
                "master extracted: "
                + str(files_extracted)
                + " without cdr data: "
                + str(files_withoud_cdr_data)
            )

        elif self.source_id == self.sources["call"]:
            os_walk = os.walk(filepath)
            self.logger.info(
                f"call path {filepath} Files in folder: {len(queue_set)}"
            )
            for root, dirs, files in os_walk:
                for filename in sorted(files):
                    if not filename.endswith((".wav", ".WAV")):
                        continue

                    if not (filename.endswith("rxtx-in.wav") or filename.endswith("rxtx-out.wav")):
                        # log information about removed file and his path
                        with open(
                            self.saved_for_analysis_path + "debug/removed.csv", "a"
                        ) as f:
                            f.write(root + ";" + filename + "\n")
                        self.logger.info("removed " + root + "/" + filename)
                        os.remove(os.path.join(root, filename))
                        continue

                    if filename in queue_set:
                        continue

                    if os.environ.get("SAVE_FOR_ANALYSIS", "0") == "1":
                        self.log("call check file " + filename)
                        try:
                            dst_file = (
                                self.saved_for_analysis_path
                                + "debug/call/"
                                + filename
                            )
                            if not os.path.exists(dst_file):
                                self.copy_file(
                                    os.path.join(root, filename),
                                    self.saved_for_analysis_path + "debug/call/",
                                )
                            else:
                                self.log(
                                    "copying canceled. file exists: " + dst_file
                                )
                        except Exception as e:
                            self.log("call debug error: " + str(e))

                    rec_source_date = re.findall(
                        r"\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}", filename
                    )
                    if len(rec_source_date) and len(rec_source_date[0]):
                        rec_date = (
                            rec_source_date[0][:10]
                            + " "
                            + rec_source_date[0][11:].replace("-", ":")
                        )

                        if (
                            len(
                                re.findall(
                                    r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", rec_date
                                )
                            )
                            == 0
                        ):
                            rec_date = "Null"
                            self.logger.info(
                                "0 Unable to extract date: " + root + " " + filename
                            )

                        date_string = re.findall(r"\d{4}-\d{2}-\d{2}", filename)
                        if len(date_string):
                            date_y = date_string[0][:4]
                            date_m = date_string[0][5:-3]
                            date_d = date_string[0][-2:]
                            linkedid, dst, src = self.linkedid_by_filename(
                                filename, date_y, date_m, date_d
                            )  # cycled query

                            filepath = root + "/"
                            file_stat = os.stat(
                                os.path.join(root, filename)
                            )
                            f_size = file_stat.st_size
                            yield (
                                filepath,
                                filename,
                                rec_date,
                                src,
                                dst,
                                linkedid,
                                0,
                                f_size
                            )
                    else:
                        self.logger.info(
                            "1 Unable to extract date: " + root + " " + filename
                        )
                        self.send_to_telegram(
                            "1 Unable to extract date: "
                            + str(root)
                            + " "
                            + str(filename)
                        )

    def set_shortest_queue_cpu(self):
        cursor = self.p_conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS tmp_cpu_queue_len;")
        cursor.execute("""
        CREATE TEMPORARY TABLE tmp_cpu_queue_len (
            cpu_id INT,
            files_count INT
        );
        """)
        self.p_conn.commit()

        insert_query = "INSERT INTO tmp_cpu_queue_len (cpu_id, files_count) VALUES " + \
                    ", ".join(f"({i}, 0)" for i in self.cpu_cores) + ";"
        cursor.execute(insert_query)
        self.p_conn.commit()

        main_query = f"""
        DO $$
        DECLARE
            result_cpu_id INT;
        BEGIN
            UPDATE tmp_cpu_queue_len
            SET files_count = (SELECT COUNT(*) FROM queue WHERE queue.cpu_id = tmp_cpu_queue_len.cpu_id);

            SELECT cpu_id INTO result_cpu_id FROM tmp_cpu_queue_len
            ORDER BY files_count, cpu_id
            LIMIT 1;

            CREATE TEMPORARY TABLE IF NOT EXISTS result_table (cpu_id INT);
            TRUNCATE result_table;
            INSERT INTO result_table VALUES (result_cpu_id);
        END $$;
        """
        cursor.execute(main_query)
        self.p_conn.commit()
        cursor.execute("SELECT cpu_id FROM result_table;")

        rows = cursor.fetchall()
        result = 0
        for row in rows:
            result += 1
            self.cpu_id = int(row[0])

        if result == 0:
            self.logger.info("Error: unable to get shortest_queue_cpu")
            self.cpu_id = 0

        cursor.execute("DROP TABLE IF EXISTS tmp_cpu_queue_len;")
        cursor.execute("DROP TABLE IF EXISTS result_table;")
        self.p_conn.commit()

    def get_source_id(self, source_name):
        for source in self.sources.items():
            if source[0] == source_name:
                return source[1]
        return 0

    def get_source_name(self, source_id):
        for source in self.sources.items():
            if source[1] == source_id:
                return source[0]
        return 0

    def add_queue(
        self,
        filepath,
        filename,
        rec_date,
        src,
        dst,
        linkedid,
        naming_version,
        file_size
    ):
        try:
            file_stat = os.stat(filepath + filename)
            f_size = file_stat.st_size
            st_mtime = file_stat.st_mtime
        except Exception as e:
            f_size = -1
            st_mtime = 0
            self.logger.info("file stat error: " + str(e))
            self.send_to_telegram(str(e))

        if time.time() - st_mtime > 1200 and f_size == file_size and file_size > 0:
            file_duration = self.calculate_file_length(filepath, filename)

            if file_duration == 0:
                message = "zero file in queue: t[" + str(time.time() - st_mtime) + "]  "
                message += "s[" + str(f_size) + "]  "
                message += "d[" + str(file_duration) + "]  "
                message += str(filename)
                self.logger.info(message)

            cursor = self.p_conn.cursor()
            current_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            sql_query = "insert into queue "
            sql_query += "(filepath, filename, cpu_id, date, "
            sql_query += "duration, record_date, source_id, src, dst, linkedid, version) "
            sql_query += "values ('"
            sql_query += filepath + "','"
            sql_query += filename + "','"
            sql_query += str(self.cpu_id) + "','"
            sql_query += current_date + "','"
            sql_query += str(file_duration) + "',"
            sql_query += rec_date if rec_date == "Null" else "'" + rec_date + "'"
            sql_query += ",'"
            sql_query += str(self.source_id) + "','"
            sql_query += str(src) + "','"
            sql_query += str(dst) + "','"
            sql_query += str(linkedid) + "',"
            sql_query += str(naming_version) + ");"

            try:
                cursor.execute(sql_query)
                self.p_conn.commit()
            except Exception as e:
                self.logger.info("add queue error. query: " + sql_query)
                self.logger.info(str(e))

    def calculate_file_length(self, filepath, filename):
        file_duration = 0
        fname = filepath + filename
        try:
            with contextlib.closing(wave.open(fname, "rb")) as f:
                frames = f.getnframes()
                rate = f.getframerate()
                file_duration = frames / float(rate)
        except wave.Error as e:
            self.logger.info("Wave error: " + fname + " " + str(e))
            try:
                data, samplerate = soundfile.read(fname)
                soundfile.write(fname, data, samplerate)

                with SoundFile(fname, "r") as f:
                    frames = f.frames
                    rate = f.samplerate
                    file_duration = frames / float(rate)
            except Exception as e:
                self.logger.info("File length calculate error: " + fname + " " + str(e))
        return file_duration

    def clean_queue(self):
        cursor = self.p_conn.cursor()
        sql_query = "delete from queue;"
        cursor.execute(sql_query)
        self.p_conn.commit()
        self.logger.info("queue cleaned")
