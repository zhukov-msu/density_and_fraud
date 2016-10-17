from adparser import AdParser
import os
import sqlite3
from queue import Queue
import logging
from threading import Thread, current_thread, enumerate as enumerate_t

logging.basicConfig(format="%(asctime)s - %(levelname)s: %(message)s",
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    filename="adparser.log",
                    filemode='a',
                    level=logging.INFO
                    )

class Worker:

    def __init__(self, t_name, db_name, wd):
        self.table_name = t_name
        self.db_name = db_name
        self.q_files = Queue()
        self.q_results = Queue()
        self.parser = AdParser()
        self.w_dir = wd


    def db_conn(self):
        try:
            conn = sqlite3.connect(self.db_name, timeout=10.0)
            conn.execute('''
                    CREATE TABLE IF NOT EXISTS {0}
                    (
                    file_name VARCHAR(256),
                    density DOUBLE PRECISION,
                    fraud BOOLEAN
                    )
                    '''.format(self.table_name))
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS 'id' ON {}(file_name)".format(self.table_name))
        except Exception as e:
            raise Exception("Database connection failed: {0}", str(e))
        return conn

    def worker(self):
        try:
            while True:
                item = self.q_files.get()
                # print(item)
                if item[0] == "file":  # если прилетело имя файла

                    f = item[1]
                    data = self.parser.parse(f)  # считаем тошноту и определяем мошенничество
                    # кладем результат в очередь на отправку в БД
                    self.q_results.put(("res",(f, data[1], str(data[2]).upper())))
                    self.q_files.task_done()
                else:  # если прилетел терминирующий символ
                    self.q_files.task_done()
                    # когда остается один "живой" процесс с помощью него кладем в конец очереди терминирующий символ
                    # < 3, т.к. живой еще и мастер-процесс.
                    if len(enumerate_t()) < 3:
                        self.q_results.put(("",))
                    break
        except Exception as e:
            raise Exception("{0} - Error while file processing: {1}".format(current_thread().getName(), str(e)))

    def do_work(self, workers_cnt):
        """
        Основной процесс
        """
        threads = [Thread(target=self.worker) for _ in range(workers_cnt)] # создаем n потоков
        for t in threads:
            t.setDaemon(True)
            t.start()
        logging.info("Process started at directory '{0}'. Threads count: {1}".format(self.w_dir, workers_cnt))
        for dirpath, _, files in os.walk(self.w_dir):
            for i, f in enumerate(files):
                self.q_files.put(("file", os.path.join(dirpath, f)))
                # if i>10: break
        for i in range(workers_cnt):
            self.q_files.put(("",))

        conn = self.db_conn()
        while True:
            res = self.q_results.get()
            if res[0] == "res":
                try:
                    conn.execute("INSERT or REPLACE INTO {}(file_name,density,fraud) VALUES(?,?,?)".format(
                        self.table_name), res[1])
                except sqlite3.OperationalError as e:
                    logging.error("Insert into database failed. DB name: {0}; Tuple: {1}".format(
                        self.db_name, res[1]))
                    raise e
                except Exception as e:
                    raise e
                else:
                    # logging.info("Success: {}".format(res[1]))
                    self.q_results.task_done()
            else:
                self.q_results.task_done()
                break
        self.q_files.join()
        self.q_results.join()
        conn.commit()
        conn.close()
