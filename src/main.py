import os
import sys
import time
from queue import Queue
from subprocess import Popen, PIPE, run, call
from threading import Thread
import socket
import logging

sys.path.append(os.getcwd())

from src.config import config


def config_logger(log_dir):
    hostname = socket.gethostname()
    log_name = 'main-%s' % hostname
    index = os.getcwd().index('AssociationAbacMiner')
    base_path = log_dir #os.getcwd()[0:index + 20]
    if not os.path.exists('%s/%s' % (base_path, hostname)):
        os.makedirs('%s/%s' % (base_path, hostname))
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        filename='%s/%s.log' % (base_path, log_name),
                        level=logging.INFO)
    print('Logging to %s/%s.log' % (base_path, log_name))

    # logger = logging.getLogger(log_name)
    # job_log_file_hdlr = logging.FileHandler('%s/%s/%s.log' % (base_path, hostname, log_name), mode='w')
    # job_log_file_hdlr.setFormatter(formatter)
    # logger.addHandler(job_log_file_hdlr)
    # return logger

def get_log_dir():
    log_dir = '/tmp/mwsanders/%s' % socket.gethostname()
    if 'SCRATCH' in os.environ:
        log_dir = ('%s/%s' % (os.environ['SCRATCH'], socket.gethostname()))
    return log_dir

config_logger(get_log_dir())

def getabit(o,q):
    for c in iter(lambda:o.read(1),b''):
        q.put(c)
    o.close()

def getdata(q):
    r = b''
    while not q.empty():
        c = q.get(False)
        r += c
    return r


def wait_for_es(log_dir):
    attempts = 0
    sleep_time = 5
    while True:
        try:
            logging.info('Pinging ES, attempt %d' % attempts)
            res = es.ping()
            if res:
                logging.info('ES responded, resuming....')
                break
        except:
            pass
        # if config.anon == True and attempts % 10 == 0:
        #     logging.info('No response after %ds , trying to restart ES...' % (attempts * sleep_time))
        #     cmd = ['pkill', '-u', 'mwsanders', 'java']
        #     call(cmd)
        #     f = open('%s/es-restart%s.log' % (log_dir,socket.gethostname()), 'w')
        #     cmd = ['nice', '-20', '${HOME}/dev/elasticsearch-6.3.0/bin/elasticsearch']
        #     proc = Popen(cmd, stdout=f, stderr=f)
        logging.info('No response from ES, sleeping...')
        time.sleep(sleep_time)
        attempts += 1



if __name__ == "__main__":
    try:
        cwd = os.getcwd()
        logging.info('CWD: %s' % cwd)
        # sys.path.append(cwd)
        os.environ["PYTHONPATH"] = cwd
        sub_env = os.environ.copy()

        os.chdir('%s/..' % cwd)
        os.chdir('%s/src' % cwd)
        logging.info('ABAC Miner PWD: %s' % os.getcwd())

        es = config.es
        time.sleep(5)
        log_dir = get_log_dir()
        logging.info('Logging to %s' % log_dir)
        logging.info('Testing for running ES...')

        wait_for_es(log_dir)
        # logging.info('ES responseded OK, deleting previous indices')
        # es.indices.delete(index='flat-*', ignore=[400, 404])
        while True:
            try:
                wait_for_es(log_dir)
                cmd = ['python', 'job/job_executor.py']
                f = open('%s/%s.log' % (log_dir, socket.gethostname()), 'w')
                p = Popen(cmd, stdin=f, stdout=f)
                p.wait()
                q = Queue()
                t = Thread(target=p.wait())
                t.daemon = True
                t.start()
                # outfile = open('%s.log' % socket.gethostname(), "w", 1)
                while True:
                    time.sleep(5)  # to ensure that the data will be processed completely
                    if not t.isAlive():
                        break
                    else:
                        f.flush()
                p.wait()
                logging.info(p.returncode)
                if p.returncode == 17:
                    break
            except Exception as e:
                logging.error(e)
    except Exception as e:
        logging.error(e)
