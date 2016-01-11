#!/usr/bin/env python
#coding=utf-8
import sys
import requests
import time
from mysql_queue import mysql_queue
import random
import logging
import traceback
import socket
from retrying import retry
import config
import validator_config


reload(sys)  # Reload does the trick!
sys.setdefaultencoding('UTF8')


@retry(wait_random_min=25000, wait_random_max=35000, stop_max_attempt_number=10)
def get_data(url):
	result = requests.get(url)
	if validator[work_type](result) is not True:
		raise Exception('RETRY',result.json())
	return result


q = mysql_queue(config.db_host,config.db_user,config.db_passwd,config.db_database,socket.gethostname())

while True:
	work = q.dequeue(['*'])
	if work is None:
		logging.info('WAITING...')
		time.sleep(30)
		continue
	reload(validator_config)
	validator = validator_config.validator
	url = work['request']
	work_key = work['work_key']
	work_type = work['work_type']
	logging.basicConfig(level=logging.DEBUG,
			format='%(asctime)s %(levelname)-8s %(message)s',
			datefmt='%a, %d %b %Y %H:%M:%S')
	logging.info("%s,%s",work_type,work_key)
	try:
		result = get_data(url)
		q.success(work_key,work_type,result.text.encode('utf-8'))
	except:
		q.fail(work_key, str(sys.exc_info()[0])+'\n'+traceback.format_exc())
	time.sleep(random.randint(7,10))
