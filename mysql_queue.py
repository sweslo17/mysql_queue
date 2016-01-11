import mysql.connector
import time
import traceback
import logging

class mysql_queue:
	def __init__(self,host,user,passwd,database,worker):
		self.connect(host,user,passwd,database)
		self.worker = worker

	def connect(self,host,user,passwd,database):
		self.cnx = mysql.connector.connect(host=host,user=user,passwd=passwd,database=database)
		self.cur = self.cnx.cursor()

	def enqueue(self,work_type,key,request,worker):
		self.cur.execute('REPLACE INTO works (`work_type`,`key`,`request`,`worker`,`stat`) VALUES (%s,%s,%s,%s,%s)',(work_type,key,request,worker,'pending'))
		self.cnx.commit()

	def dequeue(self,work_types,worker=None,work_key=None):
		self.cur.execute('SET AUTOCOMMIT=0;')
		sql = "SELECT `id`,`key`,`request`,`work_type` FROM works WHERE stat='pending' "
		parameter = []
		if work_types is not None and work_types[0] != '*':
			sql += " AND ( "
			for work_type in work_types:
				sql += "`work_type`=%s"
				if work_type != work_types[-1]:
					sql+=" OR "
				parameter.append(work_type)
			sql += " ) "
		if worker is not None:
			sql += " AND `worker`=%s"
			parameter.append(worker)
		if work_key is not None:
			sql += " AND `key`=%s"
			parameter.append(work_key)
		sql += ' ORDER BY `id` LIMIT 1  FOR UPDATE'
		self.cur.execute(sql,parameter)
		result = self.cur.fetchone()
		if result is None:
			self.cnx.commit()
			return None
		work_key = result[1]
		request = result[2]
		work_type = result[3]
		self.cur.execute("UPDATE works SET `stat`='working',worker=%s WHERE `key`=%s",(self.worker,work_key))
		self.cnx.commit()
		return {'work_type':work_type,'work_key':work_key,'request':request}

	def delete_work(self,work_key):
		self.cur.execute('SET AUTOCOMMIT=0;')
		self.cur.execute('DELETE FROM works WHERE `key`=%s',(work_key,))
		self.cnx.commit()

	def success(self,work_key,work_type,data):
		logging.debug('JOB SUCCESS')
		self.cur.execute('SET AUTOCOMMIT=0;')
		self.cur.execute('SET NAMES utf8mb4')
		self.cur.execute('REPLACE INTO results (`key`,`work_type`,`data`) VALUES (%s,%s,%s)',(work_key,work_type,data))
		self.cur.execute('DELETE FROM works WHERE `key`=%s',(work_key,))
		self.cnx.commit()
		logging.debug('JOB SUCCESS COMMIT')

	def fail(self,work_key,error_message):
		logging.debug('JOB FAIL')
		self.cnx.rollback()
		self.cur.execute('SET AUTOCOMMIT=0;')
		self.cur.execute("UPDATE works SET `stat`='failed',`error_message`=%s WHERE `key`=%s",(error_message,work_key))
		self.cnx.commit()
		logging.debug('JOB FAIL COMMIT')

	def recover(self,work_key=None,worker=None):
		self.cur.execute('SET AUTOCOMMIT=0;')
		sql = "UPDATE works SET stat='pending',error_message=NULL WHERE stat='failed'"
		parameter = []
		if work_key is not None:
			sql += " AND `key`=%s"
			parameter.append(work_key)
		if worker is not None:
			sql += " AND `worker`=%s"
			parameter.append(worker)
		self.cur.execute(sql,parameter)
		self.cnx.commit()

	def recover_all(self):
		self.cur.execute('SET AUTOCOMMIT=0;')
		sql = "UPDATE works SET stat='pending',error_message=NULL WHERE stat='failed'"
		self.cur.execute(sql)
		self.cnx.commit()
