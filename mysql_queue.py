import mysql.connector
import time


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
		pass

	def dequeue(self,work_type,worker=None,key=None):
		self.cur.execute('SET AUTOCOMMIT=0;')
		self.cnx.commit()
		sql = "SELECT `id`,`key`,`request` FROM works WHERE stat='pending' "
		parameter = []
		if work_type is not None:
			sql += " AND `work_type`=%s"
			parameter.append(work_type)
		if worker is not None:
			sql += " AND `worker`=%s"
			parameter.append(worker)
		if key is not None:
			sql += " AND `key`=%s"
			parameter.append(key)
		sql += ' ORDER BY `id` LIMIT 1  FOR UPDATE'
		self.cur.execute(sql,parameter)
		result = self.cur.fetchone()
		if result is None:
			return None
		work_key = result[1]
		request = result[2]
		self.cur.execute('UPDATE works SET `stat`=%s WHERE `key`=%s',(self.worker,work_key))
		self.cnx.commit()
		return {'work_key':work_key,'request':request}


	def success(key,worker):
		pass

	def fail(key,worker):
		pass

	def recover(key,worker):
		pass

	def recover_all():
		pass
