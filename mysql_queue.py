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

	def dequeue(self,work_type,worker=None,work_key=None):
		self.cur.execute('SET AUTOCOMMIT=0;')
		sql = "SELECT `id`,`key`,`request` FROM works WHERE stat='pending' "
		parameter = []
		if work_type is not None:
			sql += " AND `work_type`=%s"
			parameter.append(work_type)
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
			return None
		work_key = result[1]
		request = result[2]
		self.cur.execute("UPDATE works SET `stat`='working',worker=%s WHERE `key`=%s",(self.worker,work_key))
		self.cnx.commit()
		return {'work_key':work_key,'request':request}


	def success(self,work_key,data):
		self.cur.execute('SET AUTOCOMMIT=0;')
		self.cur.execute('REPLACE INTO results (`key`,`data`) VALUES (%s,%s)',(work_key,data))
		self.cur.execute('DELETE FROM works WHERE `key`=%s',(work_key,))
		self.cnx.commit()

	def fail(self,work_key,error_message):
		self.cur.execute('SET AUTOCOMMIT=0;')
		self.cur.execute("UPDATE works SET `stat`='failed',`error_message`=%s WHERE `key`=%s",(error_message,work_key))
		self.cnx.commit()

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
