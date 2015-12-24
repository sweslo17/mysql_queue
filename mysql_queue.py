import mysql.connector



class mysql_queue:
	def __init__(self,host,user,passwd,database):
		self.connect(host,user,passwd,database)

	def connect():
		self.cnx = mysql.connector(host,user,passwd,database)
		selc.cur = cnx.cursor()

	def enqueue(self,work_type,key,message,worker):
		self.cur.execute('REPLACE INTO work_queue (type,key,message,worker,stat) VALUES (%s,%s,%s,%s,%s)',(work_type,key,message,worker,0))
		pass

	def dequeue(work_type,key,worker):
		pass

	def success(key,worker):
		pass

	def fail(key,worker):
		pass

	def recover(key,worker):
		pass

	def recover_all():
		pass
