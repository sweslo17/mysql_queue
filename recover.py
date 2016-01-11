from mysql_queue import mysql_queue
q = mysql_queue('10.0.1.5','rogerlo','roge2@iii','work_queue','cent-01')

q.recover_all()
