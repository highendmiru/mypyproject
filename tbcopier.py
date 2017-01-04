# -*- coding: utf-8 -*-

import cx_Oracle
import datetime
import sys

print("--------------------------------------------")
print "Start time : %s"%(datetime.datetime.now())
print("--------------------------------------------")
# ID/PW, Server IP, 접속포트, SID 묻기

source_user_id = raw_input("Source server USER ID : ")
source_user_pw = raw_input("Source server USER PASSWORD : ")
source_srv_ip = raw_input("Source server IP : ")
source_srv_port = raw_input("Source server port : ")
source_srv_sid = raw_input("Source server SID : ")

print("--------------------------------------------")

target_user_id = raw_input("Target server USER ID : ")
target_user_pw = raw_input("Target server USER PASSWORD : ")
target_srv_ip = raw_input("Target server IP : ")
target_srv_port = raw_input("Target server port : ")
target_srv_sid = raw_input("Target server SID : ")

print("--------------------------------------------")

sync_table = raw_input("table name : ") or "jism"
sync_key = raw_input("key :") or "bat"

# 입력된 정보를 이용하여 접속 실시

connstr = '%s/%s@%s:%s/%s'%(source_user_id,source_user_pw,source_srv_ip,source_srv_port,source_srv_sid)
conn = cx_Oracle.connect(connstr)
curs = conn.cursor()

connstr2 = '%s/%s@%s:%s/%s'%(target_user_id,target_user_pw,target_srv_ip,target_srv_port,target_srv_sid)
conn2 = cx_Oracle.connect(connstr2)
curs2 = conn2.cursor()

curs.execute('select * from %s'%sync_table)
data = curs.fetchone()

# 계속할까요?

proceed = raw_input("Proceed ? (Y or N)")
if(proceed == "n"):
	sys.exit("Stop")
else:
	print("proceed!")

# main process

while data is not None:
	curs2.execute("""select * from %s where %s = '%s'"""%(sync_table,sync_key,data[0]))
	data2 = curs2.fetchone()
	if not data2 :
		datas = ",".join("'{0}'".format(str(v)) for v in data)
		datas2 = datas.replace("None","")
		print datas2
		curs2.execute("insert into %s values(%s)"%(sync_table,datas2))
		conn2.commit()

	data = curs.fetchone()
conn.close()

print "End time : %s"%(datetime.datetime.now())
