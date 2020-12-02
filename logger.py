#logging.py

import multiprocessing as mp
import threading as t
import time
import datetime
#from queue import Queue as Q


#logging in master.py
class masterLogger():
	fname = 'logs/master_log.txt'
	
	def __init__(self):
		self.logmsg = ''
		
	def initLog(self, sch_alg, wid, wrk):
		self.logmsg = '$$$ \tdate: ' + str(datetime.datetime.now()) + f" {sch_alg.__name__} $$$\n\n"
		for ele in wid:
			self.logmsg += 'Worker_id: ' + str(ele) + '\tTotal Slots: ' + str(wrk[ele].total_slots) + '\tPort number: ' + str(wrk[ele].port) + '\n'
		self.logmsg+='\n'
		with open(masterLogger.fname, 'w') as f:
			f.write(self.logmsg)
		self.logmsg = ''

	def logtime(self, job_id):
		self.logmsg = str(job_id) + " " + str(time.time()) + "\n"
		with open(masterLogger.fname, 'a') as f:
			f.write(self.logmsg)
		self.logmsg = ''
		
	def prLog(self, worker_id, workers, now):
		for i in range(len(worker_id)):
			#initial condition			
			idx = worker_id[i]
			worker = workers[idx]
			self.logmsg += str(worker.id) + ":" + str(worker.active_slots)
			self.logmsg += ' '
		self.logmsg+= f'\t\t{now}\n'
		with open(masterLogger.fname, 'a') as f:
			f.write(self.logmsg)
		self.logmsg = ''	


#logging in worker.py
class workerLogger():
	
	def __init__(self, worker_id):
		self.logmsg = ''
		self.fname = f'logs/w{worker_id}_log.txt'
		self.worker_id = str(worker_id)
		
	def initLog(self):
		self.logmsg = '$$$ \tdate: ' + str(datetime.datetime.now()) + "\t$$$\n\n"
		self.logmsg += 'job_id\ttask_id\tstart_time\t\tend_time\tworker_id\n'
		with open(self.fname, 'w') as f:
			f.write(self.logmsg)
		self.logmsg = ''

	def workerTimer(self, job_id, task_id, st_time, ed_time, worker_id):
		logmsg = str(job_id) + "\t" + str(task_id) + "\t" + str(st_time) + "\t" + str(ed_time) + "\t" + self.worker_id + "\n"
		with open(self.fname, 'a') as f:
		    f.write(logmsg)
	    

