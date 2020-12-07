#!/usr/bin/python3

import json
import socket
import time
import sys
import threading
import datetime
import os

lock = threading.Lock()

#logging in worker
class workerLogger():
	
	#initialize log file name
	def __init__(self, worker_id):
		self.logmsg = ''
		self.fname = f'logs/w{worker_id}.log'
		self.worker_id = str(worker_id)
		try:
			os.makedirs("logs")
		except:
			pass
		
	#worker's header log
	def initLog(self):
		self.logmsg = '$$$ \tdate: ' + str(datetime.datetime.now()) + "\t$$$\n\n"
		self.logmsg += 'job_id\ttask_id\tstart_time\t\tend_time\tworker_id\n'
		
		
		with open(self.fname, 'w') as f:	#write log into file
			f.write(self.logmsg)
		self.logmsg = ''	#reset log string	

	#logs start and end time of task
	def workerTimer(self, job_id, task_id, st_time, ed_time, worker_id):
		logmsg = str(job_id) + "\t" + str(task_id) + "\t" + str(st_time) + "\t" + str(ed_time) + "\t" + self.worker_id + "\n"
			
		with open(self.fname, 'a') as f:	#write log into file
		    f.write(logmsg)
		    

class Worker:

	class Task:
		
		#task initialization
		def __init__(self, job_id, task_id, duration, start_time):
			self.job_id = job_id
			self.id = task_id
			self.duration = duration
			self.start_time = start_time
			self.elapsed_time = 0

	#worker id and port initialization		
	def __init__(self, worker_id, port):
		self.id = worker_id
		self.port = port
		self.execution_pool = []	#maintains list of running tasks
		
		self.w = workerLogger(worker_id)	#initialize loggers
		self.w.initLog()
	
	#thread which listens to task requests from master
	def listen_tasks(self):

		#TCP/IP socket
		worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		#Setting address to be reusable to bypass timeout
		worker_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		worker_socket.bind(('localhost', self.port)) #localhost, assigned port from config file
		worker_socket.listen(1)	#listen to 1 master
		
		while True:
			print("Worker", self.id, "waiting for tasks...")

			#accept connection and message
			task_conn, addr = worker_socket.accept()
			t = task_conn.recv(2048).decode()
			
			#extract task information from json
			task_info = json.loads(t)
			job_id = task_info["job_id"]
			task_id = task_info["task_id"]
			task_duration = task_info["duration"]
			
			print("Task", task_id, "received by Worker", self.id)
				
			#create task object
			task = self.Task(job_id, task_id, task_duration, time.time())		
			
			lock.acquire()	#acquire lock before handling shared variable
			self.execution_pool.append(task)	#add task to execution pool
			lock.release() #release lock
			task_conn.close()
		
	#thread which executes alloted tasks
	def execute_tasks(self):

		while True:
			lock.acquire()	#acquire lock before handling shared variable
			#if no tasks are alloted, then wait
			if(len(self.execution_pool) == 0):
				pass
			else:
				for i, task in enumerate(self.execution_pool):
					now = time.time()
					task.elapsed_time = now - task.start_time
	
					#if the elapsed time is greater than the duration of the task
					if(task.elapsed_time >= task.duration):
						self.execution_pool.pop(i)	#remove task from exec pool
						self.send_update(task)	#update master about task completion

						#log task start time and completion time
						self.w.workerTimer(task.job_id, task.id, task.start_time, now, self.id)
			lock.release()

#			time.sleep(1)	#wait until the next clock second

	def send_update(self, task):

		#log completed task info on terminal
		print("Worker", self.id, "completed Task", task.id)	

		#TCP/IP socket
		updates_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		updates_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		updates_port = 5001

		#connect to master's update socket
		updates_socket.connect(('localhost', updates_port))

		finished_task = {"worker_id": self.id, "task_id": task.id}
		message = json.dumps(finished_task).encode()	#encode task information
		updates_socket.send(message)	#send update message

		print("Update sent to master")
		updates_socket.close()	#close connection

if __name__ == '__main__':

	port = int(sys.argv[1])			#port number assigned
	worker_id = int(sys.argv[2])	#and worker id

	worker = Worker(worker_id, port)	#create worker object
    
	#create listen and execute threads
	listen_tasks_thread = threading.Thread(target = worker.listen_tasks)
	execute_tasks_thread = threading.Thread(target = worker.execute_tasks)
	
	#start threads
	listen_tasks_thread.start()
	execute_tasks_thread.start()
	
	#wait for both threads to terminate
	listen_tasks_thread.join()
	execute_tasks_thread.join()
