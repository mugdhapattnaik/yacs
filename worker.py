#!/usr/bin/python3

import json
import socket
import time
import sys
import threading

from logger import workerLogger

lock1 = threading.Lock()

class Worker:

	class Task:
		
		#task initialization
		def __init__(self, job_id, task_id, duration, start_time):
			self.job_id = job_id
			self.task_id = task_id
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
		worker_socket.bind(('localhost', self.port)) #localhost, assigned port from config file
		worker_socket.listen(10)	#backlog upto 10 requests
		print("Waiting for tasks...")
		while True:
			#accept connection and message
			task_conn, addr = worker_socket.accept()
			t = task_conn.recv(8192).decode()
			
			#extract task information from json
			task_info = json.loads(t)
			job_id = task_info["job_id"]
			task_id = task_info["task_id"]
			task_duration = task_info["duration"]
			
			print("Task received at Worker ", self.id, task_id)
				
			#create task object
			task = self.Task(job_id, task_id, task_duration, time.time())		
			
			lock1.acquire()	#acquire lock before handling shared variable
			self.execution_pool.append(task)	#add task to executing pool
			lock1.release() #release lock
			task_conn.close()
		
	#thread which executes alloted tasks
	def execute_tasks(self):
		while True:
			lock1.acquire()	#acquire lock before handling shared variable
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
						self.w.workerTimer(task.job_id, task.task_id, task.start_time, now, self.id)
			lock1.release()
			time.sleep(1)	#check task completion every second

	def send_update(self, task):
		finished_task = {"worker_id": self.id, "task_id": task.task_id}
		#log completed task info on terminal
		print("Completed task: ",finished_task)	

		#TCP/IP socket
		updates_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		updates_port = 5001

		#connect to master's update socket
		updates_socket.connect(('localhost', updates_port))
		message = json.dumps(finished_task).encode()	#encode task information
		updates_socket.send(message)	#send update message
		print("Update sent to master")
		updates_socket.close()	#close connection

if __name__ == '__main__':

	port = int(sys.argv[1])			#port number assigned
	worker_id = int(sys.argv[2])	#and worker id

	worker = Worker(worker_id, port)#create worker object
    
	#execute threads
	listen_tasks_thread = threading.Thread(target = worker.listen_tasks)
	execute_tasks_thread = threading.Thread(target = worker.execute_tasks)
	
	print("Connected to Master")	#print log
	#start threads
	listen_tasks_thread.start()
	execute_tasks_thread.start()
	
	#wait for both threads to terminate
	listen_tasks_thread.join()
	execute_tasks_thread.join()
