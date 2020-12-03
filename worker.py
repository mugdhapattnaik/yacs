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
		
		def __init__(self, job_id, task_id, duration, start_time):
			self.job_id = job_id
			self.task_id = task_id
			self.duration = duration
			self.start_time = start_time
			self.elapsed_time = 0
			
	def __init__(self, worker_id, port):
		self.id = worker_id
		self.port = port
		self.execution_pool = []
		#initialize loggers
		self.w = workerLogger(worker_id)
		self.w.initLog()
	
	def listen_tasks(self):
		
		worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		worker_socket.bind(('localhost', self.port))
		worker_socket.listen(10)
		print("Waiting for tasks...")
		while True:
		
			task_conn, addr = worker_socket.accept()

			t = task_conn.recv(8192).decode()
			
			
			task_info = json.loads(t)
			job_id = task_info["job_id"]
			task_id = task_info["task_id"]
			task_duration = task_info["duration"]
			
			print("Task received at Worker ", self.id, task_id)
				
			task = self.Task(job_id, task_id, task_duration, time.time())

			#print("in", task.task_id)			
			
			lock1.acquire()			
			self.execution_pool.append(task)
			lock1.release()
			task_conn.close()
		
	def execute_tasks(self):
		while True:
			lock1.acquire()
			if(len(self.execution_pool) == 0):
				pass
			else:
				for i, task in enumerate(self.execution_pool):
					now = time.time()
					task.elapsed_time += now - task.start_time
					if(task.elapsed_time >= task.duration):
						self.execution_pool.pop(i)
						self.send_update(task)
						self.w.workerTimer(task.job_id, task.task_id, task.start_time, now, self.id)
			lock1.release()
			time.sleep(1)

	def send_update(self, task):
		finished_task = {"worker_id": self.id, "task_id": task.task_id}
		print("Completed task: ",finished_task)
		updates_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		updates_port = 5001
		updates_socket.connect(('localhost', updates_port))
		message = json.dumps(finished_task).encode()
		updates_socket.send(message)
		print("Update sent to master")
		updates_socket.close()

if __name__ == '__main__':

	port = int(sys.argv[1])
	worker_id = int(sys.argv[2])

	worker = Worker(worker_id, port)
    
	listen_tasks_thread = threading.Thread(target = worker.listen_tasks)
	execute_tasks_thread = threading.Thread(target = worker.execute_tasks)
	
	print("Connected to Master")
	listen_tasks_thread.start()
	execute_tasks_thread.start()
	
	listen_tasks_thread.join()
	execute_tasks_thread.join()
