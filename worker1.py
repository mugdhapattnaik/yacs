#!/usr/bin/python3

import json
import socket
import time
import sys
import random
import threading


class Worker:

	class Task:
		
		def __init__(self, task_id, duration, start_time):
			self.task_id = task_id
			self.duration = duration
			self.start_time = start_time
			self.elapsed_time = 0
			
	def __init__(self, worker_id, port):
		self.id = worker_id
		self.port = port
		self.execution_pool = set()
		
		'''
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.socket.bind(('localhost', port))
		self.socket.listen(1)

		while True:
			first_conn, addr = self.socket.accept()		
			if(addr[1] != '127.0.0.1'):
				continue
			c = first_conn.recv(2048).decode()
			first_conn.close()
			
			config = json.loads(c)
			
			self.slots = int(config["slots"])
			break
		'''
	
	def listen_tasks(self):
		
		worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		worker_socket.bind(('localhost', port))
		worker_socket.listen(1)
		
		while True:
			task_conn, addr = worker_socket.accept()
			if(addr[1] != '127.0.0.1'):
				continue
			t = task_conn.recv(2048).decode()
			task_conn.close()
			
			task_info = json.loads(t)
			task_id = task_info["task_id"]
			task_duration = task_info["duration"]
			
			task = Task(task_id, task_duration, time.time())
			
			self.execution_pool.add(task)
		
	def execute_tasks(self):
		
		while True:
			
			if(not self.execution_pool):
				time.sleep(1)
			else:
				for task in self.execution_pool:
					task.elapsed_time += time.time() - task.start_time
					if(task.elapsed_time >= task.duration):
						self.execution_pool.remove(task)
						send_update(task)
				time.sleep(1)

	def send_update(self, task):
		finished_task = {"worker_id": self.id, "task_id": task.task_id}
		updates_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		updates_port = 5001
		updates_socket.connect((host, updates_port))
		message = json.dumps(finished_task).encode()
		updates_socket.send(message)
		updates_socket.close()

if __name__ == '__main__':
	host = 'localhost'
	port = int(sys.argv[1])
	worker_id = int(sys.argv[2])

	worker = Worker(worker_id, port)

	listen_tasks_thread = threading.Thread(target = worker.listen_tasks)
	execute_tasks_thread = threading.Thread(target = worker.execute_tasks)
	
	listen_tasks_thread.start()
	execute_tasks_thread.start()
	
	listen_tasks_thread.join()
	execute_tasks_thread.join()