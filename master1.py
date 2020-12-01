#!/usr/bin/python3

import json
import socket
import time
import sys
import random
import threading

from queue import Queue

lock1 = threading.Lock()
lock2 = threading.Lock()
lock3 = threading.Lock()

class Master:

	class Worker:
		
		def __init__(self, config):
			self.id = config["worker_id"]
			self.total_slots = int(config["slots"])
			self.active_slots = 0
			self.port = config["port"]
		
		def available(self):
			return self.active_slots < self.total_slots
		
	class Job:
		
		def __init__(self, master, request):
			self.id = request["job_id"]
			self.map_tasks = Queue()
			self.reduce_tasks = Queue()
			
			for mt in request["map_tasks"]:
				self.map_tasks.put({"task_id": mt["task_id"], "duration": mt["duration"]})
				master.tasks[mt["task_id"]] = {"job_id": self.id, "type": "map"}
			
			for rt in request["reduce_tasks"]:
				self.reduce_tasks.put({"task_id": rt["task_id"], "duration": rt["duration"]})
				master.tasks[rt["task_id"]] = {"job_id": self.id, "type": "reduce"}

	def __init__(self, config, sch_algo='RR'):
		self.count = 0
		if sch_algo == 'RR':
			self.sch_algo = self.round_robin_algo
		elif sch_algo == 'RANDOM':
			self.sch_algo = self.random_algo
		elif sch_algo == 'LL':
			self.sch_algo = self.least_loaded_algo
		
		self.worker_ids = []
		self.workers = {}
		self.jobs = {}
		self.tasks = {}
		self.request_queue = Queue()
		self.update_queue = Queue()
		
		for worker_config in config["workers"]:
			self.worker_ids.append(worker_config["worker_id"])
			self.workers[worker_config["worker_id"]] = self.Worker(worker_config)
		
	def pr_workers(self):

		for i in self.worker_ids:
			worker = self.workers[i]
			print("Worker: ", worker.id, worker.total_slots, worker.active_slots)


	def listen_requests(self):
		requests_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		requests_port = 5000
		requests_socket.bind(('', requests_port))
		requests_socket.listen(1)
		
		while True:
			req_conn, addr = requests_socket.accept()	
			r = req_conn.recv(2048).decode()
			req_conn.close()
			request = json.loads(r)
			
			lock1.acquire()
			print("lr")
			
			job = self.Job(self, request)
			self.request_queue.put(job)
			self.jobs[request["job_id"]] = job
			lock1.release()
			
	def schedule(self):
			while True:
				if(self.request_queue.empty()):
					continue
				
				lock1.acquire()	
				job = self.request_queue.get()
				print("sc1")
				
				tmp = list(job.map_tasks.queue)
				for e in tmp:
					print(e)
				
				while(not job.map_tasks.empty()):
								
					lock2.acquire()
					print("sc2")
					worker = self.sch_algo()
					print(worker.id)
					map_task = job.map_tasks.get()
					
					self.count += 1
					print(self.count)
					print(map_task["task_id"])
					self.send_task(map_task, worker)
					self.pr_workers()
					lock2.release()
				lock1.release()		
				
			
	def listen_updates(self):
		worker_updates_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		worker_updates_port = 5001
		
		worker_updates_socket.bind(('localhost', worker_updates_port))
		worker_updates_socket.listen(5)
		
		while True:
			conn, addr = worker_updates_socket.accept()
			m = conn.recv(8192).decode()
			conn.close()
			message = json.loads(m)
			print("lu")
			
			worker_id = message["worker_id"]
			task_id = message["task_id"]
			
			self.update_queue.put((worker_id, task_id))
	
	def update_dependencies(self):		
			
			while True:
				
				if(self.update_queue.empty()):
					continue
				
				worker_id, task_id = self.update_queue.get()
				
				lock1.acquire()
				lock2.acquire()
				
				print("ud")
				worker = self.workers[worker_id]
				job_id = self.tasks[task_id]["job_id"]
				task_type = self.tasks[task_id]["type"]
				job = self.jobs[job_id]
					
				if(task_type == "reduce" or job.map_tasks.empty()):
					if(job.reduce_tasks.empty()):
						self.jobs.pop(job_id)
						worker.active_slots -= 1
						lock1.release()
						lock2.release()
						continue
					else:
						reduce_task = job.reduce_tasks.get()
						self.send_task(reduce_task, worker)					
				else:
					worker.active_slots -= 1
					lock1.release()
					lock2.release()
					continue
				lock1.release()
				lock2.release()

	def send_task(self, task, worker):
		host = 'localhost'
		port = int(worker.port)

		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as c:
			c.connect((host, port))
			message = json.dumps(task).encode()
			c.send(message)
	
	def random_algo(self):
		print("randalgo")
		while True:
			worker_id = random.choice(self.worker_ids)
			worker = self.workers[worker_id]
			if(worker.available()):
				worker.active_slots += 1
				return worker

	def round_robin_algo(self):
		print("robin")
		while True:
			worker_ids = sorted(self.worker_ids)
			for worker_id in worker_ids:
				worker = self.workers[worker_id]
				if(worker.available()):
					worker.active_slots += 1
					return worker

	def least_loaded_algo(self):
		print("leastloaded")
		least_loaded = self.workers[self.worker_ids[0]]
		for worker_id in self.worker_ids[1::]:
			worker = self.workers[worker_id]
			curr_slots = worker.total_slots - worker.active_slots
			max_slots = least_loaded.total_slots - least_loaded.active_slots
			if(curr_slots > max_slots):
				least_loaded = worker
		worker = least_loaded
		worker.active_slots += 1
		return worker


if __name__ == '__main__':	 

	config_file = open(sys.argv[1], 'r')
	config = json.load(config_file)
	scheduling_algo = str(sys.argv[2])
	
	master = Master(config, scheduling_algo)
	
	listen_requests_thread = threading.Thread(target = master.listen_requests)
	listen_updates_thread = threading.Thread(target = master.listen_updates)
	schedule_thread = threading.Thread(target = master.schedule)
	update_dependencies_thread = threading.Thread(target = master.update_dependencies)
	
	listen_requests_thread.start()
	listen_updates_thread.start()
	schedule_thread.start()
	update_dependencies_thread.start()
	
	
	listen_requests_thread.join()
	listen_updates_thread.join()
	schedule_thread.join()
	update_dependencies_thread.join()
