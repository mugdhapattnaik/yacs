#!/usr/bin/python3

import json
import socket
import time
import sys
import random
import threading

from queue import Queue
from logger import masterLogger

lock2 = threading.Lock()


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
			self.num_map_tasks = len(request["map_tasks"])
			
			for mt in request["map_tasks"]:
				self.map_tasks.put({"job_id": self.id, "task_id": mt["task_id"], "duration": mt["duration"]})
				master.tasks[mt["task_id"]] = {"job_id": self.id, "type": "map"}
			
			for rt in request["reduce_tasks"]:
				self.reduce_tasks.put({"job_id": self.id, "task_id": rt["task_id"], "duration": rt["duration"]})
				master.tasks[rt["task_id"]] = {"job_id": self.id, "type": "reduce"}

	def __init__(self, config, sch_algo='RR'):
		
		for worker_config in config["workers"]:
			self.worker_ids.append(worker_config["worker_id"])
			self.workers[worker_config["worker_id"]] = self.Worker(worker_config)

		if sch_algo == 'RR':
			worker_ids.sort()
			self.current_worker_id = worker_ids[0]
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
		
		#initializing loggers
		self.ml = masterLogger()	
		self.ml.initLog(self.sch_algo, self.worker_ids, self.workers)
	
	def pr_workers(self):

		for i in self.worker_ids:
			worker = self.workers[i]
			print("Worker ", worker.id, ": ", worker.total_slots, worker.active_slots)

	def pr_jobs(self):
		
		for k, v in self.jobs.items():
			print("Jobs: ", k, ":", v)


	def listen_requests(self):
		requests_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		requests_port = 5000
		requests_socket.bind(('', requests_port))
		requests_socket.listen(10)
		
		while True:
			req_conn, addr = requests_socket.accept()	
			r = req_conn.recv(2048).decode()
			
			request = json.loads(r)
			
			print("Listening for job requests...")
			
			job = self.Job(self, request)
			self.request_queue.put(job)
			self.jobs[request["job_id"]] = job
			
			req_conn.close()
			
	def schedule(self):
			while True:
				
				if self.request_queue.empty():
					continue
				
				job = self.request_queue.get()
				self.ml.logtime(job.id)
				
				while not job.map_tasks.empty():
					worker = self.sch_algo()
					print(worker.id)
					map_task = job.map_tasks.get()
					selsf.send_task(map_task, worker)
					print("========SENT MAP TASK=========", map_task["task_id"])
					self.pr_workers()
					#WITHIN OR OUTSIDE CRITICAL SECTION					
					self.ml.prLog(self.worker_ids, self.workers, time.time())
					lock2.release()	
			
	def listen_updates(self):
		worker_updates_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		worker_updates_port = 5001
		
		worker_updates_socket.bind(('localhost', worker_updates_port))
		worker_updates_socket.listen(10)
		
		while True:
			conn, addr = worker_updates_socket.accept()
			m = conn.recv(8192).decode()
			
			message = json.loads(m)
			print("Received update from worker")
			
			worker_id = message["worker_id"]
			task_id = message["task_id"]
			self.update_queue.put((worker_id, task_id))
						
			print("==============COMPLETED TASK==========", task_id, "FROM", worker_id)
			
			conn.close()
			
	def update_dependencies(self):
			
			while True:
				
				if(self.update_queue.empty()):
					continue
				
				worker_id, task_id = self.update_queue.get()
				
				print("Updating task dependencies")
				
				worker = self.workers[worker_id]
				job_id = self.tasks[task_id]["job_id"]
				task_type = self.tasks[task_id]["type"]
				job = self.jobs[job_id]
				
#				self.pr_jobs()
					
#				if(job.map_tasks.empty() and job.reduce_tasks.empty()):
#					continue

				if(task_type == "map"):
					job.num_map_tasks -= 1
									
				if(job.num_map_tasks == 0 or task_type == "reduce"):
					if(job.reduce_tasks.empty()):
						lock2.acquire()
						worker.active_slots -= 1
						lock2.release()
					else:
						reduce_task = job.reduce_tasks.get()
						lock2.acquire()
						self.send_task(reduce_task, worker)
						lock2.release()					
						print("========SENT REDUCE TASK=======", reduce_task["task_id"])
				else:
					lock2.acquire()
					worker.active_slots -= 1
					lock2.release()
					
	def send_task(self, task, worker):
		host = 'localhost'
		port = int(worker.port)

		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as c:
			c.connect((host, port))
			message = json.dumps(task).encode()
			c.send(message)
	
	def random_algo(self):
		print("Task scheduled using randalgo")
		while True:
			worker_id = random.choice(self.worker_ids)
			lock2.acquire()
			worker = self.workers[worker_id]
			if(worker.available()):
				worker.active_slots += 1
				return worker
			lock2.release()

	def round_robin_algo(self):
		print("Task scheduled using roundrobin")
		while True:
			#worker_ids = sorted(self.worker_ids)
			lock2.acquire()
			curr_index = self.worker_ids.index(self.current_worker_id)
			next_index = (curr_index + 1) % len(self.worker_ids)
			current_worker = self.workers[self.current_worker_id]
			if current_worker.available():
				worker.active_slots += 1
				self.current_worker_id = self.worker_ids[next_index]
				return worker
			lock2.release()
			else:
				worker_found = False
				i = next_index
				while not worker_found:
					lock2.acquire()
					worker = self.workers[i]
					if(worker.available()):
						worker.active_slots += 1
						self.current_worker_id = self.worker_ids[next_index]
						return worker
					i = (i + 1) % len(self.worker_ids)
					lock2.release()

	def least_loaded_algo(self):
		print("Task scheduled using leastloaded")
		while True:
			lock2.acquire()
			least_loaded = self.workers[self.worker_ids[0]]
			max_slots = least_loaded.total_slots - least_loaded.active_slots
			for worker_id in self.worker_ids[1::]:
				worker = self.workers[worker_id]
				curr_slots = worker.total_slots - worker.active_slots
				if(curr_slots > max_slots):
					least_loaded = worker
					max_slots = curr_slots
			
			worker = least_loaded			
			if(worker.available()):
				worker.active_slots += 1
				return worker
			
			lock2.release()


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
