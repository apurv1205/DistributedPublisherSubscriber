from concurrent import futures
import time
import grpc
import pr_pb2
import pr_pb2_grpc
import thread
import sys
import json

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
CENTRAL_SERVER_IP = ""
ACCESS_POINT = ""

if len(sys.argv) < 2 : 
	print "ERROR : Enter the port for access point server...exiting"
	exit()
port = sys.argv[1]
self_ip = "localhost:"+str(port)

class Client(pr_pb2_grpc.PublishTopicServicer):
	def forwardBackup(self, request_iterator, context):
		for request in request_iterator :
			print request
		return pr_pb2.acknowledge(ack="Backup received by the client...")

	def forwardData(self, request, context):
		print request
		return pr_pb2.acknowledge(ack="Data received by the client...")

def serve():
	server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
	pr_pb2_grpc.add_PublishTopicServicer_to_server(Client(), server)
	server.add_insecure_port(self_ip)
	server.start()
	try:
		while True:
			time.sleep(_ONE_DAY_IN_SECONDS)
	except KeyboardInterrupt:
		server.stop(0)

def subscribe_topic(topic,self_ip):
	channel = grpc.insecure_channel(ACCESS_POINT)
	stub = pr_pb2_grpc.PublishTopicStub(channel)
	response = stub.subscribeRequest(pr_pb2.topicSubscribe(topic=topic,client_ip=self_ip))

def push_topic(topic,data):
	print "ip:",ACCESS_POINT
	channel = grpc.insecure_channel(ACCESS_POINT)
	stub = pr_pb2_grpc.PublishTopicStub(channel)
	response = stub.publishRequest(pr_pb2.topicData(topic=topic, data=data))
	print("Ack received: " + response.ack)

def get_front_ip():
	channel = grpc.insecure_channel(CENTRAL_SERVER_IP)
	stub = pr_pb2_grpc.PublishTopicStub(channel)
	response = stub.getFrontIp(pr_pb2.empty())
	print("Ip alloted: " + response.ip)
	print response.ip
	return response.ip

if __name__ == '__main__':
	thread.start_new_thread(serve,())

	a = json.load(open("options","r"))
	CENTRAL_SERVER_IP = a["Central_server"]
	ACCESS_POINT = get_front_ip()

	while (True) :

		print "Type 1 for publish\nType 2 for subscribe\n"
		response = raw_input()

		if response == "1" :
			print "Enter topic"
			topic = raw_input()

			print "Enter data"
			data = raw_input()

			push_topic(topic,data)

		elif response == "2" :
			print "Enter topic"
			topic = raw_input()
			subscribe_topic(topic,self_ip)