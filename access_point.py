from concurrent import futures
import time
import grpc
import pr_pb2
import pr_pb2_grpc
import thread
import sys
import csv
from multiprocessing.dummy import Pool as ThreadPool 
import json

if len(sys.argv) < 2 : 
    print "ERROR : Enter the port for access point server...exiting"
    exit()
port = sys.argv[1]

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

CENTRAL_SERVER_IP = ""
subscribersDict = {}
newSubscribers = {}

def forwardToClient(lst):
    request = lst[0]
    client_ip = lst[1]
    channel = grpc.insecure_channel(client_ip)
    stub = pr_pb2_grpc.PublishTopicStub(channel)
    response = stub.forwardData(request)
    return response.ack

def sendToClient(lst):
    request = lst[0]
    client_ip = lst[1]
    channel = grpc.insecure_channel(client_ip)
    stub = pr_pb2_grpc.PublishTopicStub(channel)
    response = stub.sendData(request)
    return response.ack

def generateForwardBackup(requestList) :
    for request in requestList :
        yield pr_pb2.topicData(topic=request.topic,data=request.data)

def forwardBackupToClient(lst):
    requestList = lst[0]
    client_ip = lst[1]
    channel = grpc.insecure_channel(client_ip)
    stub = pr_pb2_grpc.PublishTopicStub(channel)
    response = stub.forwardBackup(generateForwardBackup(requestList))

def generateBackup(topic,dct) :
    for data in dct[topic] :
        yield pr_pb2.topicData(topic=topic,data=data)

def subscribeServer(topic,subType):
    channel = grpc.insecure_channel(CENTRAL_SERVER_IP)
    stub = pr_pb2_grpc.PublishTopicStub(channel)
    response = stub.subscribeRequestCentral(pr_pb2.topicSubscribeCentral(topic=topic,client_ip="localhost:"+str(port),type=subType))

def register_ip():
    channel = grpc.insecure_channel(CENTRAL_SERVER_IP)
    stub = pr_pb2_grpc.PublishTopicStub(channel)
    response = stub.registerIp(pr_pb2.ips(ip = "localhost:"+str(port)))
    print(response.ack)

def publishData(lst):
    request = lst[0]
    accesspoint = lst[1]
    channel = grpc.insecure_channel(accesspoint)
    stub = pr_pb2_grpc.PublishTopicStub(channel)
    response = stub.publish(pr_pb2.topicData(topic=request.topic, data=request.data))
    return response.ack

def find_ips(topic):
    channel = grpc.insecure_channel(CENTRAL_SERVER_IP)
    stub = pr_pb2_grpc.PublishTopicStub(channel)
    responses = stub.giveIps(pr_pb2.topic(topic=topic))
    lst =[]
    for response in responses :
        print("IP received: " + response.ip)
        lst.append(response.ip)
    return lst

class AccessPoint(pr_pb2_grpc.PublishTopicServicer):

    def sendData(self, request, context):
        pool = ThreadPool(len(subscribersDict[request.topic])) 
        lst = []
        print "here",request.topic,subscribersDict[request.topic]
        for client_ip in subscribersDict[request.topic]:
            print client_ip
            lst.append([request,client_ip])
        results = pool.map(forwardToClient, lst)

    def sendBackupRequest(self, request, context):
        dct = json.load(open("dataDB"+port+".json","r"))
        if request.topic not in dct.keys() :
            return pr_pb2.acknowledge(ack="no data of topic : "+request.topic+" in the topic server : "+port)
        channel = grpc.insecure_channel(request.client_ip)
        stub = pr_pb2_grpc.PublishTopicStub(channel)
        response = stub.sendBackup(generateBackup(request.topic,dct))
        return pr_pb2.acknowledge(ack="data send to : "+request.client_ip+" complete...")

    def sendBackup(self, request_iterator, context):
        requestList = []
        topic = ""
        for request in request_iterator :
            requestList.append(request)
            topic = request.topic
        pool = ThreadPool(len(newSubscribers[topic])) 
        lst = []
        for client_ip in newSubscribers[topic]:
            lst.append([requestList,client_ip])
        results = pool.map(forwardBackupToClient, lst)
        del newSubscribers[topic]
        return pr_pb2.acknowledge(ack="complete data backup received and forwarded to resepective clients...")

    def publishRequest(self, request, context):
        returned_ips = find_ips(request.topic)
        lst = []
        pool = ThreadPool(len(returned_ips)) 
        for returned_ip in returned_ips :
            lst.append([request,returned_ip])
        results = pool.map(publishData, lst)
        print results
        return pr_pb2.acknowledge(ack="Published in "+str(len(results))+" topic servers")

    def subscribeRequest(self, request, context):
        print "Subscribe request from client",request.client_ip," for topic",request.topic
        subType = ""
        print "New subscriber"
        subType = "new"
        if request.topic not in newSubscribers.keys() : 
            newSubscribers[request.topic] = []
        newSubscribers[request.topic].append(request.client_ip)
        
        # print "Old subscriber"
        # subType = "old"
        if request.topic not in subscribersDict.keys() :
            subscribersDict[request.topic]=[]
        subscribersDict[request.topic].append(request.client_ip)
        json.dump(subscribersDict,open("subscriberDB"+port+".json","w"))
        subscribeServer(request.topic,subType)
        return pr_pb2.acknowledge(ack="temporary acknowledge")

    def publish(self, request, context):
        print "Data received...",request.topic, request.data
        dct = json.load(open("dataDB"+port+".json","r"))
        if request.topic not in dct.keys() : 
            dct[request.topic] = []
        dct[request.topic].append(request.data)
        json.dump(dct,open("dataDB"+port+".json","w"))
        channel = grpc.insecure_channel(CENTRAL_SERVER_IP)
        stub = pr_pb2_grpc.PublishTopicStub(channel)
        responses = stub.giveSubscriberIps(pr_pb2.topicSubscribe(topic=request.topic,client_ip="localhost:"+port))
        ipList = []
        for response in responses :
            ipList.append(response.ip)
            print("IP received: " + response.ip)
        if ipList[0] == "none" :
            return pr_pb2.acknowledge(ack="No subscribers for this replica")
        pool = ThreadPool(len(ipList)) 
        lst = []
        for client_ip in ipList:
            lst.append([request,client_ip])
        results = pool.map(sendToClient, lst)
        return pr_pb2.acknowledge(ack="Data send to clients complete")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pr_pb2_grpc.add_PublishTopicServicer_to_server(AccessPoint(), server)
    server.add_insecure_port('[::]:'+port)
    server.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    a = json.load(open("options","r"))
    CENTRAL_SERVER_IP = a["Central_server"]
    register_ip()

    json.dump({},open("dataDB"+port+".json","w"))
    json.dump({},open("subscriberDB"+port+".json","w"))
    serve()