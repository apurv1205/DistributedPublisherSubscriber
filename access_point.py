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
THRESHOLD = 1

subscribersDict = {}
newSubscribers = {}

def forwardToClient(lst):
    request = lst[0]
    client_ip = lst[1]
    channel = grpc.insecure_channel(client_ip)
    stub = pr_pb2_grpc.PublishTopicStub(channel)
    response = stub.forwardData(request)

def sendToClient(lst):
    request = lst[0]
    client_ip = lst[1]
    channel = grpc.insecure_channel(client_ip)
    stub = pr_pb2_grpc.PublishTopicStub(channel)
    response = stub.sendData(request)

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
    def unsubscribeRequest(self, request_iterator, context):
        topicList = []
        client_ip = ""
        for request in request_iterator :
            client_ip = request.client_ip
            topicList.append(request.topic)
        subscribersDict = json.load(open("subscriberDB"+port+".json","r"))
        for topic in topicList :
            subscribersDict[topic].remove(client_ip)
            if len(subscribersDict[topic]) <= THRESHOLD and len(subscribersDict[topic]) > 0 :
                channel = grpc.insecure_channel(CENTRAL_SERVER_IP)
                stub = pr_pb2_grpc.PublishTopicStub(channel)
                response = stub.deReplicaRequest(pr_pb2.topicSubscribe(topic=topic,client_ip="localhost:"+port))
                if response.ack == "DONE" :
                    dct = json.load(open("dataDB"+port+".json","r"))
                    del dct[topic]
                    json.dump(dct,open("dataDB"+port+".json","w"))
            else :
                channel = grpc.insecure_channel(CENTRAL_SERVER_IP)
                stub = pr_pb2_grpc.PublishTopicStub(channel)
                response = stub.unsubscribeRequestCentral(pr_pb2.topicSubscribe(topic=topic,client_ip="localhost:"+port))
                if len(subscribersDict[topic]) == 0:
                    del subscribersDict[topic]
        json.dump(subscribersDict,open("subscriberDB"+port+".json","w"))
        return pr_pb2.acknowledge(ack="done")


    def sendData(self, request, context):
        subscribersDict = json.load(open("subscriberDB"+port+".json","r"))
        pool = ThreadPool(len(subscribersDict[request.topic])) 
        lst = []
        for client_ip in subscribersDict[request.topic]:
            print client_ip
            lst.append([request,client_ip])
        results = pool.map(forwardToClient, lst)
        return pr_pb2.acknowledge(ack="data sent to subscribed clients")

    def sendBackupRequest(self, request, context):
        dct = json.load(open("dataDB"+port+".json","r"))
        channel = grpc.insecure_channel(request.client_ip)
        stub = pr_pb2_grpc.PublishTopicStub(channel)
        response = stub.sendBackup(generateBackup(request.topic,dct))
        return pr_pb2.acknowledge(ack="data send to : "+request.client_ip+" complete...")

    def sendBackupRequestReplica(self, request, context):
        dct = json.load(open("dataDB"+port+".json","r"))
        channel = grpc.insecure_channel(request.client_ip)
        stub = pr_pb2_grpc.PublishTopicStub(channel)
        response = stub.sendBackupReplica(generateBackup(request.topic,dct))
        return pr_pb2.acknowledge(ack="data send to : "+request.client_ip+" replica complete...")

    def sendBackupReplica(self, request_iterator, context):
        requestList = []
        for request in request_iterator :
            requestList.append(request)
        dct = json.load(open("dataDB"+port+".json","r"))
        for request in requestList :
            print request
            if request.topic not in dct.keys() : 
                dct[request.topic] = []
            dct[request.topic].append(request.data)
        json.dump(dct,open("dataDB"+port+".json","w"))
        return pr_pb2.acknowledge(ack="complete data backup received by the replica...")

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
        subscribersDict = json.load(open("subscriberDB"+port+".json","r"))
        if request.topic not in subscribersDict.keys() : 
            subscribersDict[request.topic] = []
            print "New subscriber, new frontend subscriber"
            subType = "new"
        else : 
            print "New subscriber, old frontend subscriber"
            subType = "old"
        newSubscribers[request.topic] = []
        newSubscribers[request.topic].append(request.client_ip)
        subscribersDict[request.topic].append(request.client_ip)
        if len(subscribersDict[request.topic]) > THRESHOLD :
            channel = grpc.insecure_channel(CENTRAL_SERVER_IP)
            stub = pr_pb2_grpc.PublishTopicStub(channel)
            response = stub.replicaRequest(pr_pb2.topicSubscribe(topic=request.topic,client_ip="localhost:"+port))

        json.dump(subscribersDict,open("subscriberDB"+port+".json","w"))
        channel = grpc.insecure_channel(CENTRAL_SERVER_IP)
        stub = pr_pb2_grpc.PublishTopicStub(channel)
        response = stub.subscribeRequestCentral(pr_pb2.topicSubscribeCentral(topic=request.topic,client_ip="localhost:"+str(port),type=subType))
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