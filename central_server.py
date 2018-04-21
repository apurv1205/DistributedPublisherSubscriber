from concurrent import futures
import time
import json
import grpc
import pr_pb2
import pr_pb2_grpc
import thread
import random
import sys
import socket
from pymongo import MongoClient
from datetime import datetime
import logging

if len(sys.argv) < 3 : 
    print "ERROR : Enter the port and type of server (0 for master, 1 for backup)...exiting"
    exit()

port = sys.argv[1]
IS_MASTER = False
if sys.argv[2] == "0" :
    IS_MASTER = True
_ONE_DAY_IN_SECONDS = 60 * 60 * 24
SELF_IP=[l for l in ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1], [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0]

def two_phase_init(request):
    channel = grpc.insecure_channel(backupCentralServer)
    stub = pr_pb2_grpc.PublishTopicStub(channel)
    response = stub.commit_request(request)
    if response.ack=="AGREED":
        logging.info("%s:%s:COMMIT",str(datetime.now()),request.filename)
        response1 = stub.commit_phase_two(request)
        return response1.ack
    else :
        return "ERROR"
        

class CentralServer(pr_pb2_grpc.PublishTopicServicer):
    def commit_request(self,request,context):
        if request.filename == "twoLevelDict":        
            if request.action == "remove" :
                if request.level == "3" : 
                    twoLevelDict.delete_one({"topic":request.data_1,"subscriber":request.data_3})
                    logging.info("%s:%s:REMOVE 3 %s %s %s",str(datetime.now()),request.filename,request.data_1,"NIL",request.data_3)
                    
                elif request.level == "2" :
                    twoLevelDict.delete_many({"topic":request.data_1,"publisher":request.data_2})
                    logging.info("%s:%s:REMOVE 2 %s %s %s",str(datetime.now()),request.filename,request.data_1,request.data_2,"NIL")
                    
            elif request.action == "insert":
                if request.level == "3" :
                   twoLevelDict.insert_one({"topic":request.data_1,"publisher":request.data_2,"subscriber":request.data_3}) 
                   logging.info("%s:%s:INSERT 3 %s %s %s",str(datetime.now()),request.filename,request.data_1,request.data_2,request.data_3)
                   
        elif request.filename == "frontends" :
            if request.action == "remove" :
                if request.level == "2" : 
                    if request.data_1 == "ip" :
                        frontends.delete_one({"type":request.data_1,request.data_1:request.data_2})
                        logging.info("%s:%s:REMOVE 2 %s %s %s",str(datetime.now()),request.filename,request.data_1,request.data_2,request.data_3)
                    else :
                        frontends.delete_one({"type":request.data_1,request.data_1:int(request.data_2)})
                        logging.info("%s:%s:REMOVE 2 %s %s %s",str(datetime.now()),request.filename,request.data_1,request.data_2,request.data_3)

            elif request.action == "insert" :
                if request.level == "2" : 
                    if request.data_1 == "ip" :
                        frontends.insert_one({"type":request.data_1,request.data_1:request.data_2})
                        logging.info("%s:%s:INSERT 2 %s %s %s",str(datetime.now()),request.filename,request.data_1,request.data_2,request.data_3)
                    else :
                        frontends.insert_one({"type":request.data_1,request.data_1:int(request.data_2)})
                        logging.info("%s:%s:INSERT 2 %s %s %s",str(datetime.now()),request.filename,request.data_1,request.data_2,request.data_3)
        
        return pr_pb2.acknowledge(ack="AGREED")

    def commit_phase_two(self,request,context):
        logging.info("%s:%s:COMMIT",str(datetime.now()),request.filename)
        return pr_pb2.acknowledge(ack="COMPLETE")

    def unsubscribeRequestCentral(self, request, context):
        if IS_MASTER:
            response = two_phase_init(pr_pb2.commit_req_data(action="remove",level="3",data_1 = request.topic,data_2="", data_3 = request.client_ip, filename = "twoLevelDict",function_name="unsubscribeRequestCentral"))
            if response == "COMPLETE" :
                twoLevelDict.delete_one({"topic":request.topic,"subscriber":request.client_ip})
        else :
            twoLevelDict.delete_one({"topic":request.topic,"subscriber":request.client_ip})
        return pr_pb2.acknowledge(ack="temporary acknowledge from central server")

    def deReplicaRequest(self, request, context):
        dct = {}
        cursor = twoLevelDict.find({"topic":request.topic})
        for document in cursor:
            if dct.has_key(document["publisher"]):
                pass
            else : dct[document["publisher"]] = []
            if document["subscriber"]!="NULL" :
                dct[document["publisher"]].append(document["subscriber"])
        if len(dct.keys()) == 1:
            return pr_pb2.acknowledge(ack="ERROR")
        extraSubscribers = dct[request.client_ip]
        if IS_MASTER:
            response = two_phase_init(pr_pb2.commit_req_data(action="remove",level="2",data_1 = request.topic,data_2=request.client_ip, data_3 = "", filename = "twoLevelDict",function_name="deReplicaRequest"))
            if response == "COMPLETE" :
                twoLevelDict.delete_many({"topic":request.topic,"publisher":request.client_ip})
        else :
            twoLevelDict.delete_many({"topic":request.topic,"publisher":request.client_ip})
        # twoLevelDict.delete_many({"topic":request.topic,"publisher":request.client_ip})
        del dct[request.client_ip]
        for subscriber in extraSubscribers :
            allotedServer = ""
            l = sys.maxsize
            tempIp = ""
            for ip in dct.keys():
                tempCursor = twoLevelDict.find({"topic":request.topic,"publisher":ip})
                if tempCursor.count() < l:
                    l = tempCursor.count()
                    tempIp = ip

            if IS_MASTER:
                response = two_phase_init(pr_pb2.commit_req_data(action="insert",level="3",data_1 = request.topic,data_2=tempIp, data_3 = subscriber, filename = "twoLevelDict",function_name="deReplicaRequest"))
                if response == "COMPLETE" :
                    twoLevelDict.insert_one({"topic":request.topic,"publisher":tempIp,"subscriber":subscriber})
                else :
                    twoLevelDict.insert_one({"topic":request.topic,"publisher":tempIp,"subscriber":subscriber})
            # twoLevelDict.insert_one({"topic":request.topic,"publisher":tempIp,"subscriber":subscriber})
        return pr_pb2.acknowledge(ack="DONE") 

    def querryTopics(self, request, context):
        cursor = twoLevelDict.find({"subscriber":"NULL"})
        for document in cursor :
            yield pr_pb2.topic(topic=document["topic"])

    def replicaRequest(self, request, context):
        document = twoLevelDict.find_one({"topic":request.topic,"subscriber":"NULL"})
        allotedServer = document["publisher"]
        if twoLevelDict.find({"topic":request.topic,"publisher":request.client_ip}).count() > 0 :
            return pr_pb2.acknowledge(ack="Requesting front end server already a replica for "+request.topic)
        if twoLevelDict.find({"topic":request.topic,"subscriber":request.client_ip}).count() > 0 :
            if IS_MASTER:
                response = two_phase_init(pr_pb2.commit_req_data(action="remove",level="3",data_1 = request.topic,data_2="", data_3 = request.client_ip, filename = "twoLevelDict",function_name="replicaRequest"))
                if response == "COMPLETE" :
                    twoLevelDict.delete_one({"topic":request.topic,"subscriber":request.client_ip})
                else :
                    twoLevelDict.delete_one({"topic":request.topic,"subscriber":request.client_ip})
            # twoLevelDict.delete_one({"topic":request.topic,"subscriber":request.client_ip})

        if IS_MASTER:
            response = two_phase_init(pr_pb2.commit_req_data(action="insert",level="3",data_1 = request.topic,data_2=request.client_ip, data_3 = "NULL", filename = "twoLevelDict",function_name="replicaRequest"))
            if response == "COMPLETE" :
                twoLevelDict.insert_one({"topic":request.topic,"publisher":request.client_ip,"subscriber":"NULL"})
            else :
                twoLevelDict.insert_one({"topic":request.topic,"publisher":request.client_ip,"subscriber":"NULL"})
        # twoLevelDict.insert_one({"topic":request.topic,"publisher":request.client_ip,"subscriber":"NULL"})
        if IS_MASTER:
            response = two_phase_init(pr_pb2.commit_req_data(action="insert",level="3",data_1 = request.topic,data_2=request.client_ip, data_3 = request.client_ip, filename = "twoLevelDict",function_name="replicaRequest"))
            if response == "COMPLETE" :
                twoLevelDict.insert_one({"topic":request.topic,"publisher":request.client_ip,"subscriber":request.client_ip})
            else :
                twoLevelDict.insert_one({"topic":request.topic,"publisher":request.client_ip,"subscriber":request.client_ip})
        # twoLevelDict.insert_one({"topic":request.topic,"publisher":request.client_ip,"subscriber":request.client_ip})
        channel = grpc.insecure_channel(allotedServer)
        stub = pr_pb2_grpc.PublishTopicStub(channel)
        response = stub.sendBackupRequestReplica(pr_pb2.topicSubscribe(topic=request.topic, client_ip=request.client_ip))
        print "done modifying the dct"
        return pr_pb2.acknowledge(ack="Requesting front end server "+request.client_ip+" made a replica of topic(backup sent) "+request.topic)

    def subscribeRequestCentral(self, request, context):
        print "Subscribe request from access point",request.client_ip," for topic",request.topic," of type :",request.type
        allotedServer = ""
        if request.type == "new" :
            if twoLevelDict.find({"topic":request.topic,"publisher":request.client_ip}).count() > 0:
                allotedServer = request.client_ip 
            else :
                l = sys.maxsize
                tempIp = ""
                cursor = twoLevelDict.find({"subscriber":"NULL"})
                for document in cursor :
                    ip = document["publisher"]
                    if twoLevelDict.find({"topic":request.topic,"publisher":ip}).count() < l :
                        l=twoLevelDict.find({"topic":request.topic,"publisher":ip}).count()
                        tempIp = ip
                allotedServer = tempIp
            if IS_MASTER:
                response = two_phase_init(pr_pb2.commit_req_data(action="insert",level="3",data_1 = request.topic,data_2=allotedServer, data_3 = request.client_ip, filename = "twoLevelDict",function_name="subscribeRequestCentral"))
                if response == "COMPLETE" :
                    twoLevelDict.insert_one({"topic":request.topic,"publisher":allotedServer,"subscriber":request.client_ip})
                else :
                    twoLevelDict.insert_one({"topic":request.topic,"publisher":allotedServer,"subscriber":request.client_ip})
            # twoLevelDict.insert_one({"topic":request.topic,"publisher":allotedServer,"subscriber":request.client_ip})

        else :
            document = twoLevelDict.find_one({"topic":request.topic,"subscriber":request.client_ip})
            allotedServer = document["publisher"]

        channel = grpc.insecure_channel(allotedServer)
        stub = pr_pb2_grpc.PublishTopicStub(channel)
        response = stub.sendBackupRequest(pr_pb2.topicSubscribe(topic=request.topic, client_ip=request.client_ip))
        print response.ack
        return pr_pb2.acknowledge(ack="temporary acknowledge from central server")

    def giveSubscriberIps(self, request, context):
        if twoLevelDict.find({"topic":request.topic,"publisher":request.client_ip}).count() == 1: 
            yield pr_pb2.ips(ip="none")
        else:
            cursor = twoLevelDict.find({"topic":request.topic,"publisher":request.client_ip})
            for document in cursor:
                if document["subscriber"]!="NULL" :
                    yield pr_pb2.ips(ip=document["subscriber"])

    def giveIps(self, request, context):
        cursor = twoLevelDict.find({"topic":request.topic,"subscriber":"NULL"})
        if cursor.count() > 0:
            for document in cursor :
                yield pr_pb2.ips(ip=document["publisher"])

        else : 
            cursor = frontends.find({"type":"ip"})
            lst = []
            for document in cursor :
                lst.append(document["ip"])
            ip = random.choice(lst)
            if IS_MASTER:
                response = two_phase_init(pr_pb2.commit_req_data(action="insert",level="3",data_1 = request.topic,data_2=ip, data_3 = "NULL", filename = "twoLevelDict",function_name="giveIps"))
                if response == "COMPLETE" :
                    twoLevelDict.insert_one({"topic":request.topic,"publisher":ip,"subscriber":"NULL"})
                else :
                    twoLevelDict.insert_one({"topic":request.topic,"publisher":ip,"subscriber":"NULL"})
            # twoLevelDict.insert_one({"topic":request.topic,"publisher":ip,"subscriber":"NULL"})
            yield pr_pb2.ips(ip=ip)

    def getFrontIp(self, request, context) :
        cursor = frontends.find({"type":"index"})
        if cursor.count() == 0:
            return pr_pb2.ips(ip="NONE")
        index = 0
        for document in cursor :
            index = document["index"]
        ipList = []
        cursor = frontends.find({"type":"ip"})
        for document in cursor :
            ipList.append(document["ip"])
        m = ipList[index]
        if index == len(ipList) - 1 :
            if IS_MASTER:
                response = two_phase_init(pr_pb2.commit_req_data(action="remove",level="2",data_1 = "index",data_2=str(index), data_3 = "", filename = "frontends",function_name="getFrontIp"))
                if response == "COMPLETE" :
                    frontends.delete_one({"type":"index","index":index})
                else :
                    frontends.delete_one({"type":"index","index":index})
            # frontends.delete_one({"type":"index","index":index})
            if IS_MASTER:
                response = two_phase_init(pr_pb2.commit_req_data(action="insert",level="2",data_1 = "index",data_2="0", data_3 = "", filename = "frontends",function_name="getFrontIp"))
                if response == "COMPLETE" :
                    frontends.insert_one({"type":"index","index":0})
                else :
                    frontends.insert_one({"type":"index","index":0})
            # frontends.insert_one({"type":"index","index":0})
        else :
            if IS_MASTER:
                response = two_phase_init(pr_pb2.commit_req_data(action="remove",level="2",data_1 = "index",data_2=str(index), data_3 = "", filename = "frontends",function_name="getFrontIp"))
                if response == "COMPLETE" :
                    frontends.delete_one({"type":"index","index":index})
                else :
                    frontends.delete_one({"type":"index","index":index})
            # frontends.delete_one({"type":"index","index":index})
            if IS_MASTER:
                response = two_phase_init(pr_pb2.commit_req_data(action="insert",level="2",data_1 = "index",data_2=str(index+1), data_3 = "", filename = "frontends",function_name="getFrontIp"))
                if response == "COMPLETE" :
                    frontends.insert_one({"type":"index","index":index+1})
                else :
                    frontends.insert_one({"type":"index","index":index+1})
            # frontends.insert_one({"type":"index","index":index+1})
        return pr_pb2.ips(ip=m)

    def registerIp(self, request, context) :
        cursor = frontends.find({"type":"index"})
        if cursor.count() == 0:
            if IS_MASTER:
                response = two_phase_init(pr_pb2.commit_req_data(action="insert",level="2",data_1 = "index",data_2="0", data_3 = "", filename = "frontends",function_name="getFrontIp"))
                if response == "COMPLETE" :
                    frontends.insert_one({"type":"index","index":0})
                else :
                    frontends.insert_one({"type":"index","index":0})
            # frontends.insert_one({"type":"index","index":0})
        if IS_MASTER:
            response = two_phase_init(pr_pb2.commit_req_data(action="insert",level="2",data_1 = "ip",data_2=request.ip, data_3 = "", filename = "frontends",function_name="getFrontIp"))
            if response == "COMPLETE" :
                frontends.insert_one({"type":"ip","ip":request.ip})
            else :
                frontends.insert_one({"type":"ip","ip":request.ip})
        # frontends.insert_one({"type":"ip","ip":request.ip})
        return pr_pb2.acknowledge(ack="Ip added...")


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pr_pb2_grpc.add_PublishTopicServicer_to_server(CentralServer(), server)
    server.add_insecure_port(str(SELF_IP)+":"+port)
    server.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    logging.basicConfig(filename=port+'.log',format='%(message)s',filemode='w',level=logging.DEBUG)
    print SELF_IP
    selfIpDct = json.load(open("options","r"))
    backupCentralServer = selfIpDct["centralServerBackup"]
    mongoClient = MongoClient("localhost", 27017)
    mongoClient.drop_database('CentralServer'+port)
    db = mongoClient['CentralServer'+port]
    frontends = db["frontends"]
    twoLevelDict = db["twoLevelDict"]
    oneLevelDict = db["oneLevelDict"]
    serve()