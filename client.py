import grpc
import pr_pb2
import pr_pb2_grpc


def push_topic(topic,data,accesspoint):
    channel = grpc.insecure_channel(accesspoint)
    stub = pr_pb2_grpc.PublishTopicStub(channel)
    response = stub.publish(pr_pb2.topicData(topic=topic, data=data))
    print("Ack received: " + response.ack)


if __name__ == '__main__':
	print "do you want to publish ? type y for yes"
	response = raw_input()

	# assuming client knows the ip of the accesspoint server
	# access_point = "localhost:50051"
	access_point = "10.145.224.158:50051"
	if response == "y" :
		print "Enter topic"
		topic = raw_input()

		print "Enter data"
		data = raw_input()

    	push_topic(topic,data,access_point)