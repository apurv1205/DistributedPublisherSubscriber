# distributedPubSub
Distributed systems term project for distributed systems course in IIT Kharagpur

To run on different systems in the institute network, type : (disables proxy in current terminal session)
. env.sh
in all the systems where the scripts are run

To generate proto files : 
python -m grpc_tools.protoc -I proto/ --python_out=. --grpc_python_out=. pr.proto

To clean the dataBackup directory:
. clean.sh
