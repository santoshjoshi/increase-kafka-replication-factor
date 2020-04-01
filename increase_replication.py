import subprocess
import sys, getopt
import json

zkhosts = ''
kafkadir = ''
logfile = ''

def verify_replicas(filename):

    command_list = [
        kafkadir+"/bin/kafka-reassign-partitions.sh",
        '--zookeeper',
        zkhosts,
        '--reassignment-json-file',
        filename,
        '--verify'
    ]

    kafka_process = subprocess.Popen(command_list, stdout=subprocess.PIPE)
    output = kafka_process.communicate()[0]

    return output

def add_replicas(filename):
    #subprocess.call('unset JMX_PORT', shell=True)
    command_list = [
        kafkadir+'/bin/kafka-reassign-partitions.sh',
        '--zookeeper',
        zkhosts,
        '--reassignment-json-file',
        filename,
        '--execute'
    ]

    kafka_process = subprocess.Popen(command_list, stdout=subprocess.PIPE)
    output = kafka_process.communicate()[0]

    return output

def get_raw_current_config(topic):
    output = subprocess.check_output(
        [kafkadir+'/bin/kafka-topics.sh', '--describe', '--zookeeper', zkhosts, '--topic', topic])

    return output

def get_existing_topic_config(topicname):

    raw_topic_details = get_raw_current_config(topicname)
    topics = {}

    for line in raw_topic_details.splitlines():
        if line == line.lstrip():  # this is the header for the topic
            line_split = line.split()
            topic = ''
            for topic_config_string in line_split:
                split_config_string = topic_config_string.split(':')
                if split_config_string[0] == 'Topic':
                   topics[split_config_string[1]] = {'partitions': []}
                   topic = split_config_string[1]
                if split_config_string[0] == 'Configs':
                    if topic != '':
                        config = {}
                        if split_config_string[-1]:
                            for config_string in split_config_string[1].split(","):
                                config_string_split = config_string.split("=")
                                config[
                                    config_string_split[0]
                                ] = config_string_split[
                                    1
                                ]
                        topics[topic]['Configs'] = config
                    else:
                        raise KeyError('Topic name not found')
                else:
                    if topic != '':
                       topics[topic][
                            split_config_string[0]
                        ] = split_config_string[1]
                    else:
                        raise KeyError('Topic name not found')
        else:  
            line_split = line.split()
            line_split_filtered = filter(lambda x: ':' not in x, line_split)
            if line_split_filtered[0] not in topics.keys():
                raise KeyError('Topic name not found')

            partition = {
                'topic': line_split_filtered[0],
                'partition': int(line_split_filtered[1]),
                'replicas': [int(d) for d in line_split_filtered[(3 if len(line_split_filtered) == 5 else 2)].split(',')]
            }
            topics[partition['topic']]['partitions'].append(partition)

    return topics

def main(argv):

    kafkapath = ''
    zkdetails = ''
    topicname = ''
    operation = ''

    try:
        opts, args = getopt.getopt(argv, "hk:zk:t:ops:b:f", ["kafka=","zookeeper=", "topic=", "operation=", "brokers=", "replicafile="])
    except getopt.GetoptError:
        print 'increase_replication.py -i -k <kafka> -z <zookeeper> -t <topicname> -o <operation>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'increase_replication.py -zk zookeeper -t topicname '
            sys.exit()
        elif opt in ("-zk", "--kafka"):
            kafkapath = arg
        elif opt in ("-zk", "--zookeeper"):
            zkdetails = arg
        elif opt in ("-ops", "--operation"):
            operation = arg
        elif opt in ("-t", "--topic"):
            topicname = arg

    print 'Kafka installation directory ', kafkapath
    print 'Zookeeper connection string ', zkdetails
    print 'Topic name ', topicname
    print 'Operation ', operation

    logfile.write("Topic name is {}\n".format(topicname))
    logfile.write("Operation is {}\n".format(operation))

    global zkhosts, kafkadir
    zkhosts = zkdetails
    kafkadir = kafkapath

    if operation == 'currentconfig':
        topicsjson = get_existing_topic_config(topicname)
        print("--------------------------------------------------------------------------------------------------")
        print("--------------------------------------------------------------------------------------------------")
        print(json.dumps(topicsjson))
        print("--------------------------------------------------------------------------------------------------")

        logfile.write("\n")
        logfile.write(json.dumps(topicsjson))
        logfile.write("\n")

    elif operation == 'newconfig':

        if opt in ("-b", "--brokers"):
            brokers = arg
            topicsjson = get_existing_topic_config(topicname)
            logfile.write("Existing configuration is \n\n ")
            logfile.write(json.dumps(topicsjson))
            print("Existing configuration is ")
            print(json.dumps(json.dumps(topicsjson)))

            topics_new_config = {}

            topics_new_config["version"] = 1
            topics_new_config["partitions"] = []
            #print('partitions ', topicsjson[topicname])
            for key in topicsjson[topicname]:
                if key == 'partitions':
                    existing_partion_value = topicsjson[topicname][key]
                    for partition in existing_partion_value:
                        existing_brokers = partition["replicas"]

                        for newBroker in brokers.split(','):
                            #print "new Broker", newBroker
                            if int(newBroker) not in existing_brokers:
                                existing_brokers.append(int(newBroker))

                        topics_new_config["partitions"].append(partition)

            print("--------------------------------------------------------------------------------------------------")
            print("-----COPYING BELOW JSON TO NEW FILE at /tmp/{}_replica_change.json------------------".format(topicname))
            print("--------------------------------------------------------------------------------------------------")
            print(json.dumps(topics_new_config))
            print("--------------------------------------------------------------------------------------------------")
            print("--------------------------------------------------------------------------------------------------")

            f = open("/tmp/{}_replica_change.json".format(topicname), "w+")
            f.write(json.dumps(topics_new_config))
            f.close()

            logfile.write("\nNew Configuration is \n\n")
            logfile.write(json.dumps(topics_new_config))
            logfile.write("\n")

        else:
            print('no brokers')

    elif operation == 'increasereplica':
        if opt in ("-f", "--replicafile"):
            increase_replica_file_name = arg

        else:
            increase_replica_file_name= "/tmp/{}_replica_change.json".format(topicname)
            print('File name not provided, taking default file {}', format(increase_replica_file_name))


        print('Increase replication file name is {}'.format(increase_replica_file_name))
        addreplica_result = add_replicas(increase_replica_file_name)
        print("--------------------------------------------------------------------------------------------------")
        print("--------------------------------------------------------------------------------------------------")
        print ("Add Replica Result {}".format(addreplica_result))
        print("--------------------------------------------------------------------------------------------------")
        print("--------------------------------------------------------------------------------------------------")

        logfile.write("\n")
        logfile.write(json.dumps(addreplica_result))
        logfile.write("\n")

    elif operation == 'verify':
        if opt in ("-f", "--replicafile"):
            print('Verifying result')
            increase_replica_file_name = arg
        else:
            increase_replica_file_name = "/tmp/{}_replica_change.json".format(topicname)
            print('File name not provided, taking default file {}', format(increase_replica_file_name))

        print('Increase replication file name is {}'.format(increase_replica_file_name))
        verify = verify_replicas(increase_replica_file_name)
        print("--------------------------------------------------------------------------------------------------")
        print("--------------------------------------------------------------------------------------------------")
        print ("Verify result {} ".format(verify))
        print("--------------------------------------------------------------------------------------------------")
        print("--------------------------------------------------------------------------------------------------")

        logfile.write("\n")
        logfile.write(json.dumps(verify))
        logfile.write("\n")

    else:
        print('No operation specified')

if __name__ == "__main__":
    logfile = open("/tmp/replica.log", "a+")
    main(sys.argv[1:])
    logfile.close()
