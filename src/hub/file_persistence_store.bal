import ballerina/log;
import ballerina/websub;
import ballerina/filepath;
import ballerina/system;

public const string SUBSCRIPTION_LOG_FILE = "websub_subscription.csv";
public const string TOPIC_LOG_FILE = "websub_topic.csv";
# Description
type FilePersistenceStore object {
    
    *websub:HubPersistenceStore;
    io:WritableTextRecordChannel subsWritableChannel;
    io:WritableTextRecordChannel topicWritableChannel;

    io:ReadableTextRecordChannel subsReadableChannel;
    io:ReadableTextRecordChannel topicReadableChannel;

    public function __init(string directoryPath) {
        if (!system:exists(directoryPath)) {
            _ = checkpanic system:createDir(directoryPath, true);
        } else {
            var fileInfo = system:getFileInfo(directoryPath);
            if (fileInfo is error) {
                error err = fileInfo;
                panic err;
            } else {
                if (!fileInfo.isDir()) {
                    panic error("Invalid directory path. Path " + directoryPath + " is not a directory.");
                }
            }
        }

        string subsLogFilepath = checkpanic filepath:build(directoryPath, SUBSCRIPTION_LOG_FILE);
        string topicLogFilepath = checkpanic filepath:build(directoryPath, TOPIC_LOG_FILE);
        self.subsWritableChannel = checkpanic self.getWritableRecordChannel(subsLogFilepath, "UTF-8", ",", "\\n");
        self.topicWritableChannel = checkpanic self.getWritableRecordChannel(topicLogFilepath, "UTF-8", ",", "\\n");

        self.subsReadableChannel = checkpanic self.getReadableRecordChannel(subsLogFilepath, "UTF-8", ",", "\\n");
        self.topicReadableChannel = checkpanic self.getReadableRecordChannel(topicLogFilepath, "UTF-8", ",", "\\n");
    }

    public function addSubscription(websub:SubscriptionDetails subscriptionDetails) {
        io:println("Add subscription");
        io:println(subscriptionDetails);
        string[] fieldArray = [];
        foreach var [k, v] in subscriptionDetails.entries() {
            fieldArray.push(k + ":" + v.toString());
        }
        var result = self.subsWritableChannel.write(fieldArray);
        if (result is error) {
            log:printError("Error while writing subscription log file", result);
        }
    }

    public function removeSubscription(websub:SubscriptionDetails subscriptionDetails) {
        io:println("Remove subscription");
        io:println(subscriptionDetails);
    }

    public function addTopic(string topic) {
        io:println("Add topic: " + topic);
        string[] fieldArray = [topic];
        var result = self.topicWritableChannel.write(fieldArray);
        if (result is error) {
            log:printError("Error while writing subscription log file", result);
        }
    }

    public function removeTopic(string topic) {
        io:println("Remove topic: " + topic);
    }

    public function retrieveTopics() returns (string[]) {
        string[] topics = [];
        while(self.topicReadableChannel.hasNext()) {
            var records = self.topicReadableChannel.getNext();
            if (records is error) {
                log:printError("Error while reading topic log file", records);
            } else {
                topics.push(records.pop());
            }
        }
        return topics;
    }

    public function retrieveAllSubscribers() returns (websub:SubscriptionDetails[]) {
        websub:SubscriptionDetails[] subscriptions = [];
        while(self.subsReadableChannel.hasNext()) {
            var records = self.subsReadableChannel.getNext();
            websub:SubscriptionDetails details = {};
            if (records is error) {
                log:printError("Error while reading subscription log file", records);
            } else {
                foreach string field in records {
                    int? index = field.indexOf(":");
                    if(index is int) {
                        string key = field.substring(0, index-1);
                        string value = field.substring(index+1, field.length());

                        // if (key == "leaseSeconds" || key == "createdAt") {
                        //     details[key] = value;
                        // }
                        details[key] = value;
                    }
                }
                subscriptions.push(details);
            }
        }
        
        return subscriptions;
    }


    function getReadableRecordChannel(string filePath, string encoding, string rs, string fs)
                                            returns (io:ReadableTextRecordChannel|error) {
        io:ReadableByteChannel byteChannel = check io:openReadableFile(filePath);
        io:ReadableCharacterChannel characterChannel = new(byteChannel, encoding);
        io:ReadableTextRecordChannel delimitedRecordChannel = new(characterChannel,
                                                                rs = rs,
                                                                fs = fs);
        return <@untained> delimitedRecordChannel;
    }

    function getWritableRecordChannel(string filePath, string encoding, string rs, string fs) returns (io:WritableTextRecordChannel|error) {
        io:WritableByteChannel byteChannel = check io:openWritableFile(filePath, true);
        io:WritableCharacterChannel characterChannel = new(byteChannel, encoding);
        io:WritableTextRecordChannel delimitedRecordChannel = new(characterChannel,
                                                                rs = rs,
                                                                fs = fs);
        return <@untained> delimitedRecordChannel;
    }

    // function getReadableCharacterChannel(string filePath, string encoding)
    //                                         returns (io:ReadableCharacterChannel|error) {
    //     io:ReadableByteChannel byteChannel = check io:openReadableFile(filePath);
    //     io:ReadableCharacterChannel characterChannel = new(byteChannel, encoding);
    //     return <@untained> characterChannel;
    // }

    // function getWritableCharacterChannel(string filePath, string encoding) returns (io:WritableCharacterChannel|error) {
    //     io:WritableByteChannel byteChannel = check io:openWritableFile(filePath, true);
    //     io:WritableCharacterChannel characterChannel = new(byteChannel, encoding);
    //     return <@untained> characterChannel;
    // }

};