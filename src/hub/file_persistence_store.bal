import ballerina/log;
import ballerina/websub;
import ballerina/filepath;
import ballerina/system;
import ballerina/lang.'int as ints;

public const string SUBSCRIPTION_LOG_FILE = "websub_subscription.csv";
public const string TOPIC_LOG_FILE = "websub_topic.csv";
# Description
type FilePersistenceStore object {
    
    *websub:HubPersistenceStore;
    string subsLogFilepath = "";
    string topicLogFilepath = "";

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
    }

    public function addSubscription(websub:SubscriptionDetails subscriptionDetails) {
        io:println("Add subscription");
        io:println(subscriptionDetails);
        io:WritableTextRecordChannel subsWritableChannel = checkpanic self.getWritableRecordChannel(
                                                                            self.subsLogFilepath, "UTF-8", ",", "\n");
        string[] fieldArray = [];
        foreach var [k, v] in subscriptionDetails.entries() {
            fieldArray.push(k + ":" + v.toString());
        }
        var result = subsWritableChannel.write(fieldArray);
        if (result is error) {
            log:printError("Error while writing subscription log file", result);
        }
        checkpanic subsWritableChannel.close();
    }

    public function removeSubscription(websub:SubscriptionDetails subscriptionDetails) {
        log:printInfo("Remove subscription");
        log:printInfo(subscriptionDetails.toString());
    }

    public function addTopic(string topic) {
        io:WritableTextRecordChannel topicWritableChannel = checkpanic self.getWritableRecordChannel(
                                                                            self.topicLogFilepath, "UTF-8", ",", "\n");
        log:printInfo("Add topic: " + topic);
        string[] fieldArray = [topic];
        var result = topicWritableChannel.write(fieldArray);
        if (result is error) {
            log:printError("Error while writing subscription log file", result);
        }
        checkpanic topicWritableChannel.close();
    }

    public function removeTopic(string topic) {
        io:println("Remove topic: " + topic);
    }

    public function retrieveTopics() returns (string[]) {
       log:printInfo("Retrieving all topics");
       io:ReadableTextRecordChannel topicReadableChannel = checkpanic self.getReadableRecordChannel(
                                                                            self.topicLogFilepath, "UTF-8", "\n", ",");
        string[] topics = [];
        while(topicReadableChannel.hasNext()) {
            var records = topicReadableChannel.getNext();
            if (records is error) {
                log:printError("Error while reading topic log file", records);
            } else {
                topics.push(records.pop());
            }
        }
        checkpanic topicReadableChannel.close();
        return topics;
    }

    public function retrieveAllSubscribers() returns (websub:SubscriptionDetails[]) {
        log:printInfo("Retrieving all subscribers");
        io:ReadableTextRecordChannel subsReadableChannel = checkpanic self.getReadableRecordChannel(
                                                                            self.subsLogFilepath, "UTF-8", "\n", ",");
        websub:SubscriptionDetails[] subscriptions = [];
        while(subsReadableChannel.hasNext()) {
            var records = subsReadableChannel.getNext();
            io:println(records);
            websub:SubscriptionDetails details = {};
            if (records is error) {
                log:printError("Error while reading subscription log file", records);
            } else {
                foreach string field in records {
                    int? index = field.indexOf(":");
                    if(index is int) {
                        string key = field.substring(0, index);
                        string value;
                        if (field.endsWith("\n")) {
                            value = field.substring(index+1, field.length()-1);
                        } else {
                            value = field.substring(index+1, field.length());
                        }
                        log:printInfo("field " + key + ":" + value);
                        if (key == "leaseSeconds" || key == "createdAt") {
                            int|error intValue = ints:fromString(value);
                            if (intValue is error) {
                                log:printError("Error while converting the string value", intValue);
                            } else {
                                details[key] = intValue;
                            }
                        } else {
                            details[key] = value;
                        }
                    }
                }
                log:printInfo("Retrieved subscriber: " + details.toString());
                subscriptions.push(details);
            }
        }
        checkpanic subsReadableChannel.close();
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

};