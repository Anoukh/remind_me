import ballerina/http;
import ballerina/log;
import ballerina/task;
import ballerina/time;
import ballerinax/java.jdbc;
import ballerina/internal;
import ballerina/ 'lang\.string as str;

string INSERT_EVENT_SQL = "INSERT INTO `events` (`date_time`, `message`) VALUES (?, ?)";
string SELECT_NOTIFICATION_SQL = "SELECT `id`, `date_time`, `message` FROM `events` WHERE `date_time` > ? && `date_time` < ? && `notified` = 0";
string SELECT_OLD_NOTIFICATION_SQL = "SELECT `id`, `date_time`, `message` FROM `events` WHERE `date_time` < ? && `notified` = 0";
string UPDATE_NOTIFIED_EVENT_SQL = "UPDATE `events` SET `notified`= 1 WHERE `id` = ?";

jdbc:Client jdbcClient = new ({
    url: "jdbc:mysql://localhost:3306/reminder_db",
    username: "root",
    password: "root",
    poolOptions: {maximumPoolSize: 5},
    dbOptions: {useSSL: false}
});

public function main(string... args) {
    time:Time currentTime = time:currentTime();
    (table<DateTime> | error) result = jdbcClient->select(SELECT_OLD_NOTIFICATION_SQL, DateTime, time:toString(currentTime));
    if (result is error) {
        log:printError("error retrieving data.", result);
        return;
    }

    var dateTime = <table<DateTime>> result;
    foreach var row in <table<DateTime>>result {
        printNotification(row);
    }
}

@http:ServiceConfig {
    basePath: "/reminderService"
}
service reminderService on new http:Listener(9090) {
    @http:ResourceConfig {
        methods: ["POST"],
        path: "/addReminder",
        consumes: ["application/json"]
    }
    resource function addReminder(http:Caller caller, http:Request request) {
        (json | error) jsonPayload = request.getJsonPayload();
        if (jsonPayload is error) {
            log:printError("error in input json.", jsonPayload);
            respond(caller, "error in input json. please re-check input payload");
            return;
        }

        map<json> payload = <map<json>>jsonPayload;
        json date = payload["date_time"];
        json message = payload["message"];

        if !(date is string) {
            log:printError("error: expected a string parameter: date_time");
            respond(caller, "error: expected a string parameter: date_time", 400);
            return;
        }

        if !(message is string) {
            log:printError("error: expected a string parameter: message");
            respond(caller, "error: expected a string parameter: message", 400);
            return;
        }

        // TODO: Validate date_time is a valid date time format and time has not already expired.

        (jdbc:UpdateResult | error) result = jdbcClient->update(INSERT_EVENT_SQL, <string>date, <string>message);
        if (result is error) {
            log:printError("Error", result);
            respond(caller, "error in input json.");
            return;
        }
        respond(caller, "Success");
    }
}

task:TimerConfiguration timerConfig = {
    intervalInMillis: 20000,
    initialDelayInMillis: 0
};

listener task:Listener timer = new (timerConfig);

type DateTime record {|
    int id;
    string date_time;
    string message;
|};

service timerService on timer {
    resource function onTrigger() {
        time:Time currentTime = time:currentTime();
        time:Time oneMinTime = time:addDuration(currentTime, 0, 0, 0, 0, 1, 0, 0);
        (table<DateTime> | error) result = jdbcClient->select(SELECT_NOTIFICATION_SQL, DateTime, time:toString(currentTime), time:toString(oneMinTime));
        if (result is error) {
            log:printError("error retrieving data", result);
            return;
        }

        var dateTime = <table<DateTime>> result;
        foreach var row in <table<DateTime>>result {
            printNotification(row);
        }
    }
}

function printNotification(DateTime dateTime) {
    time:Time | error dTime = time:parse(processDateTime(dateTime.date_time), "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    if (dTime is error) {
        log:printError("error converting datetime.", dTime);
    } else {
        io:println("~~~~~~~~~~~~~~~~~~~~Reminder~~~~~~~~~~~~~~~~~");
        io:println(dateTime.message);
        io:println("~~~~~~~~~~~~~~~~~~~~~~End~~~~~~~~~~~~~~~~~~~~");
        (jdbc:UpdateResult | error) updateResult = jdbcClient->update(UPDATE_NOTIFIED_EVENT_SQL, dateTime.id);
        if (updateResult is error) {
            log:printError("error marking event as notified.", updateResult);
        }
    }
}

// TODO: Write another timer to purge the data from the reminder_db

function processDateTime(string dateTime) returns string {
    int lIndex = internal:lastIndexOf(dateTime, ":");
    string prefix = dateTime.substring(0, lIndex);
    string suffix = dateTime.substring(lIndex + 1, dateTime.length());
    return str:concat(prefix, suffix);
}

function respond(http:Caller caller, json | string payload, int statusCode = 200) {
    http:Response res = new;
    res.statusCode = statusCode;
    res.setJsonPayload(payload, contentType = "application/json");
    error? responseStatus = caller->respond(res);
    if (responseStatus is error) {
        log:printError("error in sending response.", err = responseStatus);
    }
}
