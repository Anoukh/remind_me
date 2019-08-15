import ballerina/http;
import ballerina/io;
import ballerina/log;
import ballerina/time;

public function main() {
    addReminder();
}

http:Client notifierEP = new ("http://localhost:9090/reminderService");

function addReminder() {
    string message = io:readln("Enter the Reminder Message: ");
    io:println("Enter the reminder date details: ");
    string year = io:readln("Year YYYY: ");
    string month = io:readln("Month MM: ");
    string day = io:readln("Day DD: ");

    io:println("Enter the reminder time details: ");
    string hour = io:readln("Hour HH: ");
    string minute = io:readln("Minute MM: ");

    string finalDateTime = year + "-" + month + "-" + day + " " + hour + ":" + minute + ":00";

    http:Request req = new ();
    map<json> payload = {"date_time": finalDateTime, "message": message};
    (http:Response | error) post = notifierEP->post("/addReminder", <@untianted>payload);
    if (post is error) {
        log:printError("error adding event. try again.", post);
    } else {
        log:printInfo("successfully addded reminder!");
    }
}
