import csv
from datetime import datetime
from datetime import timedelta
from collections import OrderedDict

ipDict = OrderedDict()
endTimeDict = OrderedDict()
directoryPath = './input'
logFilePath = directoryPath + '/log.csv'
inactivityPeriodFilePath = directoryPath + '/inactivity_period.txt'
outputFilePath = './output/sessionization.txt'

#read inactivity_period.txt
inactivityPeriodString = open(inactivityPeriodFilePath, 'r').read()
inactivityTime = int(inactivityPeriodString)

#open output file in write mode
outputFile = open(outputFilePath, 'w')

#read log file and output the result in sessionization.txt
with open(logFilePath, 'r') as csvfile:
        logFile = csv.reader(csvfile)
        next(logFile)
        for row in logFile:
            ip = row[0]
            date = row[1]
            time = row[2]
            entryTime = datetime.strptime(date + "," + time, "%Y-%m-%d,%H:%M:%S")

            if endTimeDict.has_key(entryTime) == False :
               endTime = entryTime - timedelta(seconds=inactivityTime + 1)
               endTimesList = [time for time in endTimeDict.keys() if time <= endTime]
               for sessionTime in endTimesList:
                    lastTimeIPList = endTimeDict[sessionTime]
                    for currentIp in lastTimeIPList:
                        if ipDict.has_key(currentIp):
                            currentIpTimes = ipDict[currentIp]
                            currentIpEndTime = currentIpTimes[len(currentIpTimes) - 1]
                            if currentIpEndTime <= sessionTime :
                                currentIpStartTime = currentIpTimes[0]
                                tempRes = currentIp + "," + str(currentIpStartTime) + "," + str(currentIpEndTime) + "," + str((currentIpEndTime - currentIpStartTime + timedelta(seconds=1)).total_seconds()).split('.')[0] +","+ str(len(currentIpTimes)) + "\n"
                                outputFile.write(tempRes)

                                ipDict.pop(currentIp)

                    endTimeDict.pop(sessionTime)

            ipDict.setdefault(ip, []).append(entryTime)
            endTimeDict.setdefault(entryTime, []).append(ip)


for currentIp , times in ipDict.items() :
    tempRes = currentIp + "," + str(times[0]) + "," + str(times[len(times)-1]) + "," + str((times[len(times)-1] - times[0] + timedelta(seconds=1)).total_seconds()).split('.')[0]  + "," + str(len(times)) + "\n"
    outputFile.write(tempRes)

outputFile.close()








