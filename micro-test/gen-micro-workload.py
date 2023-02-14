import sys

insertTemplate = "INSERT usertable {}\n"
updateTemplate = "UPDATE usertable {}\n"
searchTemplate = "READ usertable {}\n"
deleteTemplate = "DELETE usertable {}\n"

workloadNameTemplate = "workload{}.spec_trans"
workloadNameList = ["ins", "upd", "rea", "del"]
templateDict = {
    "ins": insertTemplate, 
    "upd": updateTemplate, 
    "rea": searchTemplate, 
    "del": deleteTemplate
}

workloadSize = int(sys.argv[1])

for wl in workloadNameList:
    wlName = "micro-workloads/" + workloadNameTemplate.format(wl)
    lineTemplate = templateDict[wl]
    lineList = []
    for key in range(workloadSize):
        line = lineTemplate.format(key)
        lineList.append(line)
    of = open(wlName, "w")
    of.writelines(lineList)
    of.close()