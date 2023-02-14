import sys

worklaodName = sys.argv[1]
threadIDList = [i for i in range(8)]
coroIDList = [i for i in range(10)]
workloadOpsDict = {
    "workloada": ["search", "update"],
    "workloadb": ["search", "update"],
    "workloadc": ["search"],
    "workloadd": ["search", "insert"]
}
fnameTemplate = 'results/ycsb_{}_lat_{}_{}.txt'
outFnameTemplate = 'results/{}_{}_lat.txt'

opsList = workloadOpsDict[worklaodName]
for op in opsList:
    mergedLines = []
    for tid in threadIDList:
        for cid in coroIDList:
            fname = fnameTemplate.format(op, tid, cid)
            tmpFile = open(fname, "r")
            lines = tmpFile.readlines()
            mergedLines += lines
            tmpFile.close()
    outFname = outFnameTemplate.format(worklaodName, op)
    print("merging {}".format(outFname))
    outF = open(outFname, "w")
    outF.writelines(mergedLines)
    outF.close()