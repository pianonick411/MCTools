import glob
import argparse
import os
import htcondor
import classad
import random
import math
import sys
import tarfile
import pathlib
from tqdm import tqdm
import time
from datetime import datetime
import getpass
import re
import numpy as np

def openGridpack(gridPackFile, odir, chunkNum, subFrom): 
    if not os.path.exists(odir): 
        print(odir, "not found! Making ", odir)
        os.makedirs(odir, mode=0o777)
        os.chmod(odir, 0o777)
    gPackName = gridPackFile.split("/")[-1].rstrip(".tgz")
    gpath = odir+"/"+gPackName
    if not os.path.exists(gpath): 
        os.makedirs(gpath, mode=0o777)
        os.chmod(gpath, 0o777)
    if os.path.exists(gpath+"/runcmsgrid.sh"): 
        print("runcmsgrid.sh already found!")
    else: 
        with tarfile.open(gridPackFile) as tar: 
            tar.extractall(path=gpath, filter="fully_trusted")
    if not os.path.exists(subFrom): 
        print(subFrom, " not found! Making ", subFrom)
        os.makedirs(subFrom, mode=0o777)

    with open(f"{subFrom}/submit_Chunk_{chunkNum}.sh", "w") as f:
        output = f"""#!/bin/bash
cd {gpath}
cp runcmsgrid.sh runcmsgrid_{chunkNum}.sh
sed -i 's/cmsgrid/cmsgrid_{chunkNum}/g' runcmsgrid_{chunkNum}.sh
sed -i 's/${{cmssw_version}}\/s/$LHEWORKDIR\/${{cmssw_version}}\/s/g' runcmsgrid_{chunkNum}.sh 
sed -i 's/myDir=powhegbox/myDir=powhegbox_{chunkNum}/g' runcmsgrid_{chunkNum}.sh
./runcmsgrid_{chunkNum}.sh $1 $2 $3"""
        f.writelines(output) 
    os.chmod(f"{subFrom}/submit_Chunk_{chunkNum}.sh", 0o777)


def job_status_string(code: int) -> str:
    """Convert Condor JobStatus int to human-readable string."""
    return {
        1: "Idle",
        2: "Running",
        3: "Removed",
        4: "Completed",
        5: "Held",
        6: "TransferringOutput",
        7: "Suspended"
    }.get(code, f"Unknown({code})")



def wait_for_completion(cluster_id, poll_interval=30, diag_log="job_monitor.log"):
    schedd = htcondor.Schedd()
    seen_status = {}

    with open(diag_log, "w") as f:
        f.write(f"Monitoring cluster {cluster_id}\n")
        f.write(f"Start time: {datetime.now()}\n\n")

        while True:
            jobs = list(schedd.query(
                constraint=f"ClusterId == {cluster_id}",
                projection=["ClusterId", "ProcId", "JobStatus", "HoldReason", "ExitCode"]
            ))

            if not jobs:
                f.write(f"[{datetime.now()}] All jobs are gone from the queue.\n")
                print(f"Cluster {cluster_id} finished. See {diag_log} for details.")
                break


            for job in jobs:
                jid = f"{job['ClusterId']}.{job['ProcId']}"
                status = job_status_string(job["JobStatus"])
                hold_reason = job.get("HoldReason", "")
                exit_code = job.get("ExitCode", None)

                # Log only when status changes
                if seen_status.get(jid) != status:
                    line = f"[{datetime.now()}] Job {jid} -> {status}"
                    if hold_reason:
                        line += f" | HoldReason: {hold_reason}"
                    if exit_code is not None:
                        line += f" | ExitCode: {exit_code}"
                    line += "\n"

                    f.write(line)
                    f.flush()
                    print(line.strip())

                seen_status[jid] = status

            time.sleep(poll_interval)







def main(raw_args=None): 
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--ifile', type=str, required=True, help="Input gridpack.")
    parser.add_argument('-o', '--odir', type=str, required=True, help=" Directory where output is dumped.")
    parser.add_argument('-N', '--NEvents', type=int, default=1000000, help=" Total number of events to simulate. Default = 1000000.")
    parser.add_argument('-s', '--subfrom', type=str, default='./', help= "The directory from which the executable is stored ")
    parser.add_argument('-S', '--Size', type=int, default=10000, help=" Size of each chunk. Default = 10000.")
    args = parser.parse_args(raw_args)

    print("**CalcLHEFilterEff")
    inputGridpack = args.ifile
    outputdir = os.path.abspath(args.odir)
    NEvents = args.NEvents
    chunkSize = args.Size
    

    n_jobs_raw, remainder = divmod(NEvents, chunkSize)
    if remainder == 0: 
        n_jobs = n_jobs_raw
    else: 
        n_jobs = n_jobs_raw + 1 
    

    col = htcondor.Collector()
    credd = htcondor.Credd()
    credd.add_user_cred(htcondor.CredTypes.Kerberos, None)

    
    

    seedList = random.sample(range(1, 1000000), n_jobs)

    gPackName = inputGridpack.split("/")[-1].rstrip(".tgz")
    gPath = outputdir+"/"+gPackName

    subFrom = args.subfrom + f"submit_{gPackName}"
    nCpus = 2

   
    
    itemdata = []
    nEventlist = []
    for i in tqdm(range(0, n_jobs), desc="Gridpack dirs opened"):
        if i == 0: 
            if remainder != 0: 
                # First job has the smaller size of the remainder, the rest of the jobs have the size specified by chunkSize.
                itemdata.append({"chunk_arguments": f"{remainder} {seedList[i]} {nCpus}"})
                nEventlist.append(int(remainder))
            else: 
                itemdata.append({"chunk_arguments": f"{chunkSize} {seedList[i]} {nCpus}"})
                nEventlist.append(int(chunkSize))
        else: 
            itemdata.append({"chunk_arguments": f"{chunkSize} {seedList[i]} {nCpus}"})
            nEventlist.append(int(chunkSize))
        
        openGridpack(inputGridpack, outputdir, i, subFrom)

         
    
    print(nEventlist)

    jobs = htcondor.Submit(
        {
            "executable": f"{subFrom}/submit_Chunk_$(ProcId).sh", 
            "arguments": "$(chunk_arguments)", 
            "output": f"{subFrom}/job_$(ProcId).out",
            "error": f"{subFrom}/job_$(ProcId).err",
            "log": f"{subFrom}/log.log",
            "request_memory": "4000M",
            "+JobFlavour": '"nextweek"',
            "periodic_remove": "JobStatus == 5",
            "should_transfer_files": "YES",
            'MY.SendCredential': "True", 
            "WhenToTransferOutput": "ON_EXIT",

        }
    )    

    print(jobs)
    
    schedd = htcondor.Schedd()
    submit_result = schedd.submit(jobs, itemdata = iter(itemdata))  # submit one job for each item in the itemdata

    cluster_id = submit_result.cluster()
    print("Cluster ID: ", submit_result.cluster())
    
    wait_for_completion(cluster_id, poll_interval=20)

    # Loop over output files and compile filter efficiencies: 
    filter_efficiencies = []
    matchedFiles = 0 
    regex = re.compile("Filter efficiency")
    for i in tqdm(range(0,n_jobs), desc="Aggregating filter efficiencies."):
        for LHEFile in glob.glob(gPath+f"/cmsgrid_{i}_final.log"):
                with open (LHEFile, "r") as file: 
                    for line in file: 
                        if match :=  regex.search(line): 
                            matchedFiles += 1
                            filter_efficiencies.append(float(line.rstrip().split(" ")[-2]))


    avgFilterEfficiency = np.average(filter_efficiencies)
    nTotal = matchedFiles*chunkSize
    nPassed = avgFilterEfficiency*nTotal/100
    unc = 100*np.sqrt(nPassed)/nTotal
    

    print("Overall filter efficiency: ", avgFilterEfficiency, " ± ", unc)
    with open(outputdir+"/FilterEfficiency.txt", "w") as outFile: 
        outFile.write(str(avgFilterEfficiency))
    
    



if __name__ == "__main__":
    main(sys.argv[1:])