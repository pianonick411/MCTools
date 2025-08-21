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

def openGridpack(gridPackFile, odir, chunkNum): 
    if not os.path.exists(odir): 
        print(odir, "not found! Making ", odir)
        os.makedirs(odir, mode=0o777)
    gPackName = gridPackFile.split("/")[-1].rstrip(".tgz")
    gpath = odir+"/"+gPackName+"_"
    # We have to make a directory for each job, because we don't want to have the jobs overwriting each others' output. 
    # In theory, you could modify runcmsgrid.sh to take a command line argument with a new name for each job and only make one directory, but the structure of runcmsgrid.sh isn't consistent across gridpacks, so this would be exceptionally difficult. 
    if not os.path.exists(gpath+str(chunkNum)): 
        os.makedirs(gpath+str(chunkNum), mode=0o777)
    with tarfile.open(gridPackFile) as tar: 
        tar.extractall(path=gpath+str(chunkNum), filter="fully_trusted")
    





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

    
    

    seedList = random.sample(range(1, 1000000), n_jobs)

    gPackName = inputGridpack.split("/")[-1].rstrip(".tgz")
    gPath = outputdir+"/"+gPackName+"_"
    
    itemdata = []
    for i in tqdm(range(0, n_jobs), desc="Gridpack dirs opened"):
        if i == 0: 
            if remainder != 0: 
                # First job has the smaller size of the remainder, the rest of the jobs have the size specified by chunkSize.
                itemdata.append({"chunk_arguments": f"{remainder} {seedList[i]} 1"})
            else: 
                itemdata.append({"chunk_arguments": f"{chunkSize} {seedList[i]} 1"})
        else: 
            itemdata.append({"chunk_arguments": f"{chunkSize} {seedList[i]} 1"})
        
        openGridpack(inputGridpack, outputdir, i)

         
    


    jobs = htcondor.Submit(
        {
            # "executable": f"{gPath}$(ProcId)/runcmsgrid.sh", 
            "executable": f"/bin/cat",
            # "arguments": "$(chunk_arguments)", 
            "arguments": "/afs/cern.ch/user/n/nipinto/private/MCTools/README.md",
            "output": f"{outputdir}/job_$(ProcId).out",
            "error": f"{outputdir}/job_$(ProcId).err",
            "log": f"{outputdir}/log.log",
            "request_memory": "4000M",
            "+JobFlavour": "nextweek",
            "periodic_remove": "JobStatus == 5",
            "WhenToTransferOutput": "ON_EXIT_OR_EVICT",
        }
    )    

    # print(jobs)
    
    schedd = htcondor.Schedd()
    submit_result = schedd.submit(jobs, itemdata = iter(itemdata))  # submit one job for each item in the itemdata

    print(submit_result.cluster())
    
    
    



if __name__ == "__main__":
    main(sys.argv[1:])