    # Loop over output files and compile filter efficiencies: 
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

gPath = "/eos/user/n/nipinto/MC/ttH_126/ttHToZZTo4LFilter_slc7_amd64_gcc10_CMSSW_12_4_3_my_ttH_M126"

regex = re.compile("Filter efficiency")
filter_efficiencies = []
nEventlist = []
regex = re.compile("Filter efficiency")
matchedFiles = 0
for i in tqdm(range(0,100), desc="Aggregating filter efficiencies."):
    for LHEFile in glob.glob(gPath+f"/cmsgrid_{i}_final.log"):
            with open (LHEFile, "r") as file: 
                for line in file: 
                    if match :=  regex.search(line): 
                        matchedFiles += 1
                        filter_efficiencies.append(float(line.rstrip().split(" ")[-2]))

avgFilterEfficiency = np.average(filter_efficiencies)
with open("./FilterEfficiency.txt", "w") as outFile: 
        outFile.write(str(avgFilterEfficiency))
    
print("Jobs completed: ", matchedFiles)
print("Overall filter efficiency: ", avgFilterEfficiency)


nTotal = matchedFiles*10000
nPassed = avgFilterEfficiency*nTotal/100
unc = 100*np.sqrt(nPassed)/nTotal

print("unc", unc)