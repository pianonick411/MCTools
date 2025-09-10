# MCTools
CalcLHEFilterEff: Command Line Arguments 
- "-i" input gridpack, should come from the fragment on McM. 
- "-o" the directory where output will be written. EOS is recommended. 
- "s" the directory from which jobs will be submitted. Facilitates submitting on AFS but writing to EOS. 
- "N" the number of events to request. 
- "S" the size (number of events) per job. 

This program will calculate the filter efficiency on its own. If it is killed, the script FilterEfficiencyPost.py can be run. It is currently set for the default job size of 10000. 

In either case, the filter efficiency is computed by finding the number of jobs that finished and averaging their filter efficiencies. The uncertainty takes into account the actual number of events that were simulated and not the requested number, as to keep this value accurate. 

For the recent ttH jobs, 100 jobs of 10000 events were run. Not all jobs finished, however, so the total number of simulated events is different from the requested number (1 million). As stated above, the uncertainty takes this into account, properly reflecting a smaller number of requested events. 