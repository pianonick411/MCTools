# MCTools
If installing for the first time on LXPlus, you must set up a virtual environment to install the htcondor, classad, and tqdm libraries: 

`python3.9 -m venv myenv`
`source myenv/bin/activate`
`pip install htcondor; pip install classad; pip install tqdm`

You will need to activate the environment with `source myenv/bin/activate` every new session. I do not recommend initializing a cmsenv while working in this repository. 