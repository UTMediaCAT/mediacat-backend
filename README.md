[![Build Status](https://travis-ci.org/UTMediaCAT/mediacat-backend.svg?branch=master)](https://travis-ci.org/UTMediaCAT/mediacat-backend)
# mediacat-backend

## Post Processor Usage
```
cd Post-Processor
python3 processor.py
```

Required files and folder structure within Post-Processor directory:
- DomainOutput: holds all domain crawler output files
- TwitterOutput: holds all twitter crawler output files
- input_scope_final.csv: this is the scope file the processor reads from 
- Output: a folder to hold the output of the processor, including output.json and interest_output.json (can be empty prior to running)
- Saved: a folder to hold saved intermediate states of files (can be empty)
- logs: a folder to hold logs (can be empty)

The output from the previous run of the processor is in /Post-Processor/output.json

Note: read_from_memory flag is can be manually turned on and off on processor.py main. If picking up the processor from a previous break, then run the program with read_from memory set to True.

