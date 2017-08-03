# Workflow for publishing MUSIT data through IPT

## Procedure

1. Download data from the [MUSIT dump](http://www.unimus.no/nedlasting/datasett/)
2. intial transformation and recoding 
3. Store transformed table to the zootron PostgreSQL cluster on database musit_to_ipt 
4. Import to IPT from NaTron. Set IPT to dayily autopublishing

Step 1-3 run in R script musit_to_zootron (in R folder of this repro). R script runs as nightly crontab job by calling this script on this repository: R -e 'source("https://git.vm.ntnu.no/projects/MUS/repos/musit_to_ipt/raw/R/musit_to_zootron.R?at=refs%2Fheads%2Fmaster")'. Each dataset, identified by link to .csv file runs sepparately. Update datasets by updating the 'dataset' vector in the 'lists of datasets to process' section of the R script.  

Step 4 occurres internally on the IPT. 