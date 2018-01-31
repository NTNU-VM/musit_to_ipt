# Workflow for publishing MUSIT data to GBIF through IPT

## Procedure

1. Download data from the [MUSIT dump](http://www.unimus.no/nedlasting/datasett/)
2. intial transformation and recoding (to be updated regularely based upon changes in the musit-dump)
3. Store transformed table to the NaTron PostgreSQL cluster on database musit_to_ipt 
4. Export from NaTron to IPT. Set IPT to dayily autopublishing

Step 1-3 run in R script musit_to_zootron (in R folder of this repro). R script runs as nightly cronjob by calling this script on this repository: R -e 'source("https://raw.githubusercontent.com/NTNU-VM/musit_to_ipt/master/R/musit_to_zootron.R")'. Each dataset, identified by link to .csv file runs sepparately. Update datasets by updating the 'dataset' vector in the 'lists of datasets to process' section of the R script.  

Step 4 occurres internally on the IPT. 

### Daily update at 23:00
