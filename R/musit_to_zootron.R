
# To be run as loop, downloading and storing each dataset named in the vector "dataset" 
# for each iteration

# script stored at bitbucket, run on server as: 
# source("https://git.vm.ntnu.no/users/andersfi/repos/musit_to_ipt/raw/R/musit_to_zootron.R?at=refs%2Fheads%2Fmaster")


# dependencie
library(countrycode)
library(dplyr)
library(dbplyr)
library(RPostgreSQL)
library(stringr)
library(DBI)

# lists of datasets to process ------ 
# (see http://www.unimus.no/nedlasting/datasett/ )

dataset <- c("entomology_ntnuvmti")

# download, clean and upload to db ------
for (i in 1:length(dataset)){

  url <- paste("http://www.unimus.no/nedlasting/datasett/",dataset[i],".gz",sep="")
  tmp <- tempfile()
  download.file(url,tmp)
  # NOTE: dataset is further reffered to as "inndata"
  inndata <- read.csv(gzfile(tmp), sep="\t", header=TRUE, stringsAsFactors=FALSE)
  
  # some cleaning of data, and adding of terms
  inndata$geodeticDatum <- "WGS84" # add term
  inndata$kingdom <- "Animalia" # add term
  inndata$countryCode <- countrycode(inndata$country, 'country.name', 'iso3c') # get country code
  inndata$dateIdentified[inndata$dateIdentified=="0000-00-00"] <- NA
  inndata$eventDate[inndata$eventDate=="0000-00-00"] <- NA
  inndata$eventDate <- stringr::str_replace_all(inndata$eventDate,"-00","")
  inndata$year <- stringr::str_sub(inndata$eventDate,1,4)
  inndata$month <- stringr::str_sub(inndata$eventDate,6,7)
  inndata$month[inndata$month==""] <- NA
  inndata$day <- stringr::str_sub(inndata$eventDate,9,10)
  inndata$day[inndata$day==""] <- NA
  inndata$eventDate <- stringr::str_replace_all(inndata$eventDate,"-00","")
  inndata$dateIdentified <- stringr::str_replace_all(inndata$dateIdentified,"-00","")
  inndata$occurrenceID <- paste("urn:uuid:",inndata$occurrenceID,sep="") # decleare the nature of the identifier by adding urn:uuid at start
  
  # upload data to database 

  con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(), # DB connection
                        dbname="musit_to_ipt")
  dbSendStatement(con,paste("DROP TABLE IF EXISTS", dataset[i])) # delete existing table
  copy_to(con,inndata,paste(dataset[i]),temporary = FALSE) # upload table
  dbSendStatement(con,paste("GRANT SELECT ON", dataset[i], "TO ipt;")) # make sure db user ipt has read access
  dbDisconnect(con) # disconnect from DB

}






