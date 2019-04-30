# To be run as loop, downloading and storing each dataset named in the vector "dataset" 
# for each iteration

# dependencie
library(countrycode)
library(dplyr)
library(dbplyr)
library(RPostgreSQL)
library(stringr)
library(DBI)
library(rio)

# lists of datasets to process ------ 
# (see http://www.unimus.no/nedlasting/datasett/ )

dataset <- c("entomology_ntnuvmti","marine_ntnuvmmi")

# download, clean and upload to db ------
for (i in 1:length(dataset)){

  url <- paste("http://www.unimus.no/nedlasting/datasett/",dataset[i],".gz",sep="")
  tmp <- tempfile()
  download.file(url,tmp)
  # NOTE: dataset is further reffered to as "inndata"
  inndata <- read.csv(gzfile(tmp), sep="\t", header=TRUE, stringsAsFactors=FALSE, quote = "")
  
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
  inndata$db_import_datetime <- Sys.time()
  # upload data to database 

  con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(), # DB connection
                        dbname="musit_to_ipt", options="-c search_path=public")
  dbSendStatement(con,paste("DROP TABLE IF EXISTS", dataset[i])) # delete existing table
  copy_to(con,inndata,paste(dataset[i]),temporary = FALSE) # upload table
  dbSendStatement(con,paste("ALTER TABLE ", dataset[i], " ADD import_id SERIAL PRIMARY KEY;")) # make the table content readable accross database platform. OBS! import_id is not persistent it is replace at every import.
  dbSendStatement(con,paste("ALTER TABLE ", dataset[i], " RENAME \"dcterms.modified\" TO modified;")) # make field name readable in other system that does not support field name with point.
  dbSendStatement(con,paste("ALTER TABLE ", dataset[i], " ADD send_to_ipt boolean DEFAULT(TRUE);")) # create a filter to for sending true data to the ipt
  dbSendStatement(con,paste("update ", dataset[i], " set \"basisOfRecord\" = 'PreservedSpecimen' 
        where lower(\"basisOfRecord\") like '%preserved%' and lower(\"basisOfRecord\") not like 'preservedspecimen';")) # update basisOfRecord for misspelling
  dbSendStatement(con,paste("update ", dataset[i], " set send_to_ipt = FALSE 
        where position(lower(\"basisOfRecord\") in lower('PreservedSpecimen|FossilSpecimen|LivingSpecimen|HumanObservation|MachineObservation|MaterialSample|Occurrence'))=0;")) # exclude record for export to ipt where basisOfRecord is not normalized according to the DcW vocabulary.
  dbSendStatement(con,paste("with double_rec_temp as (
        select \"occurrenceID\", count(\"collectionCode\") as n from ", dataset[i], " group by \"occurrenceID\")
        update ", dataset[i], " set send_to_ipt = FALSE
        from (select * from double_rec_temp where n > 1) dbl
	      where dbl.n > 1 and dbl.\"occurrenceID\"=", dataset[i], ".\"occurrenceID\";")) # exclude record to export to ipt when occurrenceID occures more than one
  dbSendStatement(con,paste("GRANT SELECT ON", dataset[i], "TO ipt;")) # make sure db user ipt has read access
  dbSendStatement(con,paste("GRANT SELECT ON", dataset[i], "TO natron_guest;")) # make sure db user natron_guest has read access
  dbDisconnect(con) # disconnect from DB

}






