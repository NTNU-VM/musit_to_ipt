# To be run as loop, downloading and storing each dataset named in the vector "dataset" 
# for each iteration

# dependencie
#library(countrycode) the function is disabled due to 10000 byte limitation
library(dplyr)
library(dbplyr)
library(RPostgreSQL)
library(stringr)
library(DBI)

# lists of datasets to process ------ 
# (see http://www.unimus.no/nedlasting/datasett/ )

dataset <- c("marine_ntnuvmmi","entomology_ntnuvmti")

#connect to the database
pg_drv<-dbDriver("PostgreSQL")
pg_db <- "musit_to_ipt"
pg_host <- "vm-srv-zootron.vm.ntnu.no"
#when running locally use this log in
#con<-dbConnect(pg_drv,dbname=pg_db,user=rstudioapi::askForPassword("Please enter your user name"), password=rstudioapi::askForPassword("Please enter your psw"),host=pg_host, options="-c search_path=public")
#when running on the server use this log in
con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(), dbname=pg_db, options="-c search_path=public")

# download, clean and upload to db ------
for (i in 1:length(dataset)){

  #url <- paste("http://www.unimus.no/nedlasting/datasett/",dataset[i],".gz",sep="")
  url <- paste("http://www.unimus.no/nedlasting/datasett/naturhistorie/",dataset[i],".zip",sep="")
  tmp <- tempfile()
  download.file(url,tmp)
  # NOTE: dataset is further reffered to as "inndata"
  # get the text file from a list of files
  txt_file <- paste(dataset[i],".txt",sep="")
  inndata <- read.csv(unzip(tmp, files=txt_file), sep="\t", header=TRUE, stringsAsFactors=FALSE, quote = "", fileEncoding = "UTF-8")
	
  # some cleaning of data, and adding of terms
  inndata$geodeticDatum <- "WGS84" # add term
  inndata$kingdom <- "Animalia" # add term
  #inndata$countryCode <- countrycode(toString(inndata$country), 'country.name', 'iso3c') # get country code
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
  dbExecute(con,paste("DROP VIEW IF EXISTS public.", dataset[i], "_view", sep="")) # delete the view that serves the ipt
  dbExecute(con,paste("DROP TABLE IF EXISTS", dataset[i], sep="")) # delete existing table
  #copy_to(con,inndata,paste(dataset[i]),temporary = FALSE) # upload table
  dbWriteTable(con,dataset[i], inndata) # upload table
  dbExecute(con,paste("ALTER TABLE ", dataset[i], " ADD import_id SERIAL PRIMARY KEY;")) # make the table content readable accross database platform. OBS! import_id is not persistent it is replace at every import.
  dbExecute(con,paste("ALTER TABLE ", dataset[i], " RENAME \"dcterms.modified\" TO modified;")) # make field name readable in other system that does not support field name with point.
  dbExecute(con,paste("ALTER TABLE ", dataset[i], " RENAME \"row.names\" TO \"row_names\";")) # make field name readable in other system that does not support field name with point.
  dbExecute(con,paste("ALTER TABLE ", dataset[i], " ADD send_to_ipt boolean DEFAULT(TRUE);")) # create a filter to for sending true data to the ipt
  dbExecute(con,paste("update ", dataset[i], " set \"basisOfRecord\" = 'PreservedSpecimen' 
        where lower(\"basisOfRecord\") like '%preserved%' and lower(\"basisOfRecord\") not like 'preservedspecimen';")) # update basisOfRecord for misspelling
  dbExecute(con,paste("update ", dataset[i], " set send_to_ipt = FALSE 
        where position(lower(\"basisOfRecord\") in lower('PreservedSpecimen|FossilSpecimen|LivingSpecimen|HumanObservation|MachineObservation|MaterialSample|Occurrence'))=0;")) # exclude record for export to ipt where basisOfRecord is not normalized according to the DcW vocabulary.
  dbExecute(con,paste("with double_rec_temp as (
        select \"occurrenceID\", count(\"collectionCode\") as n from ", dataset[i], " group by \"occurrenceID\")
        update ", dataset[i], " set send_to_ipt = FALSE
        from (select * from double_rec_temp where n > 1) dbl
	      where dbl.n > 1 and dbl.\"occurrenceID\"=", dataset[i], ".\"occurrenceID\";")) # exclude record to export to ipt when occurrenceID occures more than one
  dbExecute(con,paste("GRANT SELECT ON", dataset[i], "TO ipt;")) # make sure db user ipt has read access
  dbExecute(con,paste("GRANT SELECT ON", dataset[i], "TO natron_guest;")) # make sure db user natron_guest has read access
  dbExecute(con,paste("select public.create_", dataset[i], "_view()",sep="")) # create the view that serves the ipt
}

dbDisconnect(con) # disconnect from DB







