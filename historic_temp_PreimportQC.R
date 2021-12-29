###############################  HEADER  ######################################
#  TITLE: SQL_ServerTableCreation.R
#  DESCRIPTION: Sends SQL Transact commands to SQL Server to make tables
#  AUTHOR(S): Dan Crocker
#  DATE LAST UPDATED: 2020-11-24
#  GIT REPO:
#  R version 4.0.0 (2020-04-24)  i386
##############################################################################.

### LOAD PACKAGES ####



pkg <- c("tidyverse", "lubridate", "odbc", "DBI", "RODBC")
sapply(pkg, library, character.only = TRUE)

### Eliminate Scientific Notation
options(scipen = 999)

### Path to project files
datapath <- paste0(getwd(), "/data")



#define temperature folder data is stored in 
#folderpath <- paste0("W:/WatershedJAH/EQStaff/Aquatic Biology/Wachusett Profile Data and Physical Properties/Temperature")
#create filepath for excel file
file <- paste0(folderpath, "/Wachusett Reservoir historic temperatures MN.xlsx")
#list sheets in file
readxl::excel_sheets(file)
#select "All data stack" sheet and create dataframe from sheet
df <- as.data.frame(read_xlsx(file, sheet = 3, col_names = TRUE, col_types = c("text", "text", "text", "numeric", "text", "numeric", "numeric","numeric","numeric", "date", "numeric", "text", "text")))




# #define database to connect to 
# db <- paste0("C:/WQDatabase/AqBioDBWachusett_fe.mdb")
# #assign tables within the database to connect to 
# dbtemp <- "tbl_Temperature"
# dblocation <- "tblLocations"
# #establish database connection 
# con <- dbConnect(odbc::odbc(), 
#                  .connection_string = paste("driver={Microsoft Access Driver (*.mdb)}", 
#                                           paste0("DBQ=", db), "Uid=Admin;Pwd=;", sep = ";"),
#                                         timezone = "America/New_York")
# #read temperature table from database 
tbltemp <- dbReadTable(con, dbtemp)
#read location table from database
tbllocation <- dbReadTable(con, dblocation)
#create list of unnecessary columns from xlsx spreadsheet 
drop <- names(df) %in% c("Month_num", "GapFilledTempF", "AvgTempF", "Num_Samples", "Graph Date")
#drop columns and update dataframe 
df <- df[!drop]
#remove data created from entire profile average because dataset is surface temperature
df <- df[grep("Entire profile average", df$Type, invert = TRUE),]

df$StationCopy <- pull(df, var = Station)



###############################################################################################
#TESTING 
# unique(df$Station)
# unique(df$Location)
# unique(df$Location_Desc)
# unique(df$Comment) 
# unique(df$Type)
# 
# sixty <- df[df$Type == "60 minute interval temperature"  ,]
# cosgrove <- df[df$Station == "Cosgrove Intake"  ,]


#df$Location_Desc[gsub("42.39, -71.708", "Basin North Surface", df), ]

###############################################################################################


#These lines work 
#df$Location_Desc[df$Location_Desc =="42.39, -71.708"] <- "Basin North Surface"
#df$Location_Desc[df$Location_Desc =="42.398, -71.689"] <-  "Cosgrove Intake Surface"

df$Station[df$Station =="BN3417"] <- "BN3417-S"
df$Station[df$Station =="CI3409"] <- "CI3409-S"
df$Station[df$Station =="Bottom"] <- "WACH-D"
df$Station[df$Station =="Surface"] <- "WACH-S"
df$Station[df$Station =="Mid-Depth"] <- "WACH-M"
df$Station[df$Location =="Nashua River"] <- "NASH"
df$Station[df$Station == "Lower sluice gate ~330 BCB"] <- "WACH-A"
df$Station[df$Location =="Cosgrove Intake building"] <- "CI3409-I"
df$Station[df$Location =="Inside Cosgrove Intake"] <- "CI3409-I"
df$Station[df$Station =="Cosgrove Intake"] <- "CI3409-S"

df <- left_join(df, tbllocation, by = "Station")

# dropcol <- c("LocationMWRA", "LocationID", "LocationWaterLayer", "LocationDepth", "LocationLat", "LocationLong", "LocationType", "LocationCategory")
# df %>% select(-one_of(dropcol))

df <- df[,-c(1, 2, 7:9, 10:19)]

df <- drop_na(df)
df$AvgTempC <- round(df$AvgTempC, digits = 3)
#create ID column
df$ID <- ""
df <- df[, c(5,1:4)]
#create vector of column names from empty temperature table stored in database 
col_order <- as.vector(names(tbltemp))

names(df) <- col_order

df1 <- df

df1$unique <- ""
df1$unique <- paste(df1$Station, df1$SampleYear, df1$SampleMonth)
dupecheck <- which(duplicated(df1$unique))
dupes <- df1$unique[dupecheck]
doop <- as.data.frame(dupes)

### Update file to go into Database ####
### This portion of the script was written on 1/29/2021 using a .csv that was previously exported from the AqBio Access db
depths <- c("Surface|surface|Mid-Depth|Bottom|2-4|Aqueduct|Inside")
redepth <- c("Surface", "Mid", "Deep")

dbdf <- read_csv(paste0(datapath, "/wachtemperatures_db.csv"))




df <- dbdf %>% 
  mutate(Depth = str_extract_all(.$Station_Description, depths)) %>% 
  mutate(Station_Depth = str_replace_all(.$Depth, c("Surface" ="Surface", "surface" = "Surface", "2-4" = "Surface", "Mid-Depth" = "Mid", "Bottom" = "Deep", "Aqueduct" = "Aqueduct", "Inside" = "Inside"))) %>% 
  select(., -Depth)

dupecheck <- which(duplicated(df$UniqueID))
dupes <- df$UniqueID[dupecheck]

df <- df %>% 
  distinct(UniqueID, .keep_all=TRUE)



write.csv(df, paste0(datapath, "/wachtemperatures_db.csv"), row.names = F)


########################################################################.
###   OMMS Temperature Dataframe QAQC                               ####
########################################################################.
### CONNECT TO WQ DATABASE ####
  ### Set DB
  database <- 'DCR_DWSP'
  schema <- 'Wachusett'
  tz <- 'America/New_York'
  ### Connect to Database 
  con <- dbConnect(odbc::odbc(), 'DCR_DWSP', timezone = tz)
  ### See the tables
  tables <- dbListTables(con, schema_name = schema) %>% print()
  ### Always disconnect and rm connection when done with db
  dbDisconnect(con)
  rm(con)
  
dataframe <- dbReadTable(con, Id(schema = schema, table = 'tbl_ResTemperature'))
ImportTable <- dbReadTable(con, Id(schema = schema, table = 'tbl_ResTemperature'))

 
odbc::dbWriteTable(con, DBI::SQL(glue("{database}.{schema}.tbl")), value = dataframe, append = TRUE)

# Connect to db for queries below
con <- dbConnect(odbc::odbc(),
                 .connection_string = paste("driver={Microsoft Access Driver (*.mdb)}",
                                            paste0("DBQ=", filename.db), "Uid=Admin;Pwd=;", sep = ";"),
                 timezone = "America/New_York")



#all script above works 
# df1 <- df %>%   
#   gsub("Surface", "Surface", .$Depth) %>% 
#   gsub("Mid-Dpeth", "Mid", .$Depth) %>% 
#   gsub("Bottom", "Deep", .$Depth) 
# 
# 
#   

# 
# transmute(Station_Depth = str_replace_all(.$Station_Description, depths, redepth ))
# 
# 
# 
# 
# mutate(morts = str_extract_all(.$Notes, regex("\\d\\s(?=mort)|\\d(?=mort)|\\d\\s\\w\\s(?=mort)|\\d\\s\\S+\\s(?=mort)", ignore_case = T),simplify = T)) %>% 
#   
# 
# 






#select columns from dataframe that match column name vector and recreate dataframe 
#df1 <- select(df, which(names(df) %in% col_order))




# #switched order of values to match col_order order
# gg <- match(names(df), col_order)
# gg <- gg[!is.na(gg)]
# df <- df[,order(gg)]





#Drop if df[Type == "Entire profile average", ] DONE 
#Wachusett Reservoir, Description == Unknown <- DAM 
#Nashua River remains the same in the database? 
#Can now differentiate between CI3409 as a station and inside the intake building 
#How to maintain intricate notes and Type data? Should keep somewhere to answer "Why is this data the way that it is?" Daily or weekly data should be different than 60 minute interval temperatures 

# 
# df$Depth <- pull(filter(df, Station %in% c("Surface" , "Mid-Depth" , "Bottom")), Station)
# df$Depth <- pull(df, Station) %>% 
# df$Station <- pull(df, Location)
# 
# #select all rows with Station as Surface, Mid-Depth, Bottom
# test <- filter(df, Station %in% c("Surface" , "Mid-Depth" , "Bottom"))
# test$Depth <- pull(test, Station)
# test$Station <- pull(test, Location)


# #to create a grouped copy of a table for separate analysis
# mtcars %>% group_by(cyl) %>% summarise(avg=mean(mpg))
# #remove duplicate rows
# distinct()
# #extract rows using logical
# filter()
# #select rows by position
# slice()
# #extract column values as a vector
# pull





