################################### HEADER ###################################
#   TITLE: Historic Temperature Data Compilation 
#   DESCRIPTION:This is the script used to clean historic surface temperature data for a project beginning in winter of 2018. 
#   
#   AUTHOR(S): Max Nyquist  
#   DATE LAST UPDATED: 12/29/2021
#   R VERSION 3.X.X
##############################################################################.

### LOAD PACKAGES ####
### LOAD PACKAGES ####
pkg <- c("tidyverse", "lubridate", "odbc", "DBI", "RODBC", "readr")
sapply(pkg, library, character.only = TRUE)





# library(tidyverse)
# library(lubridate)
# library(openxlsx)
# library(xlsx)

#if file not in wd, use file.path function
#if file in wd, just use read.table, assuming this if a flat file (.csv)
#.csv can only be one page in excel

#read.table("Max_Raw_temps.xlsx", 
  #         header = TRUE, 
   #        sep= ",", 
    #       stringsAsFactors = FALSE)
####################################################################################################
#EXCEL IMPORT METHOD
### compiled raw temps spreadsheet 
filename <- "Max_Raw_temps.xlsx"
read_excel_allsheets <- function(filename, tibble = FALSE) {
  sheets <- readxl::excel_sheets(filename)
  x <- lapply(sheets, function(X) readxl::read_excel(filename, sheet = X))
  if(!tibble) x <- lapply(x, as.data.frame)
  names(x) <- sheets
  x
}
rawtemps <- read_excel_allsheets(filename)
df <- ldply(rawtemps, data.frame)

# read_csv_custom <- function(filname, tibble = FALSE) {
#   files <- read.csv(filename)
#   x <- lapply(files, function(X) read.csv(filename, files = X))
#   if(!tibble) x <- lapply(x, as.data.frame)
#   names(x) <- files
#   x
# }
# temps <- read_csv_custom()
# temps <- lapply(Sys.glob(pattern = "CITempData*.csv$"), read.csv)
# 


####################################################################################################
#CSV IMPORT METHOD
#1998-2004. 60 minute interval temperature data, presumably in same location as current CI temp probe. ### 
### Old p drive .csv compilation  
options(scipen = 50)
df98 <- read_csv("P:/R/WachTemps/1998.csv", col_names = TRUE, col_types = cols(
  Date = col_character(),
  Time = col_time(format = ""),
  Temp = col_double()))
df98 <- as.data.frame(df98)
df98$Date <- as.Date(df98$Date, format = "%m/%d/%Y")
df98$Time <- format(as.POSIXct(df98$Time), format = "%H:%M:%S")


df9901 <- read_csv("P:/R/WachTemps/CI_Telog_Data_1999_2001.csv")
df9901 <- df9901[complete.cases(df9901[ , 3]),]
df9901 <- as.data.frame(df9901)

df9901$Temp[df9901$Temp < 0] <- NA
df9901 <- na.omit(df9901)
df9901$Date <- as.Date(df9901$Date, format = "%m/%d/%Y")
df9901$Time <- format(as.POSIXct(df9901$Time), format = "%H:%M:%S")

df02 <- read_csv("P:/R/WachTemps/2002.csv")
df02 <- as.data.frame(df02)
df02$Date <- as.Date(df02$Date, format = df98$Date <- as.Date(df98$Date, format = "%m/%d/%Y"))
df02$Time <- format(as.POSIXct(df02$Time), format = "%H:%M:%S")
colnames(df02) <- c("Date", "Time", "Temp")

df0304 <- read_csv("2003_2004_CI_Wac_Int_PI.csv")
df0304 <- as.data.frame(df0304)
df0304 <- separate(df0304, 1, c("Date", "Time"), sep = " ", remove = T, convert = TRUE)
df0304$Date <- as.Date(df0304$Date, format = "%m/%d/%Y")
df0304$Time <- paste0(df0304$Time, ":00")
df0304 <- df0304[ , -c(4)]
df0304 <- df0304[complete.cases(df0304[ , 3]),]
colnames(df0304) <- c("Date", "Time", "Temp")


df <- rbind(df98, df02, df9901, df0304)

df$Station <- paste("Cosgrove Intake")
df$Location <- paste("Inside Cosgrove Intake")
df$Location_Desc <- paste("similar location to current CI temp probe")
summary(df)
df$Month <- months(df$Date)
df$Year <- format(df$Date, format = "%Y")
df$Month_Num <- month(df$Date, label = FALSE)

# df9901[rowSums(is.na(df9901[ , 3])) ==0, ]
# row.has.na <- apply(df9901, 1, function(x){any(is.na(x))})
# sum(row.has.na)
convertCtoF<- function(df) {
  fahrenheit <- ((df *(9/5)+32))
  return(fahrenheit)
}
df$TempF <- convertCtoF(df$Temp)

df2 <- aggregate(cbind(Temp, TempF) ~ Month + Year + Month_Num + Location_Desc + Station + Location, df, mean )
df2 <- df2[ , c(6, 4, 5, 2, 1, 3, 7, 8)]
write.xlsx(df2, "P:/R/WachTemps/TempData_1998_2004.xlsx", sheetName = "1998-2004")

####################################################################################################
#2002-2008 Daily or weekly data, Inside Cosgrove Intake building. 
df0208 <- read_csv("P:/R/WachTemps/WachTemp_Grab02to08.csv")
df0208 <- df0208[ , c(3, 4, 6)]
df0208 <- as.data.frame(df0208)
colnames(df0208) <- c("Date", "Temp", "Station")
df0208$Station <- paste("Cosgrove Intake")
df0208$Date <- dmy(df0208$Date)
df0208 <- df0208[complete.cases(df0208[ , 2]),]


df0208$Month <- months(df0208$Date)
df0208$Month_num <- month(df0208$Date, label = FALSE)
df0208$Year <- format(df0208$Date, format = "%Y")
df3 <- aggregate(Temp ~ Month+Month_num+Year +Station , df0208, mean )
df3$TempF <- convertCtoF(df3$Temp)
df3$Location <- paste("Inside Cosgrove Intake")
df3$Location_Desc <- paste("In line readings")
df3 <- df3[,c(7,8,4,3,1,2,5,6)]
write.xlsx(df3, "P:/R/WachTemps/TempData_2002_2008.xlsx", sheetName = "2002-2008")


####################################################################################################
#2009-2016 15 minute interval temprature Inside Cosgrove Intake building, raw water intake. 
df0916 <- read_csv("P:/R/WachTemps/CI_Temp2009_201610_15m.csv")
df0916 <- as.data.frame(df0916)
colnames(df0916) <- c("Date", "Temp", "TempRaw")
df4 <- df0916[ , c(1,2)]
df4$Date <- mdy_hm(as.character(df4$Date))
df4$Month <- months(df4$Date)
df4$Month_num <- month(df4$Date, label = FALSE)
df4$Year <- format(df4$Date, format = "%Y")
df4 <- aggregate(Temp ~ Month +Year+Month_num , df4, mean )
df4$Location <- paste("Cosgrove Intake building")
df4$Location_Desc <- paste("Inside Cosgrove intake building, raw water intake")
df4$Station <- paste("Cosgrove Intake")
df4$TempF <- convertCtoF(df4$Temp)
df4 <- df4[,c(5,6,7,2,1,3,4,8)]
write.xlsx(df4, "P:/R/WachTemps/TempData_2009_2016.xlsx", sheetName = "2009-2016")

#TempRaw may be from CWTP
# df5 <- df0916[, c(1,3)]
# df5$Date <- mdy_hm(as.character(df5$Date))
# df5$Month <- months(df5$Date)
# df5$Year <- format(df5$Date, format = "%Y")
# df5 <- aggregate(TempRaw ~ Month +Year , df5, mean )

####################################################################################################
#1967-1989 Off the back of Cosgrove Intake, sample collected at surface (Guy Foss 2016). 
df6789 <- read_csv("P:/R/WachTemps/Temperature Wach_1967_1989_surfaceshaftA.csv")
df6789 <- as.data.frame(df6789)
df6 <- df6789[ , c(1:3,4,11)]
df6$Date <- ymd(paste(df6$YEAR,  df6$MONTH, df6$DAY,sep = "-"))
df6$YEAR <- as.character(df6$YEAR)


colnames(df6) <- c("Year", "Location_Desc", "Month_num", "Day", "TempF", "Date")

####################################################################################################
### 2019-2021 Cosgrove internal data
my_files <- list.files(path = paste0(getwd(),"/data"), pattern = "CITempData+.*csv")

all_csv <- lapply(paste0(getwd(),"/data/",my_files), read.csv)

names(all_csv) <- gsub("CITempData.csv", "", list.files("", full.names = FALSE), fixed = TRUE)

df <- do.call(rbind.data.frame, all_csv)

df <- df %>% 
   rename(Date = Date.Time, Average_Temp_C = Value) %>% 
  mutate(ID = "", UniqueID="", Station = "CI3409-I-R", Station_Description = "Cosgrove Intake building, Inside Cosgrove intake building, raw water intake, Cosgrove Intake, NA", Station_Depth_Strata = "Inside")

 
df$Time <- as_datetime(df$Date, format =  "%m/%d/%Y %H:%M")


df$Date <- as_date(df$Date, format = "%m/%d/%Y")
df$Location <- "CI3409"
df <- df %>% 
  mutate(Sample_Year = year(Date), Sample_Month = month(Date)) #%>% 
  
df$Average_Temp_C[df$Average_Temp_C<0] <- NA
df$Average_Temp_C[df$Average_Temp_C>50] <- NA

df <- unique(df)

df <- aggregate(Average_Temp_C ~ Sample_Month + Sample_Year + Station_Description + Station + Location + ID + Station_Depth_Strata +UniqueID, df, mean , na.rm=TRUE)

df <- df[,c(6, 5, 4, 2, 1, 9, 8, 3, 7)]



#dataframe <-  seq.int(nrow(dataframe))

#len <- length(dataframe)
len <- last(dataframe$ID)

df$ID <- seq.int(from = len +1, length.out = nrow(df))

df$Sample_Month <- month.name[df$Sample_Month]

df <- df%>% 
  mutate(UniqueID = paste0(Station, Sample_Year, Sample_Month))


rm(df.t, dfh, test, dataframe.t)
dataframe <- rbind(dataframe, df)

saveRDS(dataframe, "temp.rds")

## Make sure it is unique within the data file - if not then exit function and send warning
dupecheck <- which(duplicated(df$UniqueID))
dupes <- df$UniqueID[dupecheck] # These are the dupes

if (length(dupes) > 0){
  # Exit function and send a warning to userlength(dupes) # number of dupes
  stop(paste("This data file contains", length(dupes),
             "records that appear to be duplicates. Eliminate all duplicates before proceeding.",
             "The duplicate records include:", paste(head(dupes, 15), collapse = ", ")), call. = FALSE)
}

Uniq <- dbGetQuery(con,paste0("SELECT UniqueID, ID FROM ", ImportTable))
dupes2 <- Uniq[Uniq$UniqueID %in% df$UniqueID,]

if (nrow(dupes2) > 0){
  # Exit function and send a warning to user
  stop(paste("This data file contains", nrow(dupes2),
             "records that appear to already exist in the database!
             Eliminate all duplicates before proceeding.",
             "The duplicate records include:", paste(head(dupes2$UniqueID, 15), collapse = ", ")), call. = FALSE)
}
rm(Uniq)


setIDs <- function(){
  query.wq <- dbGetQuery(con, paste0("SELECT max(ID) FROM ", ImportTable))
  # Get current max ID
  if(is.na(query.wq)) {
    query.wq <- 0
  } else {
    query.wq <- query.wq
  }
  ID.max.wq <- as.numeric(unlist(query.wq))
  rm(query.wq)
  
  ### ID wq
  df$ID <- seq.int(nrow(df)) + ID.max.wq
}
df$ID <- setIDs()


# Get column names from db table
cnames <- dbListFields(con, ImportTable)
#list(cnames)
names(df.wq) <- cnames






odbc::dbWriteTableable(con, DBI::SQL(glue("{database}.{schema}.tbl")), value = dataframe, append = TRUE)

#learning notes on creating a function
#namedfunction <- function(nameofdatatogointofunction){
# line defining/describing what to do with nameofdatatogointofunction
# namedreturnvalue <- ((nameofdatatogointofunction + 1))
# define output
# return(namedreturnvalue)
#}
convertFtoC<- function(df6) {
  celsius <- ((df6 - 32)*(5/9))
  return(celsius)
}
 
df6$TempC <- convertFtoC(df6$TempF)
df6$Temp <- convertFtoC(df6$Temp)
df6$Month <- months(df6$Date)
df6$Year <- format(df6$Date, format = "%Y")
df7 <- aggregate(cbind(TempC, TempF) ~ Month + Year + Month_num + Location_Desc, df6, mean )
df7$Location <- "Back of Cosgrove Intake"
df7$Station <- "Cosgrove Intake"
df7 <- df7[, c(7, 4, 8, 2, 1, 3, 5, 6)]
summary(df7)

write.xlsx(df7, "P:/R/WachTemps/TempData_1967_1989.xlsx", sheetName = "1967-1989")

CItempNov2016_Dec2016 <- read_csv("P:/R/WachTemps/CItempNov2016_Dec2016.csv")
CItempDec2016_Mar2017 <- read_csv("P:/R/WachTemps/CItempDec2016_Mar2017.csv")
CItempMar2017_July2017 <- read_csv("P:/R/WachTemps/CItempMar2017_July2017.csv")
CItempJuly2017_Oct2017 <- read_csv("P:/R/WachTemps/CItempJuly2017_Oct2017.csv")
CItempOct2017_Jan2018 <- read_csv("P:/R/WachTemps/CItempOct2017_Jan2018.csv")
CItempJan2018_May2018 <- read_csv("P:/R/WachTemps/CItempJan2018_May2018.csv")
CItempMay2018_Aug2018 <- read_csv("P:/R/WachTemps/CItempMay2018_Aug2018.csv")
CItempAug2018_Oct2018 <- read_csv("P:/R/WachTemps/CItempAug2018_Oct2018.csv")
dfCI <- rbind(CItempNov2016_Dec2016, CItempDec2016_Mar2017, CItempMar2017_July2017, CItempJuly2017_Oct2017, CItempOct2017_Jan2018, CItempJan2018_May2018, CItempMay2018_Aug2018, CItempAug2018_Oct2018)

# colnames(df0916) <- c("Date", "Temp", "TempRaw")
# dfCI <- df0916[ , c(1,2)]
dfCI$Date <- mdy_hm(as.character(dfCI$`Date/Time`))
dfCI$Month <- months(dfCI$Date)
dfCI$Month_num <- month(dfCI$Date, label = FALSE)
dfCI$Year <- format(dfCI$Date, format = "%Y")
dfCI <- aggregate(Value ~ Month +Year +Month_num +Location, dfCI, mean)
dfCI$Location <- paste("Cosgrove Intake building")
dfCI$Location_Desc <- paste("Inside Cosgrove intake building, raw water intake")
dfCI$Station <- paste("Cosgrove Intake")
dfCI$TempF <- convertCtoF(dfCI$Value)
dfCI <- dfCI[,c(4, 6, 7, 2, 1, 3, 5, 8)]
write.xlsx(dfCI, "P:/R/WachTemps/TempData_2016_2018.xlsx", sheetName = "2016-2018")


