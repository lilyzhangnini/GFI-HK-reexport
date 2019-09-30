# load package
library('pkg')
library("aws.s3")

usr <- 'aws00'
keycache <- read.csv('~/vars/accesscodes.csv', header = TRUE, stringsAsFactors = FALSE)
ec2env(keycache,usr)

## load the reference data, to make sure in same format as the Hong Kong reexport data in our S3 bucket.
reference <- in_hkrx(2016)
### find NA value exist in 'origin_hk' and "consig_hk'
na_count_2016 <-sapply(reference, function(y) sum(length(which(is.na(y)))))
na_count_2016 <- data.frame(na_count_2016)

## load bridge file 
bridge <- in_bridge()
ref_bridge <- in_bridge(c('un_code','un_nm','d_gfi'))
ref_bridge <- ref_bridge[ref_bridge$d_gfi == 1, ]

# deal with 2017 data
## read data in the working directory
hk_2017 <- read.delim('result2017dat.txt',header = FALSE)
colnames(hk_2017) <- c("t","k","origin_hk","consig_hk","vrx_hkd")

## read HKHS description data
hkhscode <- read.csv('Country list and HKHS 6 digit description.csv')
### keep the useful columns
keeps <- c("Country.Code","Country.Short.Description")
hkhscode <- hkhscode[ , keeps, drop = FALSE]

#### make country name consistent in two datafram
library(plyr)
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("THE MAINLAND OF CHINA"="CHINA"))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("CZECHIA"="CZECH REP."))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("KOREA"="REP. OF KOREA"))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("TAIWAN"="Other Asia, nes"))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("DOMINICAN REPUBLIC"="Dominican Rep."))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("BOSNIA & HERZEGOVINA"="Bosnia Herzegovina"))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("MICRONESIA FS & PALAU"="FS Micronesia"))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("RUSSIA"="Russian Federation"))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("MACEDONIA"="TFYR of Macedonia"))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("ESWATINI"="Swaziland"))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("BOLIVIA, P STATE OF"="Bolivia (Plurinational State of)"))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("CENTRAL AFRICAN REP"="Central African Rep."))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("HONG KONG"="China, Hong Kong SAR"))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("COTE D'IVOIRE"="Côte d'Ivoire"))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("CONGO, DEM REP THE"="Dem. Rep. of the Congo"))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("DOMINICAN REPUBLIC"="Dominican Rep."))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("IRAN, ISLAMIC REP"="Iran"))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("LAOS"="Lao People's Dem. Rep."))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("MOLDOVA, REPUBLIC OF"="Rep. of Moldova"))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("SAO TOME & PRINCIPE"="Sao Tome and Principe"))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("SOLOMON ISLANDS"="Solomon Isds"))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("SYRIAN ARAB REPUBLIC"="Syria"))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("TRINIDAD & TOBAGO"="Trinidad and Tobago"))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("TANZANIA,UNITED REP"="United Rep. of Tanzania"))
hkhscode$Country.Short.Description <- revalue(hkhscode$Country.Short.Description, c("VENEZUELA, BOLIVARIAN"="Venezuela"))

## add lower case column
ref_bridge$un_nm_lower <- tolower(ref_bridge$un_nm)
hkhscode$Country.Short.Description.lower <- tolower(hkhscode$Country.Short.Description)

## create un_hk bridge
temp <- merge(ref_bridge, hkhscode, by.x = "un_nm_lower", by.y = "Country.Short.Description.lower",all.x = TRUE) 
keeps <- c("un_code","Country.Code","un_nm")
un_hk_bridge <- temp[ , keeps, drop = FALSE]
names(un_hk_bridge)[2]<-"hkhs_code"

## while create 
new_DF <- temp[rowSums(is.na(temp)) > 0,]


## merge un and hk code together
hkrx_2017 <- merge(x = hk_2017, y = un_hk_bridge[ ,c("hkhs_code", "un_code")], by.x = "origin_hk", by.y = "hkhs_code",all.x=TRUE)
names(hkrx_2017)[6]<-"origin_un"
hkrx_2017 <- merge(x = hkrx_2017, y = un_hk_bridge[ ,c("hkhs_code", "un_code")], by.x = "consig_hk", by.y = "hkhs_code",all.x=TRUE)
names(hkrx_2017)[7]<-"consig_un"

## deal with the exchange rate
hkrx_2017$vrx_un <- round(hkrx_2017$vrx_hkd*0.128317,3)

## reshape column name
col_order <- c("t", "k", "origin_hk",
               "consig_hk", "vrx_hkd","origin_un","consig_un","vrx_un")
hkrx_2017 <- hkrx_2017[, col_order]
write.csv(hkrx_2017, file = "hkrx_2017.csv",row.names=FALSE)

# for 2018 data

hk_2018 <- read.delim('result2018dat.txt',header = FALSE)
colnames(hk_2018) <- c("t","k","origin_hk","consig_hk","vrx_hkd")

## merge un and hk code together
hkrx_2018 <- merge(x = hk_2018, y = un_hk_bridge[ ,c("hkhs_code", "un_code")], by.x = "origin_hk", by.y = "hkhs_code",all.x=TRUE)
names(hkrx_2018)[6]<-"origin_un"
hkrx_2018 <- merge(x = hkrx_2018, y = un_hk_bridge[ ,c("hkhs_code", "un_code")], by.x = "consig_hk", by.y = "hkhs_code",all.x=TRUE)
names(hkrx_2018)[7]<-"consig_un"


## deal with the exchange rate
hkrx_2018$vrx_un <- round(hkrx_2018$vrx_hkd*0.128317,3)

## reshape column name
col_order <- c("t", "k", "origin_hk",
               "consig_hk", "vrx_hkd","origin_un","consig_un","vrx_un")
hkrx_2018 <- hkrx_2018[, col_order]
write.csv(hkrx_2018, file = "hkrx_2018.csv",row.names=FALSE)


## write out un_hk_bridge file
write.csv(un_hk_bridge, file = "un_hk_bridge.csv",row.names=FALSE)