# dat<-read.csv("S:\\GCMC\\Data\\AirPollution\\HAPs\\AmbientConcentrations\\2014\\2014_long.csv",stringsAsFactors = F)
# strpdat<-dat[,c("Tract","Pollutant.Name","Total.Conc")]
# widedat<-reshape(strpdat,idvar="Tract",timevar="Pollutant.Name",direction='wide')
# names(widedat)[grep(pattern="Total.Conc.",names(widedat))]<-gsub(pattern="Total.Conc.","",names(widedat)[grep(pattern="Total.Conc.",names(widedat))])
# 
# widedat2<-merge(x=widedat,y=unique(dat[,c("State","EPA.Region","County","FIPS","Population","Tract")]),by="Tract",all.x=T)
# write.csv(widedat2,"S:\\GCMC\\Data\\AirPollution\\HAPs\\AmbientConcentrations\\2014\\2014_Toxics_Ambient_Concentrations.csv")
# 
# 
# ####################################################
# dat<-read.csv("S:\\GCMC\\Data\\AirPollution\\HAPs\\AmbientConcentrations\\HAPs_AmbientConcentrations\\2011\\2011_Toxics_long.csv",as.is = T,stringsAsFactors = F)
# strpdat<-dat[,c("Tract","Pollutant.Name","Total.Conc")]
# widedat<-reshape(strpdat,idvar="Tract",timevar="Pollutant.Name",direction='wide')
# names(widedat)[grep(pattern="Total.Conc.",names(widedat))]<-gsub(pattern="Total.Conc.","",names(widedat)[grep(pattern="Total.Conc.",names(widedat))])
# 
# widedat2<-merge(x=widedat,y=unique(dat[,c("State","EPA.Region","County","FIPS","Population","Tract")]),by="Tract",all.x=T)
# write.csv(widedat2,"S:\\GCMC\\Data\\AirPollution\\HAPs\\AmbientConcentrations\\HAPs_AmbientConcentrations\\2011\\2011_Toxics_Ambient_Concentrations.csv")
# 
# 
# ####################################################
# dat<-read.csv("S:\\GCMC\\Data\\AirPollution\\HAPs\\AmbientConcentrations\\2005\\2005_long.csv",stringsAsFactors = F)
# dat$GEOID<-paste0(dat$FIPS,dat$TRACT)
# strpdat<-dat[,c("GEOID","HAPCAT","CONCTOT")]
# widedat<-reshape(strpdat,idvar="GEOID",timevar="HAPCAT",direction='wide')
# names(widedat)[grep(pattern="CONCTOT.",names(widedat))]<-gsub(pattern="CONCTOT.","",names(widedat)[grep(pattern="CONCTOT.",names(widedat))])
# 
# widedat2<-merge(x=widedat,y=unique(dat[,c("STATE","COUNTY","FIPS","TRACT","POP","GEOID")]),by="GEOID",all.x=T)
# write.csv(widedat2,"S:\\GCMC\\Data\\AirPollution\\HAPs\\AmbientConcentrations\\2005\\2005_Toxics_Ambient_Concentrations.csv")

# ####################################################
# dat<-read.csv("S:\\GCMC\\Data\\AirPollution\\HAPs\\AmbientConcentrations\\2002\\2002_long.csv",stringsAsFactors = F)
# dat$GEOID<-paste0(dat$FIPS,dat$TRACT)
# strpdat<-dat[,c("GEOID","HAPCAT","CONCTOT")]
# widedat<-reshape(strpdat,idvar="GEOID",timevar="HAPCAT",direction='wide')
# names(widedat)[grep(pattern="CONCTOT.",names(widedat))]<-gsub(pattern="CONCTOT.","",names(widedat)[grep(pattern="CONCTOT.",names(widedat))])
# 
# widedat2<-merge(x=widedat,y=unique(dat[,c("STATE","COUNTY","FIPS","TRACT","POP","GEOID")]),by="GEOID",all.x=T)
# write.csv(widedat2,"S:\\GCMC\\Data\\AirPollution\\HAPs\\AmbientConcentrations\\2002\\2002_Toxics_Ambient_Concentrations.csv")

# ####################################################
dat<-read.csv("S:\\GCMC\\Data\\AirPollution\\HAPs\\AmbientConcentrations\\HAPs_AmbientConcentrations\\1999\\1999_long.csv",stringsAsFactors = F)
dat2<-read.csv("S:\\GCMC\\Data\\AirPollution\\HAPs\\AmbientConcentrations\\HAPs_AmbientConcentrations\\1999\\AL_Conc.csv",stringsAsFactors = F)
dat<-rbind(dat,dat2)
#dat$GEOID<-paste0(dat$FIPS,dat$TRACT)
strpdat<-dat[,c("tract_id","hapname","ASPENTotal")]
widedat<-reshape(strpdat,idvar="tract_id",timevar="hapname",direction='wide')
names(widedat)[grep(pattern="ASPENTotal.",names(widedat))]<-gsub(pattern="ASPENTotal.","",names(widedat)[grep(pattern="ASPENTotal.",names(widedat))])

widedat2<-merge(x=widedat,y=unique(dat[,c("tract_id","county","uflag")]),by="tract_id",all.x=T)
write.csv(widedat2,"S:\\GCMC\\Data\\AirPollution\\HAPs\\AmbientConcentrations\\HAPs_AmbientConcentrations\\1999\\1999_Toxics_Ambient_Concentrations_v2.csv")

# ####################################################
# require(tidycensus)
# dat<-read.csv("S:\\GCMC\\Data\\AirPollution\\HAPs\\AmbientConcentrations\\1996\\1996_long.csv",stringsAsFactors = F)
# data(fips_codes)
# fips_codes$county_name<-gsub(" County","",fips_codes[,"county"])
# dat<-merge(dat,fips_codes[,c("state_code","county_code","state_name","county_name")],by.x=c("State","County"),by.y=c("state_name","county_name"),all.x=T)
# dat$GEOID<-paste0(dat$state_code,dat$county_code,dat$FIPS)
# strpdat<-dat[,c("GEOID","Pollutant","Average")]
# widedat<-reshape(strpdat,idvar="GEOID",timevar="Pollutant",direction='wide')
# names(widedat)[grep(pattern="Average.",names(widedat))]<-gsub(pattern="Average.","",names(widedat)[grep(pattern="Average.",names(widedat))])
# 
# widedat2<-merge(x=widedat,y=unique(dat[,c("GEOID","State","County","Urban.or.Rural")]),by="GEOID",all.x=T)
# write.csv(widedat2,"S:\\GCMC\\Data\\AirPollution\\HAPs\\AmbientConcentrations\\1996\\1996_Toxics_Ambient_Concentrations.csv")





dat<-read.csv("S:\\GCMC\\Data\\AirPollution\\HAPs\\AmbientConcentrations\\HAPs_AmbientConcentrations\\2020\\2020_Toxics_long.csv",as.is = T,stringsAsFactors = F)
