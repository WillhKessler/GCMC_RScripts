require(lubridate)
require(oce)
require(numbers)
require(lutz)

Latitude_posN<-	38
Longitude_posE<-	-98
TimeZone_posE<-	"-07:00"

LocalTime_hrs<-	"12:00:00"
Year<-	2010
Date<-"01/01/2010"
Date2<-as.POSIXct(paste(Date,LocalTime_hrs,TimeZone_posE),tryFormat="%m/%d/%Y %H:%M:%S %z")
Time_pastlocalmidnight<-(as.numeric(Date2)-as.numeric(as.POSIXct(paste(Date,"00:00:00",TimeZone_posE),tryFormat="%m/%d/%Y %H:%M:%S %z")))/3600/24
JulianDay<-julianDay(as.POSIXct(Date2,tz="UTC"))

JulianCentury<-(JulianDay-2451545)/36525
GeomMeanLongSun_deg<-(280.46646+JulianCentury*(36000.76983 + JulianCentury*0.0003032))%%360
GeomMeanAnomSun_deg	<-357.52911+JulianCentury*(35999.05029 - 0.0001537*JulianCentury)
EccentEarthOrbit	<-0.016708634-JulianCentury*(0.000042037+0.0000001267*JulianCentury)

SunEqofCtr	<-sin(GeomMeanAnomSun_deg*(pi/180))*(1.914602-JulianCentury*(0.004817+0.000014*JulianCentury))+sin((2*GeomMeanAnomSun_deg)*(pi/180))*(0.019993-0.000101*JulianCentury)+sin((3*GeomMeanAnomSun_deg)*(pi/180))*0.000289

SunTrueLong_deg	<-GeomMeanLongSun_deg+SunEqofCtr
SunTrueAnom_deg	<-GeomMeanAnomSun_deg+SunEqofCtr
SunRadVector_AUs	<-(1.000001018*(1-EccentEarthOrbit*EccentEarthOrbit))/(1+EccentEarthOrbit*cos(SunTrueAnom_deg*(pi/180)))
SunAppLong_deg	<-SunTrueLong_deg-0.00569-0.00478*sin((pi/180)*(125.04-1934.136*JulianCentury))
MeanObliqEcliptic_deg	<-23+(26+((21.448-JulianCentury*(46.815+JulianCentury*(0.00059-JulianCentury*0.001813))))/60)/60
ObliqCorr_deg	<-MeanObliqEcliptic_deg+0.00256*cos((pi/180)*(125.04-1934.136*JulianCentury))
SunRtAscen_deg	<-(180/pi)*(atan2(cos((pi/180)*(ObliqCorr_deg))*sin((pi/180)*(SunAppLong_deg)),cos((pi/180)*(SunAppLong_deg))))
SunDeclin_deg	<-(180/pi)*(asin(sin((pi/180)*(ObliqCorr_deg))*sin((pi/180)*(SunAppLong_deg))))
vary<-tan((pi/180)*(ObliqCorr_deg/2))*tan((pi/180)*(ObliqCorr_deg/2))
EqofTime_minutes	<-4*(180/pi)*(vary*sin(2*(pi/180)*(GeomMeanLongSun_deg))-2*EccentEarthOrbit*sin((pi/180)*(GeomMeanAnomSun_deg))+4*EccentEarthOrbit*vary*sin((pi/180)*(GeomMeanAnomSun_deg))*cos(2*(pi/180)*(GeomMeanLongSun_deg))-0.5*vary*vary*sin(4*(pi/180)*(GeomMeanLongSun_deg))-1.25*EccentEarthOrbit*EccentEarthOrbit*sin(2*(pi/180)*(GeomMeanAnomSun_deg)))
HASunrise_deg	<-(180/pi)*(acos(cos((pi/180)*(90.833))/(cos((pi/180)*(Latitude_posN))*cos((pi/180)*(SunDeclin_deg)))-tan((pi/180)*(Latitude_posN))*tan((pi/180)*(SunDeclin_deg))))

SolarNoon_LST	<-(720-4*Longitude_posE-EqofTime_minutes+(as.numeric(sub(":","",TimeZone_posE))/100)*60)/1440

SolarNoon_LST_date	<-as.POSIXct(as.Date(SolarNoon_LST,origin=Date2))
SunriseTime_LST	<-SolarNoon_LST-HASunrise_deg*4/1440
SunriseTime_LST_date	<-as.POSIXct(as.Date(SolarNoon_LST-HASunrise_deg*4/1440,origin=Date2))
SunsetTime_LST<- SolarNoon_LST+HASunrise_deg*4/1440
SunsetTime_LST_date<- as.POSIXct(as.Date(SunriseTime_LST))  
SunlightDuration_minutes	<-8*HASunrise_deg

TrueSolarTime_min	<-(((as.numeric(Date2)-as.numeric(as.POSIXct(paste(Date,"00:00:00",TimeZone_posE),tryFormat="%m/%d/%Y %H:%M:%S %z")))/3600/24)*1440+EqofTime_minutes+(4*Longitude_posE)-(60*(as.numeric(strftime(as.POSIXct(Date2,tz="UTC"),format = '%z'))/100)))%%1440

HourAngle_deg	<-if(TrueSolarTime_min/4<0) TrueSolarTime_min/4+180 else TrueSolarTime_min/4-180
SolarZenithAngle_deg	<-(180/pi)*(acos(sin((pi/180)*(Latitude_posN))*sin((pi/180)*(SunDeclin_deg))+cos((pi/180)*(Latitude_posN))*cos((pi/180)*(SunDeclin_deg))*cos((pi/180)*(HourAngle_deg))))
SolarElevationAngle_deg	<-90-SolarZenithAngle_deg
ApproxAtmosphericRefraction_deg	<-if(SolarElevationAngle_deg>85){0/3600
}else if(SolarElevationAngle_deg>5){(58.1/tan((pi/180)*(SolarElevationAngle_deg))-0.07/((tan((pi/180)*(SolarElevationAngle_deg)))^3)+0.000086/((tan((pi/180)*(SolarElevationAngle_deg)))^5)
)/3600}else if(SolarElevationAngle_deg>-0.575){(1735+SolarElevationAngle_deg*(-518.2+SolarElevationAngle_deg*(103.4+SolarElevationAngle_deg*(-12.79+SolarElevationAngle_deg*0.711)))
)/3600}else{(-20.772/tan((pi/180)*(SolarElevationAngle_deg)))/3600}

SolarElevationcorrectedforatmrefraction_deg	<-SolarElevationAngle_deg+ApproxAtmosphericRefraction_deg
SolarAzimuthAngle_degcwfromN<-if(HourAngle_deg>0){
  ((180/pi)*(acos(((sin((pi/180)*(Latitude_posN))*cos((pi/180)*(SolarZenithAngle_deg)))-sin((pi/180)*(SunDeclin_deg)))/(cos((pi/180)*(Latitude_posN))*sin((pi/180)*(SolarZenithAngle_deg)))))+180)%%360
}else{(540-(180/pi)*(acos(((sin((pi/180)*(Latitude_posN))*cos((pi/180)*(SolarZenithAngle_deg)))-sin((pi/180)*(SunDeclin_deg)))/(cos((pi/180)*(Latitude_posN))*sin((pi/180)*(SolarZenithAngle_deg))))))%%360
}
