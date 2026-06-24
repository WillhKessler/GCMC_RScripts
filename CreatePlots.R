require('sf')
require('terra')
require('dplyr')
require('tmap')
require('leaflet')
require('ggplot2')
require('spData')
tmap_options(component.autoscale = F)
#NDVI raster for map
#ndvi_rast<-"/pc/nhair0a/Raw_Exposure_Data/Natural_Environment/Greenness/NDVI/NDVI_30m/fs1230m/NDVI_fs1230m_2"
ndvi_rast<-"/media/williamkessler/extradrive1/Harvard/NDVI_fs1230m_2023-07-01.tif"
ndvi_rast<-rast(ndvi_rast)
ndvi_rast<-clamp(ndvi_rast,lower=0,upper=1)
#Cohort Geocodes
#cohortpnts<-"/pc/nhair0a/Projects/NHS/Geocoding/nhsgeo7624.csv"
cohortpnts<-read.csv('exampledata/VITAL_toycohort57.csv')
cohortpnts<-vect(cohortpnts,geom=c("x","y"),crs="EPSG:4326")

#Participant IDs for map
#mapIDS<-"/pc/nhair0a/Projects/NHS/MEEE_ndvi_manuscript_0622026/nhs_nodep_ids_for_map.csv"
#cohortpnts<-cohortpnts["ID" %in% mapIDS$ids]

data(us_states)

us_states<-project(vect(us_states),ndvi_rast)
ndvi_rast<-mask(ndvi_rast,us_states,inverse=F)
cohortpnts<-project(cohortpnts,ndvi_rast)

##Make The Map
conus_bbox<-st_bbox(buffer(us_states,5000))

# Add fill layer to US shape
USmap<- tm_shape(us_states,bbox=conus_bbox) +
  tm_fill(fill_alpha=0)+
  tm_borders(col_alpha=0.5) 

ndvimap<- tm_shape(ndvi_rast,bbox=conus_bbox) + 
  tm_raster(
    col.scale=tmap::tm_scale_continuous(
      limits= c(0,1),
      values="-hcl.green_yellow",
      value.na="transparent",
      label.na = NA,
      midpoint=NA),
    col.legend = tm_legend(
      show=T,
      na.show = F,
      title = "Normalized Difference Vegetation Index (NDVI)",
      title.size = 0.75,
      title.align = "center",
      orientation="landscape",
      position=tm_pos_in("Left","bottom"),
      frame=F,
      item.width = 3.0,
      text.size=0.75,
      bg.color="transparent"
      )
    )
  
cohortmap<- tm_shape(cohortpnts) + 
  tm_symbols(
    shape=20,
    size=0.5,
    fill="black",
    col="black",
    )

panel_a<- USmap + cohortmap +
  tm_layout(frame = F)

panel_b<- ndvimap + USmap +
  tm_layout(frame=F,
            legend.show=T) +
  tm_compass(position = tm_pos_in("right","bottom"),
             size=1.5,
             type="arrow") +
  tm_scalebar()+
  tm_components(c("tm_compass","tm_scalebar"),
                position = tm_pos_in("right","bottom",align.h="center"),
                stack = "vertical")
  


finalmap<- tmap_arrange(panel_a, panel_b,
             nrow=2)
finalmap

tmap_save(finalmap,filename="ParticipantOverview.jpg",width=8,height=10,dpi=300)

###############
examplepoint<-vect(x=matrix(c(-71.1143211325372,42.31480155226845),nrow=1,ncol=2),type="points",crs="EPSG:4326")
examplebuffers<-lapply(X = c(90,270,510,750,990,1230,1500,2100),FUN = function(x) buffer(examplepoint,x))
examplebuffers<-do.call(rbind,examplebuffers)
examplebuffers[["distance"]]<-c("90m","270m","510m","750m","990m","1230m","1500m","2100m")
examplebuffers<-project(examplebuffers,ndvi_rast)
examplerast<-crop(ndvi_rast,buffer(examplebuffers,300))
examplerast[examplerast<0.55]<-app(examplerast,fun=function(x){x^runif(x,min=0.5,max=2)})
examplerast[examplerast>=0.55]<-app(examplerast,fun=function(x){x^runif(x,min=0.1,max=1)})

plot(examplerast,range=c(0,1),type="continuous")
bufferpnts<-as.points(as.lines(examplebuffers))[match(
  unique(as.data.frame(as.points(as.lines(examplebuffers)))$distance),
         as.data.frame(as.points(as.lines(examplebuffers)))$distance)+5]

examplebuffermap<- tm_shape(examplebuffers) +
  tm_polygons(fill="white",
              fill_alpha = 0,
              col="black",
              lwd=0.5) +
  tm_add_legend(title="Radial Distance \n Buffers (m)",
                fill="white",
                shape=22,
                size=c(1)) +
  tm_shape(bufferpnts) +
  tm_text(
    text = "distance",
    size=0.65,
    ymod=0.8)

examplendvimap<- tm_shape(examplerast) + 
  tm_raster(
    col.scale=tmap::tm_scale_continuous(
      values="-hcl.green_yellow",
      value.na="transparent",
      midpoint=NA,
      limits=c(0,1)),
    col.legend = tm_legend(
     title = "Normalized Difference \n Vegetation Index (NDVI)",
     orientation="portrait",
     frame=F)
    )

require(magick)
house_svg<- image_write(image_read_svg("https://upload.wikimedia.org/wikipedia/commons/3/34/Home-icon.svg"),format="png32",flatten=F,density=90,"house_icon.png")

house_icon<- tmap_icons(
  file="house_icon.png",
  width=16,
  height=16,
  keep.asp = T,
  merge=T,
  as.local=T)

examplepntmap<- tm_shape(examplepoint) + 
  tm_symbols(
    shape=house_icon,
    size=0.2) 
  #tm_add_legend(title="Address Location",
  #              type="symbol",
  #              shape=house_icon)
    
examplemap<-examplendvimap + examplepntmap + examplebuffermap
examplemap + 
  tm_scalebar() + 
  tm_layout(frame=F,
            #legend.outside=T
            ) 
  

