require('sf')
require('terra')
require('dplyr')
require('tmap')
require('leaflet')
require('ggplot2')
require('spData')
tmap_options(component.autoscale = F)
#NDVI raster for map
ndvi_rast<- "/pc/nhair0a/Projects/NHS/MEEE_ndvi_manuscript_062026/NDVI_fs1230m_2023-07-01_5070.tif"
#ndvi_rast<- "/media/williamkessler/extradrive1/Harvard/NDVI_fs1230m_2023-07-01_5070.tif"
ndvi_rast<-rast(ndvi_rast)

#Cohort Geocodes
#cohortpnts<-"/run/user/1000/gvfs/smb-share:server=chanrhsmb.bwh.harvard.edu,share=nhair0a/Projects/NHS/Geocoding/nhsgeo7624.csv"
cohortpnts<-"/pc/nhair0a/Projects/NHS/Geocoding/nhsgeo7624.csv"
#cohortpnts<-read.csv('exampledata/VITAL_toycohort57.csv')
cohortpnts<-read.csv(cohortpnts)
cohortpnts2<-vect(cohortpnts,geom=c("gdtlong","gdtlat"),crs="EPSG:4326")
#cohortpnts2<-vect(cohortpnts,geom=c("x","y"),crs="EPSG:4326")

#Participant IDs for map
#mapIDS<-"/run/user/1000/gvfs/smb-share:server=chanrhsmb.bwh.harvard.edu,share=nhair0a/Projects/NHS/MEEE_ndvi_manuscript_062026/Code/nhs_nodep_ids_fo_map.csv"
mapIDS<-"/pc/nhair0a/Projects/NHS/MEEE_ndvi_manuscript_062026/Code/nhs_nodep_ids_fo_map.csv"
mapIDS<-read.csv(mapIDS)
cohortpnts2<-cohortpnts2[mapIDS$id]
cohortpnts2$start_date<-as.Date(cohortpnts2$start_date,format = "%m/%d/%y")
cohortpnts2$stop_date<-as.Date(cohortpnts2$stop_date,format = "%m/%d/%y")
cohortpnts2<-cohortpnts2[cohortpnts2$start_date >= as.Date("01/01/2000",format="%m/%d/%Y") &cohortpnts2$start_date <= as.Date("12/31/2016",format="%m/%d/%Y") ]

data(us_states)

us_states<-project(vect(us_states),ndvi_rast)
ndvi_rast<-mask(ndvi_rast,us_states,inverse=F)
cohortpnts2<-project(cohortpnts2,ndvi_rast)

##Make The Map
conus_bbox<-st_bbox(buffer(us_states,5000))

# Add fill layer to US shape
USmap<- tm_shape(us_states,
                 bbox=conus_bbox,
                 unit="mi") +
  tm_fill(fill_alpha=0)+
  tm_borders(col_alpha=0.5) 

cohortmap<- tm_shape(cohortpnts2) + 
  tm_symbols(
    shape=20,
    size=0.1,
    fill="black",
    col="black",
  )

panel_a<- USmap + cohortmap +
  tm_layout(frame = F,
            meta.margins = c(0.0, 0, 0, 0))

ndvimap<- tm_shape(ndvi_rast,bbox=conus_bbox,unit="mi") + 
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
      frame=F,
      item.width = 3.0,
      text.size=0.75,
      bg.color="transparent",
      ticks.col="transparent",
      )
    )
  


panel_b<- ndvimap + USmap  +
  tm_compass(position = tm_pos_in("right","bottom"),
             size=1.5,
             type="arrow") +
  tm_scalebar()+
  tm_components(c("tm_compass","tm_scalebar"),
                position = tm_pos_in("left","bottom",align.h="center"),
                stack = "vertical") +
  tm_layout(frame=F,
            legend.show=T,
            position=tm_pos_out("left","bottom"),
            legend.outside=T,
            meta.margins = c(0.0, 0, 0, 0))

  
panel_b

finalmap<- tmap_arrange(panel_a, 
                        panel_b,
                        nrow=2,
                        asp = 0,
                        outer.margins = c(0.2,0,0,0)) 
finalmap

#tmap_save(finalmap,filename="/run/user/1000/gvfs/smb-share:server=chanrhsmb.bwh.harvard.edu,share=nhair0a/Projects/NHS/MEEE_ndvi_manuscript_062026/ParticipantOverview_v2.jpg",width=8,height=10,dpi=300)
tmap_save(finalmap,filename="/media/williamkessler/extradrive1/Harvard/ParticipantOverview_v2.jpg",width=8,height=10,dpi=300)

###############
examplepoint<-vect(x=matrix(c(-71.1143211325372,42.31480155226845),nrow=1,ncol=2),type="points",crs="EPSG:4326")
examplepoint<-project(examplepoint,ndvi_rast)
examplepoint$atb<-"Address Point"
examplebuffers<-lapply(X = c(90,150,270,510,750,990,1230,1500,2100),FUN = function(x) buffer(examplepoint,x))
examplebuffers<-do.call(rbind,examplebuffers)
examplebuffers[["distance"]]<-c("90m","150m","270m","510m","750m","990m","1230m","1500m","2100m")
#examplebuffers<-project(examplebuffers,ndvi_rast)
examplerast<-crop(ndvi_rast,buffer(examplebuffers,300))
examplerast[examplerast<0.55]<-app(examplerast,fun=function(x){x^runif(x,min=0.5,max=2)})
examplerast[examplerast>=0.55]<-app(examplerast,fun=function(x){x^runif(x,min=0.1,max=1)})

plot(examplerast,range=c(0,1),type="continuous")
bufferpnts<-as.points(as.lines(examplebuffers))[match(
  unique(as.data.frame(as.points(as.lines(examplebuffers)))$distance),
         as.data.frame(as.points(as.lines(examplebuffers)))$distance)+c(35,5,10,10,10,10,10,10,10)]
plot(bufferpnts,add=T)

examplebuffermap<- tm_shape(examplebuffers,
                            unit = "mi") +
  tm_polygons(fill="white",
              fill_alpha = 0,
              col="red",
              lwd=1.5) +
  tm_add_legend(group_id="1",
                title="Circular Distance\nBuffers (m)",
                fill="white",
                col="red",
                shape=21,
                size=c(1)) +
  tm_shape(bufferpnts) +
  tm_text(
    text = "distance",
    size=0.65,
    ymod=0,
    xmod=1,
    bgcol = "white",
    bgcol_alpha = 0.5)

examplepntmap<- tm_shape(examplepoint,unit="mi") + 
  tm_squares(
    size=0.4,
    col="black",
    fill="black") + 
  tm_add_legend(title="Address Location",
                group_id="1",
                shape=15,
                fill="black",
                size=c(1))

examplendvimap<- tm_shape(examplerast,unit="mi") + 
  tm_raster(
    col.scale=tmap::tm_scale_continuous(
      values="-hcl.green_yellow",
      value.na="transparent",
      midpoint=NA,
      limits=c(0,1)),
    col.legend = tm_legend(
     title = "Normalized Difference\nVegetation Index (NDVI)",
     group_id="1",
     orientation="portrait",
     reverse=T,
     ticks.col="transparent",
     frame=F)
    )



  
    
examplemap<-examplendvimap + examplepntmap + examplebuffermap  
finalexamplemap<- examplemap + 
  tm_scalebar(text.size=0.75,
              position = tm_pos_out("center", "bottom")) + 
  tm_layout(frame=F,
            legend.outside=T
            ) + 
  tm_components(group_id = "1", position = tm_pos_out("right", "center"))
  

finalexamplemap
#tmap_save(finalexamplemap,filename="/run/user/1000/gvfs/smb-share:server=chanrhsmb.bwh.harvard.edu,share=nhair0a/Projects/NHS/MEEE_ndvi_manuscript_062026/ExampleAddress_v2.jpg",width=8,height=10,dpi=300)
tmap_save(finalexamplemap,filename="/pc/nhair0a/Projects/NHS/MEEE_ndvi_manuscript_062026/ExampleAddress_v2.jpg",width=8,height=10,dpi=300)
tmap_save(finalexamplemap,filename="/media/williamkessler/extradrive1/Harvard/ExampleAddress.jpg",width=8,height=10,dpi=300)
