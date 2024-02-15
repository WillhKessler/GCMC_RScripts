####################
# Use this code to read a JSON file into a TIDYVERSE 'Tibble'. From here you should be able 
# to coerce the Tibble into a Dataframe. Each row in the json_raw corresponds to a JSON row i.e. a participant or response. 


require(tidyjson)
#########################################
##---Required Inputs---------############

# Specify an input file: For example: 
jsonfile<-"S:\\GCMC\\_Code\\TESTING_datasets\\sample.json"



#########################################


json_raw<-tidyjson::read_json(jsondir,format = "jsonl")


