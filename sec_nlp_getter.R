#devtools::install_packages('trinker/textclean')
#devtools::install_github("mwaldstein/edgarWebR")
#devtools::install_github("r-lib/xml2") #this for edgarWebR
#devtools::install_github('trinker/textclean')


#library from https://mwaldstein.github.io/edgarWebR/
suppressPackageStartupMessages(library(edgarWebR)) #this is an up to date library with an active maintainer.
suppressPackageStartupMessages(library(xml2))
suppressPackageStartupMessages(library(knitr))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(purrr))
suppressPackageStartupMessages(library(rvest))
suppressPackageStartupMessages(library(tidyr))
suppressPackageStartupMessages(library(readr))

source('sec_R_utils.R')

#https://github.com/DavisVaughan/furrr
library(furrr) 

#read our ticker list
#df_tickers <- read_csv('implementation_ticker_list.csv',col_types = cols())
df_tickers <- read_csv('test_ticker_list.csv',col_types = cols()) 
print(df_tickers)

#create our data folder
#dir.create('sec_data_folder', showWarnings = FALSE)

#prep for multiprocessing
future::plan(multiprocess)

#tell us what we're doing


#get_ticker_text("AXP")
#map get_document_text from sec_R_utils.R across the vector Symbol in the df_tickers dataframe
#show progress bar, thank you Davis 
result <- future_map_dfr(df_tickers$Symbol, get_ticker_text,.progress = TRUE) #takes a few minutes
result <- future_map_dfr(df_tickers$Symbol, get_mdna_text,.progress = TRUE) #takes a few minutes
result <- future_map_dfr(df_tickers$Symbol, get_riskfactors_text,.progress = TRUE) #takes a few minutes

print('done')

