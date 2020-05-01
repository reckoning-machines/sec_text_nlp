
#file creates a set of csv from ticker list which include metadata & text data.

library(edgarWebR) #this is an up to date library with an active maintainer.
library(xml2)
library(knitr)
library(dplyr)
library(purrr)
library(rvest)
library(tidyr)
library(readr)
#library(googledrive)
#library(log4r) TODO logging file.

HOME_DIR <- getwd()
DATA_DIR <- paste0(HOME_DIR,"/DATA/")
#dir.create('/data') #doesn't work on the mac .. .w out chmod stuff TODO

#df_tickers <- read_csv("test_ticker_list.csv")
#df_tickers <- drive_download("~/sec_getter/test_ticker_list.csv",type="csv",overwrite = TRUE)
df_tickers <- read_csv("test_ticker_list.csv")

get_filings_links <-function(str_ticker) {
  df_filings <- company_filings(str_ticker, type = "10-", count = 20)
  df_filings <- df_filings[df_filings$type == "10-K" | df_filings$type == "10-Q", ]
  df_filing_infos <- map_df(df_filings$href, filing_information)
  df_filings <- bind_cols(df_filings, df_filing_infos)
  return(head(as_tibble(df_filings),6))
}

get_section_text <- function(str_href, str_section, str_search) {
  df_filing_documents <- filing_documents(str_href)
  str_doc_href <- df_filing_documents[df_filing_documents$type == "10-K" | df_filing_documents$type == "10-Q",]$href
  doc <- parse_filing(str_doc_href)

  df_txt <- doc[grepl(str_section, doc$item.name, ignore.case = TRUE) & grepl(str_search, doc$item.name, ignore.case = TRUE), ] # only discussion for now
  #we could do some text preprocessing here.

  df_txt <- as_tibble(df_txt) %>%
    mutate(section = str_search)

  return(df_txt)
}

get_document_text <- function(str_ticker, force = FALSE) { #not using force yet
  start_time <- Sys.time()
  
  print(str_ticker)
  
  df_filings <- get_filings_links(str_ticker)

  print(href)
  
  df_data <- (df_filings) %>% 
    rowwise() %>%
    mutate(nest_discussion = map(.x = href, str_section = 'item 2|item 7',str_search = 'discussion', .f = get_section_text)) %>%
    mutate(nest_qualitative = map(.x = href, str_section = 'item 3|item 7a', str_search = 'qualitative', .f = get_section_text)) %>%
    mutate(nest_controls = map(.x = href, str_section = 'item 4|item 9a',str_search = 'controls', .f = get_section_text)) %>%
    mutate(nest_risk = map(.x = href, str_section = 'item 1',str_search = 'risk factors', .f = get_section_text)) %>%
    ungroup() %>%
    select(period_date,filing_date,type,form_name,documents,nest_discussion,nest_qualitative,nest_controls,nest_risk) %>%
    group_by(period_date) %>%
    arrange(desc(period_date))
  
  #jenky - find a rowwise application
  a <- df_data %>% 
    select(period_date,filing_date,type,form_name,documents,nest_discussion) %>%
    unnest(nest_discussion)
  b <- df_data %>% 
    select(period_date,filing_date,type,form_name,documents,nest_qualitative) %>%
    unnest(nest_qualitative)
  c <- df_data %>% 
    select(period_date,filing_date,type,form_name,documents,nest_controls) %>%
    unnest(nest_controls)
  d <- df_data %>% 
    select(period_date,filing_date,type,form_name,documents,nest_risk) %>%
    unnest(nest_risk)
  df_data <- rbind(a,b,c,d) %>%
    write_csv(paste0(DATA_DIR,str_ticker,"_sec_text.csv")) 
  
  #df_data %>%
  #  unnest(cols=c(nest_discussion)) %>%
  #  select(-nest_qualitative,-nest_controls,-nest_risk) %>%
  #  write_csv(paste0(str_ticker,"_discussion.csv")) 


  #df_data %>%
  #  unnest(cols=c(nest_qualitative)) %>%
  #  select(-nest_discussion,-nest_controls,-nest_risk) %>%
  #  write_csv(paste0(str_ticker,"_qualitative.csv")) 

  #df_data %>%
  #  unnest(cols=c(nest_controls)) %>%
  #  select(-nest_discussion,-nest_qualitative,-nest_risk) %>%
  #  write_csv(paste0(str_ticker,"_controls.csv")) 
    
  #print("four")
  #df_data %>%
  #  unnest(cols=c(nest_risk)) %>%
  #  select(-nest_discussion,-nest_controls,-nest_qualitative) %>%
  #  write_csv(paste0(str_ticker,"_risk.csv")) 
  
  end_time <- Sys.time()
  print(end_time - start_time)
  return(df_data)
}

upload_files <- function(str_ticker, force = FALSE) { #too slow to be useful
  print(str_ticker)
  drive_upload(paste0(getwd(),"/",str_ticker,"_discussion.csv"),path = paste0("~/sec_getter/data/",str_ticker,"_discussion.csv"),overwrite = TRUE) 
  drive_upload(paste0(getwd(),"/",str_ticker,"_qualitative.csv"),path = paste0("~/sec_getter/data/",str_ticker,"_qualitative.csv"),overwrite = TRUE) 
  drive_upload(paste0(getwd(),"/",str_ticker,"_controls.csv"),path = paste0("~/sec_getter/data/",str_ticker,"_controls.csv"),overwrite = TRUE) 
  drive_upload(paste0(getwd(),"/",str_ticker,"_risk.csv"),path = paste0("~/sec_getter/data/",str_ticker,"_risk.csv"),overwrite = TRUE) 
}

#long run.
df_data <- map_df(df_tickers$Symbol, get_document_text)

#map(df_tickers$Symbol, upload_files)


