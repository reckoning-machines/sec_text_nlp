
#file creates a set of csv from ticker list which include metadata & text data.

library(edgarWebR) #this is an up to date library with an active maintainer.
library(xml2)
library(knitr)
library(dplyr)
library(purrr)
library(rvest)
library(tidyr)
library(readr)

#for now we're only doing the discussion section.

#i want to think how to map a generic section function.  may just put it all in one vs a parameter.

#str_mdna <- 'discussion'
#str_mkt_risk <- 'qualitative'
#str_controls <-'controls'
#str_risk <- 'risk factors'

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

  #print(str_section)
  
  df_txt <- doc[grepl(str_section, doc$item.name, ignore.case = TRUE) & grepl(str_search, doc$item.name, ignore.case = TRUE), ] # only discussion for now
  #we could do some text preprocessing here.
  df_txt$section <- str_section
  return(as_tibble(df_txt))
}

get_document_text <- function(str_ticker) {
  start_time <- Sys.time()
  
  print(str_ticker)
  
  df_filings <- get_filings_links(str_ticker)
  
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
    
  df_data %>%
    unnest(cols=c(nest_discussion)) %>%
    select(-nest_qualitative,-nest_controls,-nest_risk) %>%
    write_csv(paste0(str_ticker,"_discussion.csv"))

  df_data %>%
      unnest(cols=c(nest_qualitative)) %>%
      select(-nest_discussion,-nest_controls,-nest_risk) %>%
      write_csv(paste0(str_ticker,"_qualitative.csv"))
  
  df_data %>%
      unnest(cols=c(nest_controls)) %>%
      select(-nest_discussion,-nest_qualitative,-nest_risk) %>%
      write_csv(paste0(str_ticker,"_controls.csv"))
    
  df_data %>%
      unnest(cols=c(nest_risk)) %>%
      select(-nest_discussion,-nest_controls,-nest_qualitative) %>%
      write_csv(paste0(str_ticker,"_risk.csv"))
      
  end_time <- Sys.time()
  print(end_time - start_time)
  return(df_data)
}

df_data <- map_df(df_tickers$Symbol, get_document_text)

#df_data <- get_document_text('AXP')
#df_data %>%
#  select(type,nest_discussion,nest_qualitative,nest_controls,nest_risk) %>%
#  tail()
#df_data
