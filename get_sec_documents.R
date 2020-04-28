
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
  return(as_tibble(df_filings))
}

get_section_text <- function(str_href) {
  df_filing_documents <- filing_documents(str_href)
  str_doc_href <- df_filing_documents[df_filing_documents$type == "10-K" | df_filing_documents$type == "10-Q",]$href
  doc <- parse_filing(str_doc_href)
  df_txt <- doc[grepl("discussion", doc$item.name, ignore.case = TRUE), ] # only discussion for now
  #we could do some text preprocessing here.
  return(as_tibble(df_txt))
}

get_mdna_file <- function(str_ticker) {
  start_time <- Sys.time()
  print(str_ticker)
  df_filings <- get_filings_links(str_ticker)
  
  df_data <- df_filings %>% rowwise() %>%
    mutate(mdna = map(href, get_section_text)) %>%
    ungroup() %>%
    select(period_date,filing_date,type,form_name,documents,mdna) %>%
    group_by(period_date) %>%
    arrange(desc(period_date)) %>%
    unnest(cols=c(mdna))
  
  df_data %>%
    write_csv(paste0(str_ticker,'.csv'))
  
  end_time <- Sys.time()
  print(end_time - start_time)
  return(df_data)
}

df_data <- map_df(df_tickers$Symbol, get_mdna_file)
#munge from here.  probably in python.

x <- get_mdna_file("AXP")
x
