
#file creates a set of csv from ticker list which include metadata & text data.

library(edgarWebR)
library(xml2)
library(knitr)
library(dplyr)
library(purrr)
library(rvest)
library(tidyr)
library(readr)

#for now we're only doing the discussion.
#i want to think how to map a generic section function.  may just put it all in one vs a parameter.

#str_mdna <- 'discussion'
#str_mkt_risk <- 'qualitative'
#str_controls <-'controls'
#str_risk <- 'risk factors'

tickers <- read_csv("test_ticker_list.csv")

get_filings_links <-function(ticker) {
  filings <- company_filings(ticker, type = "10-", count = 20)
  filings <- filings[filings$type == "10-K" | filings$type == "10-Q", ]
  filing_infos <- map_df(filings$href, filing_information)
  filings <- bind_cols(filings, filing_infos)
  return(as_tibble(filings))
}

get_section_text <- function(href) {
  df_filing_documents <- filing_documents(href)
  doc_href <- df_filing_documents[df_filing_documents$type == "10-K" | df_filing_documents$type == "10-Q",]$href
  doc <- parse_filing(doc_href)
  txt <- doc[grepl("discussion", doc$item.name, ignore.case = TRUE), ] # only discussion for now
  #we could do some text preprocessing here.
  return(as_tibble(txt))
}

get_mdna_file <- function(ticker) {
  start_time <- Sys.time()
  print(ticker)
  filings <- get_filings_links(ticker)
  
  data <- filings %>% rowwise() %>%
    mutate(mdna = map(href, get_section_text)) %>%
    ungroup() %>%
    select(period_date,filing_date,type,form_name,documents,mdna) %>%
    group_by(period_date) %>%
    arrange(desc(period_date)) %>%
    unnest(cols=c(mdna)) %>%
    write_csv(paste0(ticker,'.csv'))
  
  end_time <- Sys.time()
  print(end_time - start_time)
  return(data)
}

data <- map_df(tickers$Symbol, get_mdna_file)
#munge from here.  probably in python.





