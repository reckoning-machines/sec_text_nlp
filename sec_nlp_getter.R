#devtools::install_packages('trinker/textclean')
#devtools::install_github("mwaldstein/edgarWebR")
#devtools::install_github("r-lib/xml2") #this for edgarWebR
#devtools::install_github('trinker/textclean')

library(edgarWebR) #this is an up to date library with an active maintainer.
library(xml2)
library(knitr)
library(dplyr)
library(purrr)
library(rvest)
library(tidyr)
library(readr)
#library(textshape)
#library(lexicon)
library(textclean)
library(furrr)

LOGFILE = format(Sys.time(), "%b_%d_%Y.log")
print(LOGFILE)

CSVFILE = format(Sys.time(), "%b_%d_%Y.csv")
print(CSVFILE)

get_filings_links <-function(str_ticker) {
    df_filings <- company_filings(str_ticker, type = "10-", count = 20)
    df_filings <- df_filings[df_filings$type == "10-K" | df_filings$type == "10-Q", ]
    df_filing_infos <- map_df(df_filings$href, filing_information)
    df_filings <- bind_cols(df_filings, df_filing_infos)
    return(head(as_tibble(df_filings),16))
  }

write_log <- function(str_text) {
      print(str_text)
      if (file.exists(LOGFILE)) {
          write(str_text,file=LOGFILE,append=TRUE)
      } else {
          write(str_text,file=LOGFILE,append=FALSE)
      }

  }

write_log_csv <- function(df) {
    if (file.exists(CSVFILE)) {
          write_csv(df,CSVFILE,append=TRUE)
      } else {
          write_csv(df,CSVFILE,append=FALSE)
      }

  }

get_mdna_text <- function(str_href) {
  write_log(str_href)
  #str_href <- 'https://www.sec.gov/Archives/edgar/data/1390777/000139077720000062/0001390777-20-000062-index.htm'
  #default search
  str_section = 'item 2|item 7'
  str_search = 'discussion'

  df_filing_documents <- filing_documents(str_href) %>%
    filter(!grepl('.pdf',href))

  str_doc_href <- df_filing_documents[df_filing_documents$type == "10-K" | df_filing_documents$type == "10-Q",]$href
  
  file_end <- gsub("https://www.sec.gov",'',str_doc_href)
  
  file_name = paste0(getwd(),file_end)
  
  #use cache if possible
  if (file.exists(file_name)) {

    doc <- read_csv(file_name,col_types = cols(.default = "c"))
    print("local cache")
    
  } else {

    doc <- parse_filing(str_doc_href)    

    str_file_path <- ''
    file_path = strsplit(file_name,'/')
    for (i in 3:length(file_path[[1]])-1) {
      str_file_path = paste0(str_file_path,"/",(file_path[[1]][i]))
    }
    str_file_path <- paste0(str_file_path,"/")
    dir.create(str_file_path,recursive = TRUE)
    write_csv(as_tibble(doc),file_name)
    
  }

  df_txt <- doc[grepl(str_section, doc$item.name, ignore.case = TRUE) & grepl(str_search, doc$item.name, ignore.case = TRUE), ] # only discussion for now
  #if default search fails, use a dictionary attempt
  if (nrow(df_txt) == 0) {
    write_log('going to backup')
    # paired vector of start and ending text to slice if found

    #JPM and #ECL
    vec_start_end <- c('RESULTS OF OPERATIONS'='QUANTITATIVE AND QUALITATIVE DISCLOSURES',
                       'Overview'='Forward-Looking Statements',
                       'Entergy operates'='New Accounting Pronouncements',
                       'MANAGEMENT’S FINANCIAL DISCUSSION'='New Accounting Pronouncements',
                       'General'='Website information',
                       'Management’s Discussion'='Risk Disclosures',
                       'EXECUTIVE SUMMARY'='RISK FACTORS',
                       'EXECUTIVE OVER'='A summary of contractual obligations is included',
                       'EXECUTIVE OVERVIEW'='CONSOLIDATED RESULTS OF OPERATIONS',
                       'The following management discussion and analysis'='NON-GAAP FINANCIAL MEASURES',
                       'CURRENT ECONOMIC CONDITIONS'='FORWARD-LOOKING STATEMENTS')

    #this would be case sensitive
    for (start_text in names(vec_start_end)) {

      end_text = as.character(vec_start_end[start_text])

      write_log(paste0('trying ',start_text))
      write_log(paste0('to ',end_text))

      i_start = as.integer(which(grepl(start_text, doc$text))) #this should be a loop for each dictionary item
      if (length(i_start) > 1) { #handle table of contents
        i_start = i_start[2]
      }
      i_end = as.integer(which(grepl(end_text, doc$text)))
      if (length(i_end) > 1) {
        i_end = i_end[2]
      }

      write_log(i_start)
      write_log(i_end)

      if (length(i_start) != 0 & length(i_end) != 0) {
        df_txt = doc[i_start:i_end,]
        break
      }

    }

  }
  #we could do some text preprocessing here.

  df_txt <- as_tibble(df_txt) %>%
    #mutate(text = textclean::strip(text)) %>%
    mutate(section = str_search)

  return(df_txt)
}

get_section_text <- function(str_href, str_section, str_search) {
  write_log(str_href)

  df_filing_documents <- filing_documents(str_href)
  str_doc_href <- df_filing_documents[df_filing_documents$type == "10-K" | df_filing_documents$type == "10-Q",]$href
  doc <- parse_filing(str_doc_href)

  df_txt <- doc[grepl(str_section, doc$item.name, ignore.case = TRUE) & grepl(str_search, doc$item.name, ignore.case = TRUE), ] # only discussion for now
  #we could do some text preprocessing here.

  df_txt <- as_tibble(df_txt) %>%
#    mutate(text = textclean::strip(text)) %>%
    mutate(section = str_search)

  return(df_txt)
}


get_document_text <- function(str_ticker, force = FALSE) { #not using force yet
#  str_ticker <- 'MS'
  start_time <- Sys.time()

  write_log(str_ticker)

  str_write_name <- paste0('sec_data_folder/',str_ticker)

  write_log("get filings links ...")

  df_filings <- get_filings_links(str_ticker) %>%
        mutate(ticker = str_ticker)

  write_log_csv(df_filings)

#for debug
  i_test = nrow(df_filings) #for some reason this won't evaulate inside the if statement
  if (i_test == 0) {
      return(NULL)
  }

  write_log("get section text ...")

  df_data <- (df_filings) %>%
    rowwise() %>%
    mutate(nest_discussion = map(.x = href, .f = get_mdna_text)) %>%
    ungroup() %>%
    group_by(period_date) %>%
    arrange(desc(period_date))

  #jenky - find a rowwise application
  a <- df_data %>%
    select(period_date,filing_date,type,form_name,documents,nest_discussion) %>%
    unnest(nest_discussion)

  write_log("write to local csv  ...")
  df_data <- a %>%
    as_tibble() %>%
    write_csv(paste0(str_write_name,".csv"))

  end_time <- Sys.time()

  write_log(end_time - start_time)

  return(df_data)
}

#long run.
df_tickers <- read_csv('implementation_ticker_list.csv')
#df_tickers <- df_tickers %>%
#  filter(Symbol=='DE')

dir.create('sec_data_folder', showWarnings = FALSE)

#file creates a set of csv from ticker list which include metadata & text data.
#df_tickers <- (df_tickers)

future::plan(multiprocess)

#df_data <- map_df(df_tickers$Symbol, get_document_text)

df_data <- future_map_dfr(df_tickers$Symbol, get_document_text,.progress = TRUE)
print('done')
print(head(df_data))
