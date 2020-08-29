library(tidytext)

LOGFILE = format(Sys.time(), "%b_%d_%Y.log")
print(LOGFILE)

DF_FILTER_LIST <- data.frame(
  start_text = c('Introduction',
                 'FUNCTIONAL EARNINGS', 
                 'DISCUSSION AND ANALYSIS',
                 'DISCUSSION AND ANALYSIS',
                 'DISCUSSION AND ANALYSIS',
                 'OVERVIEW',
                 'Business Overview',
                 'Financial Review',
                 'RESULTS OF OPERATIONS',
                 'Overview',
                 'Entergy operates',
                 "MANAGEMENT\'S FINANCIAL DISCUSSION",
                 'General',
                 "Management's Discussion",
                 'EXECUTIVE SUMMARY',
                 'EXECUTIVE OVERVIEW',
                 'EXECUTIVE OVERVIEW',
                 'The following management discussion and analysis',
                 'CURRENT ECONOMIC CONDITIONS',
                 'Overview and Highlights',
                 'Financial Review - Results of Operations'),
  end_text = c('Quantitative and qualitative disclosures about market risk',
               "MANAGEMENT\'S REPORT",
               'RISK FACTORS',
               'FIVE-YEAR PERFORMANCE GRAPH',
               'FINANCIAL STATEMENTS AND NOTES',
               'Risk management includes the identification',
               'Selected Loan Maturity Data',
               'Risk Management',
               'QUANTITATIVE AND QUALITATIVE DISCLOSURES',
               'Forward-Looking Statements',
               'New Accounting Pronouncements',
               'New Accounting Pronouncements',
               'Website information',
               'Risk Disclosures',
               'RISK FACTORS',
               'A summary of contractual obligations is included',
               'CONSOLIDATED RESULTS OF OPERATIONS',
               'NON-GAAP FINANCIAL MEASURES',
               'FORWARD-LOOKING STATEMENTS',
               'Critical Accounting Policies and Estimates',
               'Unregistered Sales of Equity Securities and Use of Proceeds')
)


#CSVFILE = format(Sys.time(), "%b_%d_%Y.csv")
CSVFILE = format(Sys.time(), "file_index.csv")
print(CSVFILE)

get_filings_links <-function(str_ticker) {
  #str_ticker <- "AXP"
  df_filings <- company_filings(str_ticker, type = "10-", count = 20)
  df_filings <- df_filings[df_filings$type == "10-K" | df_filings$type == "10-Q", ]
  df_filing_infos <- map_df(df_filings$href, filing_information)
  df_filings <- bind_cols(df_filings, df_filing_infos)
  return(head(as_tibble(df_filings),20))
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

get_mdna_text <- function(str_ticker) {
  #str_ticker <- 'AXP'
  
  df_filing_documents <- read_csv('file_index.csv') %>%
    filter(ticker == str_ticker)

  str_section = 'item 2|item 7'
  str_search = 'discussion'
  
  df_filing_documents <- df_filing_documents[df_filing_documents$type...7 == "10-K" | df_filing_documents$type...7 == "10-Q",]
  print(df_filing_documents$href)
  #a_row = 1
  for (a_row in 1:nrow(df_filing_documents)) {
    #df_filing_documents[a_row,'href']
    str_doc_href <- df_filing_documents[a_row, "href"]

    file_end <- gsub("https://www.sec.gov",'',str_doc_href)
    file_end <- strsplit(file_end,'/')[[1]][1:6]
    file_end <- paste(file_end, collapse='/' )
    file_end = paste0(getwd(),'/',file_end)
    file.ls <- list.files(path=file_end,pattern="sentences")
    file_name = paste0(file_end,'/',file.ls)
    
    print(file_name)
    
    result <- try({
      df_txt <- read_csv(file_name)
    }, silent = TRUE)
    df_txt <- df_txt[grepl(str_section, df_txt$item.name, ignore.case = TRUE) & grepl(str_search, df_txt$item.name, ignore.case = TRUE), ] # only discussion for now
    i_start = ''
    i_end = ''
    
    if (nrow(df_txt) ==0) {
    #b_row = 1
    for (b_row in 1:nrow(DF_FILTER_LIST)) { #should flip this to apply()
      
        start_text <- DF_FILTER_LIST[b_row, "start_text"]
        end_text <- DF_FILTER_LIST[b_row, "end_text"]
      
        write_log(paste0('trying ',start_text))
        write_log(paste0('to ',end_text))
      
        i_start = as.integer(which(grepl(start_text, df_txt$text))) 
        if (length(i_start) > 1) { #handle table of contents duplicates
          i_start = i_start[2]
        }
        i_end = as.integer(which(grepl(end_text, df_txt$text)))
        if (length(i_end) > 1) {
          i_end = i_end[2]
        }
      
        write_log(i_start)
        write_log(i_end)
      
        if (length(i_start) != 0 & length(i_end) != 0) {
          if (i_start < i_end) {        
            print(paste0('istart is:',i_start,' iend is:',i_end))
            df_txt = df_txt[i_start:i_end,]
            break
          }
        }
      } 
    }
    
    if (length(i_start) == 0 || length(i_end) == 0) {
        write_log(paste0("missing section for:",str_ticker," ",str_doc_href))
        write_log(str_doc_href)
    }
    
    mdna_file_name <- gsub('_sentences.csv','_mdna.csv',file_name)
    df_txt <- as_tibble(df_txt) %>%
#      unnest_tokens(sentence_text,sentence_text,token='sentences') %>%
      mutate(section = str_search) %>%
      write_csv(mdna_file_name)
    }
#df_txt
#mdna_file_name
}

get_ticker_text <- function(str_ticker, force = FALSE) { #not using force yet
  # get the full text of filing and save as a csv
#  dir.create('sec_data_folder', showWarnings = FALSE)
  #str_ticker = "AXP"
  start_time <- Sys.time()
  
  write_log(str_ticker)
  
#  str_write_name <- paste0('sec_data_folder/',str_ticker)
  
  write_log("get filings links ...")
  
#  filings_csv <- paste0(str_write_name,"_filings.csv")
  
  write_log("from sec ...")

  df_filings <- get_filings_links(str_ticker) %>%
      mutate(ticker = str_ticker) #%>%
#      write_csv(filings_csv) 
  
  write_log_csv(df_filings)
  
  #out <- tryCatch({
  df_filings %>%
    rowwise() %>%
    mutate(nest_discussion = list(map2_dfr(href,ticker,get_documents_text)))
  #},
  #error=function(cond) {
  #   message(cond)
  #   return(NA)
  # },
  # warning=function(cond) {
  #   message(cond)
  #   return(NULL)
  # },
  # finally={
  #   message(paste("Processed ticker:", str_ticker))
  # }
  # )    
  # 
  end_time <- Sys.time()
  write_log(end_time - start_time)
  
}

get_string_file_name <- function(str_href) {
  #str_href <- 'https://www.sec.gov/Archives/edgar/data/4962/000000496220000079/0000004962-20-000079-index.htm'
  #str_href <- "https://www.sec.gov/Archives/edgar/data/4962/000000496220000079/axp-20200630.htm"
  file_path <- strsplit(str_href,'/')
  str_file_path <- ''
  for (i in 5:length(file_path[[1]])-1) {
    str_file_path = paste0(str_file_path,"/",(file_path[[1]][i]))
  }
  str_file_path <- paste0(getwd(),"/",str_file_path)
  dir.create(str_file_path,recursive = TRUE)
  str_file_name <- ''
  file_path = strsplit(str_href,'/')
  for (i in 4:length(file_path[[1]])) {
    str_file_name = paste0(str_file_name,"/",(file_path[[1]][i]))
  }
  str_file_name <- paste0(getwd(),str_file_name)
  str_file_name <- gsub(".htm",".csv",str_file_name)
  return(str_file_name)
}

# get_document_text <- function(str_ticker, force = FALSE) { #not using force yet
#   start_time <- Sys.time()
#   
#   write_log(str_ticker)
#   
#   #str_write_name <- paste0('sec_data_folder/',str_ticker)
#   
#   write_log("get filings links ...")
#   
#   filings_csv <- paste0(str_write_name,"_filings.csv")
#   
#   if (file.exists(filings_csv)) {  #add force equals true
#     write_log("from cache ...")
#     
#     df_filings <- read_csv(filings_csv,col_types = cols()) 
#     df_filings <- df_filings %>% mutate_if(is.logical, as.character)
#   } else {
#     write_log("from sec ...")
#     
#     df_filings <- get_filings_links(str_ticker) %>%
#       mutate(ticker = str_ticker) %>%
#       write_csv(filings_csv)
#   }
#   
#   write_log_csv(df_filings)
#   
#   #for debug
#   i_test = nrow(df_filings) #for some reason this won't evaulate inside the if statement
#   if (i_test == 0) {
#     return(NULL)
#   }
#   
# 
#   end_time <- Sys.time()
#   
#   write_log(end_time - start_time)
#   
#   return(df_data)
# }

get_documents_text <- function(str_href,str_ticker) {
  #str_href = "https://www.sec.gov/Archives/edgar/data/4962/000000496220000079/0000004962-20-000079-index.htm"

  write_log(str_href)

  
  df_filing_documents <- filing_documents(str_href)
  str_doc_href <- df_filing_documents[df_filing_documents$type == "10-K" | df_filing_documents$type == "10-Q",]$href
  file_doc = get_string_file_name(str_doc_href)  
  
  html_doc <- gsub('.csv','.html',file_doc)
  download.file(str_doc_href, html_doc)
  
  doc <- parse_filing(str_doc_href)
  
  df_txt <- doc
  
  file_doc = get_string_file_name(str_doc_href)
  
  #full_doc <- gsub('.csv','_document.csv',file_doc)
  #df_txt <- as_tibble(df_txt) %>%
  #  mutate(href = str_href) %>%
  #  mutate(ticker = str_ticker) %>%
  #  write_csv(full_doc)
  
  file_doc <- gsub('.csv','_sentences.csv',file_doc)
  df_txt <- as_tibble(df_txt) %>%
    unnest_tokens(sentence_text,text,token='sentences') %>%
    mutate(sentence_id = row_number()) %>%
    mutate(href = str_href) %>%
    mutate(ticker = str_ticker) %>%
    write_csv(file_doc)
  
  return(df_txt)
}

get_riskfactors_text <- function(str_ticker) {
  str_ticker <- 'AXP'
  
  df_filing_documents <- read_csv('file_index.csv') %>%
    filter(ticker == str_ticker)
  
  str_section = 'item 1a'
  str_search = 'risk_factors'
  
  df_filing_documents <- df_filing_documents[df_filing_documents$type...7 == "10-K",]
  
  #df_filing_documents
  
  #a_row = 1
  for (a_row in 1:nrow(df_filing_documents)) {
    #df_filing_documents[a_row,'href']
    str_doc_href <- df_filing_documents[a_row, "href"]
    file_end <- gsub("https://www.sec.gov",'',str_doc_href)
    file_end <- strsplit(file_end,'/')[[1]][1:6]
    file_end <- paste(file_end, collapse='/' )
    file_end = paste0(getwd(),'/',file_end)
    file.ls <- list.files(path=file_end,pattern="sentences")
    file_name = paste0(file_end,'/',file.ls)
    
    result <- try({
      df_txt <- read_csv(file_name)
    }, silent = TRUE)
    #df_txt %>%
    #  group_by(item.name) %>%
    #  summarise(n=n())
    
    df_txt <- df_txt[grepl(str_section, df_txt$item.name, ignore.case = TRUE), ] # only discussion for now
    
    if (nrow(df_txt) == 0) {
      write_log(paste0("missing risk factor section for:",str_ticker," ",str_doc_href))
      write_log(str_doc_href)
      return(NA)
    }
    
    rf_file_name <- gsub('_sentences.csv','_riskfactors.csv',file_name)
    df_txt <- as_tibble(df_txt) %>%
      mutate(section = str_search) %>%
      write_csv(rf_file_name)
  }
  #df_txt
  #mdna_file_name
}
