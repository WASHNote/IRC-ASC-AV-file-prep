library(dplyr)
library(airtabler)
library(rairtable)
library(here)

#devtools::install_github("bergant/airtabler", force = TRUE)

# structure of the folders is:
# room / day / session / author / files

get_asc_base_id <- function() {
  "appbsLMgFDD5pEcSw"
}

get_content_table_id <- function() {
  "tblr37SxOsy6K9J3N"
}

get_event_table_id <- function() {
  "tblw4Q2RsmrFHpi3L"
}

get_content_files_table_id <- function() {
  "tblr37SxOsy6K9J3N"
}

get_latest_versions_view_id <- function() {
  "viwaVA1KPcgMjVxbg"
}

Sys.setenv("AIRTABLE_API_KEY"=Sys.getenv(x = "ASC_DOWNLOAD"))
rairtable::set_airtable_api_key(Sys.getenv(x = "ASC_DOWNLOAD"), install = FALSE)

asc_content <- airtabler::airtable(
  base = get_asc_base_id(),
  tables = c("Event", "Content", "Content files")
)

latest_file_view <- rairtable::airtable(
  "Content files",
  base = get_asc_base_id(),
  view = get_latest_versions_view_id())

content_table <- rairtable::airtable(
  "Content",
  base = get_asc_base_id()
)

person_table <- rairtable::airtable(
  "Person",
  base = get_asc_base_id()
)

event_table <- rairtable::airtable(
  "Event",
  base = get_asc_base_id()
)


resource_table <- rairtable::airtable(
  "Resource",
  base = get_asc_base_id()
)

latest_files <- read_airtable(latest_file_view)
content <- read_airtable(content_table)
person <- read_airtable(person_table)

event <- read_airtable(event_table)

resource <- read_airtable(resource_table)

# Process
# List latest files
# Fetch the author
# Fetch the event
# Fetch the day
# Add the urls to a column


all_files <- latest_files %>%
  mutate(url = purrr::map(Attachments, function(x) {x$url})) %>%
  filter(lengths(Content)>0) %>% mutate(content_id = unlist(Content)) %>%
  select(
    airtable_record_id,
    latest_name = Name,
    Attachments,
    Created,
    content_id
  ) %>%
  left_join(
    content %>%
      select(airtable_record_id, Event, `Content author`) %>%
      filter(lengths(Event)>0, lengths(`Content author`)>0) %>%
      mutate(
        event_id = unlist(Event),
        author_id = unlist(`Content author`)
        ),
    by = c("content_id" = "airtable_record_id")) %>%
  left_join(
    person %>%
      select(airtable_record_id, author_name = Name),
    by = c("author_id" = "airtable_record_id")
  ) %>%
  left_join(
    event %>%
      select(airtable_record_id, event_name = Name, Day, theme_code = `Theme code`, event_code = `Event code`, Room) %>%
      mutate(full_event_code = paste0(theme_code, ".",event_code)) %>%
      mutate(room_id = unlist(purrr::map(Room, function(x) {
        if (length(x)==1) {
          x[[1]]
        } else if (length(x)==0) {
          NA_character_
        } else {
          stop("More than one room for an event.")
        }
      }))),
    by = c("event_id" = "airtable_record_id")
  ) %>%
  left_join(
    resource %>%
      select(airtable_record_id, room_name = Name),
    by = c("room_id" = "airtable_record_id")
  ) %>%
  mutate(path = paste(
    "output",
    stringr::str_replace_all(make.names(room_name), pattern = "[.]+", replacement = "_"),
    Day,
    full_event_code,
    stringr::str_sub(stringr::str_replace_all(make.names(author_name), pattern = "[.]+", replacement = "_"),1L, 80L),
    sep = "/"
  ))

##asc_content$`Content files`$select_all()

paths <- here(unique(all_files$path))

unlink(here("output"), recursive=TRUE)

lapply(paths, function(x) {
  dir.create(x, recursive = TRUE)
})

for (i in 1:nrow(all_files)) {
  x <- all_files[i,]
  x_file <- "::::"
  attachments <- x$Attachments
  lapply(attachments, function(y) {
    for (j in 1:nrow(y)) {
      if (!grepl(pattern = "\\.(pptx)|(mp4)|(pdf)|(jpg)$", x = y[j,]$filename, ignore.case = TRUE)) {
        if (y[j,]$type=="application/vnd.openxmlformats-officedocument.presentationml.presentation") {
          x_file <- paste0(y[j,]$filename,".pptx")
        } else if (y[j,]$type=="video/mp4") {
          x_file <- paste0(y[j,]$filename,".mp4")
        } else{
          cat(y[j,]$filename,"\n")
          cat(y[j,]$type,"\n")
          stop("Not a recognized file type and no extension")
        }
      } else {
        x_file <- y[j,]$filename
      }
      cat(y[j,]$filename,"in: \n")
      cat(x$path,"\n\n")
      httr::GET(y[j,]$url, httr::write_disk(paste0(x$path, "/", x_file), overwrite=TRUE))
    }
  })
}

