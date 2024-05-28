library(fs)
library(stringr)
library(rio)
library(dplyr)
library(purrr)
library(DBI)
library(dbplyr)


# NEW project id
old_pid <- 25
pid <- 26


# upload tbls to REDCap's SQL -------------------------------------

con <- dbConnect(
  odbc::odbc(),
  Driver = "mysql",
  Server = "mysql-ubep.mysql.database.azure.com",
  Port = "3306",
  UID = "gregorid@mysql-ubep",
  PWD = rstudioapi::askForPassword("Database password:"),
  Database = "susysafe_v2",
  timeout = 10
)


# attached files --------------------------------------------------

original_files <- fs::dir_ls(
  str_glue("~/Downloads/FAITAVI_pid{old_pid}_to_pid{pid}/allegati"),
  type = "file"
)

destination_files <- original_files |>
  str_replace_all(
    str_glue("(?<=\\d)_pid{old_pid}_"),
    str_glue("_pid{pid}_")
  )

file_move(original_files, destination_files)



# csv edocs metadata table ----------------------------------------

original_meta <- import(
  str_glue("~/../Downloads/FAITAVI_pid{old_pid}_to_pid{pid}/_{old_pid}_edocs.csv"),
  setclass = "tibble"
)


get_last_doc_id <- function(con) {
  con |>
    dbReadTable(
      Id(schema = "edc04_redcap", table = "redcap_docs")
    ) |>
    pull("docs_id") |>
    max()
}

destination_meta <- original_meta |>
  mutate(
    doc_id = seq_len(nrow(original_meta)) + get_last_doc_id(con),
    stored_name = stored_name |>
      str_replace_all("_pid\\d+_", str_glue("_pid{pid}_")),
    project_id = pid,
    # This is supposed to signal warnings: all NULL must be converted to
    # double NAs, while other possibly real content should be converted
    # from character to double.
    delete_date = as.double(delete_date),
    date_deleted_server = as.double(date_deleted_server)
  )

destination_meta |>
  export(str_glue(
    "~/../Downloads/FAITAVI_pid{old_pid}_to_pid{pid}/20240527-pid{pid}_edocs.csv"
  ))


## edocs_metadata
con |>
  dbAppendTable(
    Id(schema = "edc04_redcap", table = "redcap_edocs_metadata"),
    destination_meta
  )


# csv info table --------------------------------------------------


## info
## NOTA: la tabella SQL è da prendere dalla tabella projects
redcap_data_tbl <- con |>
  dbReadTable(Id(schema = "edc04_redcap", table = "redcap_projects")) |>
  # IMPORTANT: This is the project ID!!
  filter(project_id == pid) |>
  pull(data_table)

redcap_event_id <- con |>
  dbReadTable(Id(schema = "edc04_redcap", table = redcap_data_tbl)) |>
  filter(project_id == pid) |>
  pull(event_id) |>
  unique()

stopifnot(
  `!!! Manualy investigation on event_id is required !!!` =
    length(redcap_event_id) == 1
)

doc_id_mapping <- destination_meta[["doc_id"]] |>
  set_names(original_meta[["doc_id"]])

original_info <- import(
  str_glue("~/../Downloads/FAITAVI_pid{old_pid}_to_pid{pid}/_{old_pid}_info.csv"),
  setclass = "tibble"
)

if (length(redcap_event_id) == 1) {

  destination_info <- original_info |>
    mutate(
      project_id = pid,
      event_id = redcap_event_id,
      value = doc_id_mapping[as.character(.data[["value"]])]
    ) |>
    select(all_of(c(
      "project_id", "event_id", "record", "field_name", "value", "instance"
    )))

  destination_info |>
    export(str_glue(
      "~/../Downloads/FAITAVI_pid{old_pid}_to_pid{pid}/20240520-pid{pid}_info.csv"
    ))

  con |>
    dbAppendTable(
      Id(schema = "edc04_redcap", table = redcap_data_tbl),
      destination_info |>
        mutate(instance = NA_integer_)
    )

}


current_redcap_dataN <- con |>
  dbReadTable(Id(schema = "edc04_redcap", table = redcap_data_tbl))

original_redcap_dataN <- current_redcap_dataN |>
  filter(project_id != pid)

#
# con |>
#   dbRemoveTable(
#     Id(schema = "edc04_redcap", table = redcap_data_tbl)
#   )
#
# con |>
#   dbCreateTable(
#     Id(schema = "edc04_redcap", table = redcap_data_tbl),
#     original_redcap_dataN
#   )
#
# con |>
#   dbAppendTable(
#     Id(schema = "edc04_redcap", table = redcap_data_tbl),
#     original_redcap_dataN
#   )
#



# just explore ----------------------------------------------------

con |>
  dbReadTable(Id(schema = "edc04_redcap", table = redcap_data_tbl)) |>
  count(record)



con |>
  dbReadTable(Id(schema = "edc04_redcap", table = "redcap_edocs_metadata")) |>
  as_tibble() |>
  filter(str_detect(doc_name, "1006-1"))


con |>
  dbReadTable(Id(schema = "edc04_redcap", table = redcap_data_tbl)) |>
  as_tibble() |>
  filter(str_detect(value, "^163$"))

