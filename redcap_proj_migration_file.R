library(fs)

library(stringr)
library(dplyr)
library(purrr)
library(lubridate)

library(rio)

library(DBI)
library(dbplyr)


# NEW project id
proj <- "IT_MASTERS"
old_pid <- 208
pid <- 33
redcap_server <- "edc04"
today_str <- today() |>
  str_remove_all("-")


# upload tbls to REDCap's SQL -------------------------------------

get_or_ask_sql_psw <- function() {
  env_psw <- Sys.getenv("REDCAP_SQL_PSW")
  if (env_psw == "") {
    rstudioapi::askForPassword("SQL password:")
  } else {
    env_psw
  }
}

connect_to_redcap <- function() {
  dbConnect(
    odbc::odbc(),
    Driver = "mysql",
    Server = "mysql-ubep.mysql.database.azure.com",
    Port = "3306",
    UID = "gregorid@mysql-ubep",
    PWD = get_or_ask_sql_psw(),
    Database = "susysafe_v2",
    timeout = 10
  )
}

# attached files --------------------------------------------------

original_files <- dir_ls(
  str_glue("~/Downloads/{proj}_pid{old_pid}_to_pid{pid}/allegati"),
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
  str_glue(
    "~/../Downloads/{proj}_pid{old_pid}_to_pid{pid}/",
    "{proj}_{old_pid}_edocs.csv"
  ),
  setclass = "tibble"
)


get_last_doc_id <- function(con, redcap_server) {
  connect_to_redcap() |>
    dbReadTable(
      Id(
        schema = str_glue("{redcap_server}_redcap"),
        table = "redcap_edocs_metadata"
      )
    ) |>
    pull("doc_id") |>
    max()
}

update_meta <- function(original_meta, con, redcap_server) {
  original_meta |>
    mutate(
      doc_id = seq_len(nrow(original_meta)) +
        get_last_doc_id(con, redcap_server),
      stored_name = stored_name |>
        str_replace_all("_pid\\d+_", str_glue("_pid{pid}_")),
      project_id = pid,
      # This is supposed to signal warnings: all NULL must be converted to
      # double NAs, while other possibly real content should be converted
      # from character to double.
      delete_date = as.double(delete_date),
      date_deleted_server = as.double(date_deleted_server)
    )
}


## edocs_metadata
connect_to_redcap() |>
  dbAppendTable(
    Id(
      schema = str_glue("{redcap_server}_redcap"),
      table = "redcap_edocs_metadata"
    ),
    {
      destination_meta <- original_meta |>
        update_meta(con, redcap_server)
    }
  )

destination_meta |>
  export(str_glue(
    "~/../Downloads/{proj}_pid{old_pid}_to_pid{pid}/{today_str}-pid{pid}_edocs.csv"
  ))

# csv info table --------------------------------------------------


## info
## NOTA: la tabella SQL è da prendere dalla tabella projects
redcap_data_tbl <-  connect_to_redcap() |>
  dbReadTable(
    Id(
      schema = str_glue("{redcap_server}_redcap"),
      table = "redcap_projects"
    )
  ) |>
  # IMPORTANT: This is the project ID!!
  filter(project_id == pid) |>
  pull(data_table)


redcap_event_id <-  connect_to_redcap() |>
  dbReadTable(
    Id(
      schema = str_glue("{redcap_server}_redcap"),
      table = redcap_data_tbl
    )
  ) |>
  filter(project_id == pid) |>
  pull(event_id) |>
  unique() |>
  sort()


doc_id_mapping <- destination_meta[["doc_id"]] |>
  set_names(original_meta[["doc_id"]])

original_event_id_mapping <- unique(original_info[["event_id"]]) |>
  (\(x) set_names(order(x), x))()



original_info <- import(
  str_glue(
    "~/../Downloads/{proj}_pid{old_pid}_to_pid{pid}/",
    "{proj}_{old_pid}_info.csv"
  ),
  setclass = "tibble"
)

destination_info <- original_info |>
  mutate(
    project_id = pid,
    event_id = redcap_event_id[
      original_event_id_mapping[as.character(original_info[["event_id"]])]
    ],
    value = doc_id_mapping[as.character(.data[["value"]])]
  ) |>
  select(all_of(c(
    "project_id", "event_id", "record", "field_name", "value", "instance"
  )))

destination_info |>
  export(str_glue(
    "~/../Downloads/{proj}_pid{old_pid}_to_pid{pid}/",
    "{today_str}-pid{pid}_info.csv"
  ))

connect_to_redcap() |>
  dbAppendTable(
    Id(
      schema = str_glue("{redcap_server}_redcap"),
      table = redcap_data_tbl
    ),
    destination_info |>
      mutate(instance = as.double(instance))
  )

# just explore ----------------------------------------------------

current_redcap_dataN <-  connect_to_redcap() |>
  dbReadTable(Id(schema = str_glue("{redcap_server}_redcap"), table = redcap_data_tbl))

original_redcap_dataN <- current_redcap_dataN |>
  filter(project_id != pid)

#
#  connect_to_redcap() |>
#   dbRemoveTable(
#     Id(schema = str_glue("{redcap_server}_redcap"), table = redcap_data_tbl)
#   )
#
#  connect_to_redcap() |>
#   dbCreateTable(
#     Id(schema = str_glue("{redcap_server}_redcap"), table = redcap_data_tbl),
#     original_redcap_dataN
#   )
#
#  connect_to_redcap() |>
#   dbAppendTable(
#     Id(schema = str_glue("{redcap_server}_redcap"), table = redcap_data_tbl),
#     original_redcap_dataN
#   )
#




connect_to_redcap() |>
  dbReadTable(Id(schema = str_glue("{redcap_server}_redcap"), table = redcap_data_tbl)) |>
  count(record)



connect_to_redcap() |>
  dbReadTable(Id(schema = str_glue("{redcap_server}_redcap"), table = "redcap_edocs_metadata")) |>
  as_tibble() |>
  filter(str_detect(doc_name, "1006-1"))


connect_to_redcap() |>
  dbReadTable(Id(schema = str_glue("{redcap_server}_redcap"), table = redcap_data_tbl)) |>
  as_tibble() |>
  filter(str_detect(value, "^163$"))


# filtered <-  connect_to_redcap() |>
#   dbReadTable(
#     Id(
#       schema = str_glue("{redcap_server}_redcap"),
#       table = redcap_data_tbl
#     )
#   ) |>
#   filter(str_detect(field_name, "^please", negate = TRUE)) |>
#   as_tibble()
#  connect_to_redcap() |>
#   dbWriteTable(
#     "edc04_redcap.redcap_data",
#     filtered,
#     overwrite = TRUE
#   )
