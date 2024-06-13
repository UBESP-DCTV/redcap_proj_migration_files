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
original_pid <- 208
old_pid <- 33
pid <- 34
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
  str_glue("~/Downloads/{proj}_pid{original_pid}/allegati"),
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
    "~/../Downloads/{proj}_pid{original_pid}/",
    "{proj}_{original_pid}_edocs.csv"
  ),
  setclass = "tibble"
)


get_last_doc_id <- function(redcap_server) {
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

update_meta <- function(original_meta, redcap_server) {
  current_docs <- connect_to_redcap() |>
    dbReadTable(
      Id(
        schema = str_glue("{redcap_server}_redcap"),
        table = "redcap_edocs_metadata"
      )
    ) |>
    tibble::as_tibble() |>
    dplyr::filter(project_id == pid) |>
    dplyr::pull(doc_name) |>
    unique()

  pruned_meta <- original_meta |>
    dplyr::filter(!doc_name %in% current_docs)

  pruned_meta |>
    mutate(
      doc_id = seq_len(nrow(pruned_meta)) +
        get_last_doc_id(redcap_server),
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
update_edocs_metadata <- function() {
  usethis::ui_todo("Start updating metadata: {tic <- lubridate::now()}")
  connect_to_redcap() |>
    dbAppendTable(
      Id(
        schema = str_glue("{redcap_server}_redcap"),
        table = "redcap_edocs_metadata"
      ),
      {
        destination_meta <- original_meta |>
          update_meta(redcap_server)
      }
    )
  usethis::ui_done("Finalized metadata update: {toc <- lubridate::now()}")
  usethis::ui_info("Rows appended: {nrow(destination_meta)}")
  round(toc - tic, 2)
}
update_edocs_metadata()

destination_meta |>
  export(str_glue(
    "~/../Downloads/{proj}_pid{original_pid}/{today_str}-pid{pid}_edocs.csv"
  ))

# csv info table --------------------------------------------------


## info
doc_id_mapping_db <- destination_meta |>
  dplyr::select(doc_id, stored_name) |>
  left_join(
    original_meta |>
      dplyr::select(doc_id, stored_name) |>
      dplyr::mutate(
        stored_name = stored_name |>
          str_replace_all("_pid\\d+_", str_glue("_pid{pid}_"))
      ) |>
      dplyr::rename(
        original_doc_id = doc_id
      )
  )

doc_id_mapping <- purrr::set_names(
  doc_id_mapping_db[["doc_id"]],
  doc_id_mapping_db[["original_doc_id"]]
)



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

# Gli event id sono TUTTI gli eventi della form originale
# Ma noi dobbiamo mappare solo quelli che contengono file allegati
#

# Questi sono gli event id contenenti file allegati
original_info <- import(
  str_glue(
    "~/../Downloads/{proj}_pid{original_pid}/",
    "{proj}_{original_pid}_info.csv"
  ),
  setclass = "tibble"
)


# A MANO!!! dobbiamo assegnare agli event id contenenti file allegati
# i nomi deigli event id originali corrispondenti
#
# qui assegnamo ai nuovi event id i nomi di quelli orginali, in modo da mapparli
# guardando i file di un record A MANO con l éxel di file info  prendere i due id e andare a vedere quali soo gli id degli eventi corrispondenti.
#

stopifnot(
 `devi farlo a mano!!` = {
   event_id_maping <- c(
     `908` = 96, # procedure_file_upload
     `920` = 103 # repeat_angiography_file_upload
   )
   usethis::ui_yeah(
     "Lo hai modificato a mano verificando su REDCap la corrispondenza?!"
  )
 }
)



destination_info <- original_info |>
  mutate(
    project_id = pid,
    event_id = event_id_maping[as.character(.data[["event_id"]])],
    value = doc_id_mapping[as.character(.data[["value"]])]
  ) |>
  select(all_of(c(
    "project_id", "event_id", "record", "field_name", "value", "instance"
  )))

destination_info |>
  export(str_glue(
    "~/../Downloads/{proj}_pid{original_pid}/",
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

# current_redcap_dataN <-  connect_to_redcap() |>
#   dbReadTable(Id(schema = str_glue("{redcap_server}_redcap"), table = redcap_data_tbl))
#
# original_redcap_dataN <- current_redcap_dataN |>
#   filter(project_id != pid)

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


#
#
# connect_to_redcap() |>
#   dbReadTable(Id(schema = str_glue("{redcap_server}_redcap"), table = redcap_data_tbl)) |>
#   count(record)
#
#
#
# connect_to_redcap() |>
#   dbReadTable(Id(schema = str_glue("{redcap_server}_redcap"), table = "redcap_edocs_metadata")) |>
#   as_tibble() |>
#   filter(str_detect(doc_name, "1006-1"))
#
#
# connect_to_redcap() |>
#   dbReadTable(Id(schema = str_glue("{redcap_server}_redcap"), table = redcap_data_tbl)) |>
#   as_tibble() |>
#   filter(str_detect(value, "^163$"))
#

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
