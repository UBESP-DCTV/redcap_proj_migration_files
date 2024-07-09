library(fs)

library(stringr)
library(dplyr)
library(purrr)
library(lubridate)

library(rio)

library(DBI)
library(dbplyr)


# NEW project id
proj <- rstudioapi::showPrompt(
  "REDCap info set",
  "Inserisci il nome del progetto REDcap",
  if (exists("proj")) proj else ""
)

original_pid <- rstudioapi::showPrompt(
  "REDCap info set",
  "Inserisci il project id del progetto originale",
  if (exists("original_pid")) original_pid else ""
) |>
  as.integer()


old_pid <- rstudioapi::showPrompt(
  "REDCap info set",
  "Inserisci il project id dell'ultimo progetto importato (se è la prima volta, inserisci l'original_pid)",
  if (exists("old_pid")) old_pid else ""
) |>
  as.integer()

pid <- rstudioapi::showPrompt(
  "REDCap info set",
  "Inserisci il project id del nuovo progetto su cui importarlo",
  if (exists("pid")) pid else ""
) |>
  as.integer()

redcap_server <- rstudioapi::showPrompt(
  "REDCap info set",
  "Inserisci il server REDCap (es. 'susysafe')",
  if (exists("redcap_server")) redcap_server else ""
)

today_str <- today() |>
  str_remove_all("-")


stopifnot(
  `Devi controllare che i record siano associati ai DAG!!` =
  usethis::ui_yeah(
    "Hai controllato su REDCap ({redcap_server}) che, nel progetto {proj} (pid {pid}), i record siano stati correttaemnte associati ai corrispondenti DAG?"
  )
)




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
      # All "NULL" characters must
      # be converted to double NAs, while other possibly real content
      # should be converted from character to double.
      delete_date = delete_date |>
        str_replace_all("NULL", NA_character_) |>
        as.double(),
      # same for date_deleted_server
      date_deleted_server = date_deleted_server |>
        str_replace_all("NULL", NA_character_) |>
        as.double()
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
        destination_meta <<- original_meta |>
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

original_info |>
  dplyr::select(event_id, field_name) |>
  unique() |>
  dplyr::slice_head(n = 1, by = event_id)

# A MANO!!! dobbiamo assegnare agli event id contenenti file allegati
# i nomi deigli event id originali corrispondenti
#
# qui assegnamo ai nuovi event id i nomi di quelli orginali, in modo da mapparli
# guardando i file di un record A MANO con l éxel di file info  prendere i due id e andare a vedere quali soo gli id degli eventi corrispondenti.
#

stopifnot(
 `Devi farlo a mano!!` = {
   event_id_mapping <- c(
     `908` = 70,
     `920` = 77
   )
   usethis::ui_yeah(
     "Questa l'attuale corrisppondenza:
{str_c('old: ', names(event_id_mapping), '--> new: ', event_id_mapping, collapse = '\n')}

Lo hai modificato a mano verificando su REDCap la corrispondenza?!"
  )
 }
)



destination_info <- original_info |>
  mutate(
    project_id = pid,
    event_id = event_id_mapping[as.character(.data[["event_id"]])],
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
      mutate(
        instance = instance |>
          str_replace_all("NULL", NA_character_) |>
          as.double()
      )
  )
