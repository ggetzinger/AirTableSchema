#' Get AirTable Schema
#'
#' Retrieves AirTable schema available to an API key. Bases can be selected
#' using the optinal `names` argument.
#'
#' @param api_key AirTable API Key. If not specified, tries to read from
#'   `.Renviron` (e.g., `Sys.getenv("AIRTABLE_API_KEY")`)
#' @param names Names of bases to retrieve schema for.
#'
#' @return A nested tibble with columns `BaseID`, `name`, `permissionLevel`  and
#' `schema`.
#' @export
#'
get_schema <- function(api_key, names = NULL){
  if (is.null(api_key)) {
    stopifnot("AIRTABLE_API_KEY" %in% names(Sys.getenv()))
    api_key <- Sys.getenv("AIRTABLE_API_KEY")
  }

  bases <- list_bases(api_key)
  if(!is.null(names)){
    bases <- dplyr::filter(bases, name %in% names)
  }
  schema <- purrr::map_dfr(bases$id, ~base_schema(BaseId = ., api_key))

  rst <- dplyr::left_join(bases, schema, by = c("id" = "BaseId")) %>%
    dplyr::rename("BaseId" = "id")

  return(rst)
}

#' List bases
#'
#' Returns the list of bases the API key can access in the order they appear on
#' the user's home screen. The result will be truncated to only include the
#' first 1000 bases (https://airtable.com/api/meta)
#'
#' @param api_key API key
#'
#' @return
#' @export
#'
list_bases <- function(api_key = NULL) {
  if (is.null(api_key)) {
    stopifnot("AIRTABLE_API_KEY" %in% names(Sys.getenv()))
    api_key <- Sys.getenv("AIRTABLE_API_KEY")
  }
  base_url <- "https://api.airtable.com/v0/meta/bases"
  header <-
    httr::add_headers(Authorization = sprintf("Bearer %s", api_key))

  resp <- httr::GET(url = base_url, header)
  if(httr::http_error(resp)) {
    httr::stop_for_status(resp)
  }
  cont <- httr::content(resp, as = "parsed")
  cont_df <- purrr::map_dfr(cont$bases, tibble::as_tibble)
  return(cont_df)
}

#' Base schema
#'
#' Returns the schema of the tables in the specified base
#' (https://airtable.com/api/meta).
#'
#' @param BaseId Base identifier
#' @param api_key API key
#'
#' @return
#' @export
#'
base_schema <- function(BaseId = NULL, api_key = NULL) {
  stopifnot(!is.null(BaseId))
  base_url <- "https://api.airtable.com/v0/meta/bases/%s/tables"
  if (is.null(api_key)) {
    stopifnot("AIRTABLE_API_KEY" %in% names(Sys.getenv()))
    api_key <- Sys.getenv("AIRTABLE_API_KEY")
  }
  header <-
    httr::add_headers(Authorization = sprintf("Bearer %s", api_key))
  resp <- httr::GET(url = sprintf(base_url, BaseId), header)

  if(httr::http_error(resp)) {
    httr::stop_for_status(resp)
  }

  cont <- httr::content(resp, as = "parsed")
  cont_parsed <- purrr::map_dfr(cont$tables, parse_schema) %>%
    dplyr::mutate(BaseId = BaseId) %>%
    tidyr::nest(schema = c(-BaseId))

  return(cont_parsed)
}

parse_schema <- function(x){
    dplyr::bind_cols(
      parse_identifiers(x),
      parse_fields(x),
      parse_views(x)
      )
}

parse_identifiers <- function(x) {
  as_tibble(
    list(
      "id" = purrr::pluck(x, "id"),
      "name" = purrr::pluck(x, "name"),
      "primaryFieldId" = purrr::pluck(x, "primaryFieldId")
    )
  )
}

parse_fields <- function(x){
  purrr::map_dfr(x$fields, dplyr::as_tibble) %>%
    tidyr::nest(fields = everything())
}

parse_views <- function(x){
  purrr::map_dfr(x$views, dplyr::as_tibble) %>%
    tidyr::nest(views = everything())
}
