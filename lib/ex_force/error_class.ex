defmodule ExForce.ErrorClass do
  @moduledoc """
  Single source of truth for classifying Salesforce errors into retry policy.

  Classes:

    * `:permanent`       - retrying can never succeed (revoked auth, bad
                           credentials, malformed query). Requires human or
                           customer action (re-authentication / admin fix).
    * `:throttled`       - rate or quota limit. Retryable, but only on a long
                           backoff (minutes to hours), never the fast retry.
    * `:refresh_session` - access token stale (`INVALID_SESSION_ID`). Refresh
                           the token once and retry the request once; never loop.
    * `:transient`       - network/pool/5xx blip. Safe to retry soon.

  Sources (verified against Salesforce docs, 2026-07):

    * OAuth token endpoint errors: Salesforce OAuth flow error tables
      (HTTP 400 + `{"error", "error_description"}` body).
    * REST API errors: "Status Codes and Error Responses", REST API Developer
      Guide (`[{"errorCode", "message"}]` body).

  Unknown shapes default to `:transient` - it is safer to retry something dead
  than to kill something alive. Unknowns are logged so this table can be
  extended from evidence.
  """

  require Logger

  # OAuth token endpoint `error` values that can never succeed on retry.
  # - invalid_grant: expired/revoked refresh token, authentication failure
  #   (password change or deactivated user revokes all tokens), IP restricted
  #   or invalid login hours, unapproved consumer.
  # - invalid_app_acess: Salesforce's own error tables contain this
  #   misspelling; both spellings are matched defensively.
  # - inactive_org: org locked/closed/suspended; requires customer/Salesforce
  #   action and a reconnect afterwards.
  @permanent_oauth ~w(
    invalid_grant
    invalid_client
    invalid_client_id
    invalid_app_access
    invalid_app_acess
    inactive_user
    inactive_org
    invalid_scope
    invalid_request
    unsupported_grant_type
  )

  # REST API errorCode values where retrying is pointless: the query/config is
  # wrong or the org has the API disabled.
  @permanent_api ~w(
    MALFORMED_QUERY
    MALFORMED_ID
    INVALID_FIELD
    INVALID_TYPE
    NOT_FOUND
    API_DISABLED_FOR_ORG
    INVALID_QUERY_FILTER_OPERATOR
  )

  # REST API errorCode values documented as retryable: UNABLE_TO_LOCK_ROW
  # (409, row-lock contention), SERVER_UNAVAILABLE (503), UNKNOWN_EXCEPTION (500).
  @transient_api ~w(
    UNABLE_TO_LOCK_ROW
    SERVER_UNAVAILABLE
    UNKNOWN_EXCEPTION
  )

  @type class :: :permanent | :throttled | :refresh_session | :transient

  @doc """
  Classify any error reason produced by `ExForce.OAuth`, `ExForce` REST calls,
  or the transport layer into a retry class.
  """
  @spec classify(term()) :: class()
  # OAuth token endpoint errors: {:error, %{"error" => code}}
  def classify(%{"error" => code}) when code in @permanent_oauth, do: :permanent
  # "Number of logins exceeded" - a refresh storm can trigger this; long backoff.
  def classify(%{"error" => "rate_limit_exceeded"}), do: :throttled
  def classify(%{"error" => _}), do: :transient

  # REST API errors: list of %{"errorCode" => code} - classified by the most
  # severe entry (a batch containing any permanent error is permanent).
  def classify([%{"errorCode" => _} | _] = errors) do
    errors
    |> Enum.map(&classify_api_code(Map.get(&1, "errorCode")))
    |> Enum.min_by(&severity/1)
  end

  def classify(%{"errorCode" => code}), do: classify_api_code(code)

  # Wrapped shapes produced by our own layers.
  def classify({:error, reason}), do: classify(reason)
  # timed_step converts raises (e.g. Finch pool checkout timeout) to this shape.
  def classify({:exception, _msg}), do: :transient
  # Transport-level errors surfaced by the Tesla/Finch adapter.
  def classify(%{__struct__: struct}) when struct in [Mint.TransportError, Finch.Error],
    do: :transient

  def classify(reason) when reason in [:timeout, :closed, :econnrefused, :nxdomain],
    do: :transient

  def classify(other) do
    Logger.info(
      "ExForce.ErrorClass: unclassified error, treating as transient: #{inspect(other)}"
    )

    :transient
  end

  defp classify_api_code("INVALID_SESSION_ID"), do: :refresh_session
  # Daily org API quota (403); retry in minutes/hours, never seconds.
  defp classify_api_code("REQUEST_LIMIT_EXCEEDED"), do: :throttled
  defp classify_api_code(code) when code in @permanent_api, do: :permanent
  defp classify_api_code(code) when code in @transient_api, do: :transient
  defp classify_api_code(_), do: :transient

  # Lower = more severe, used to pick the dominant class for error batches.
  defp severity(:permanent), do: 0
  defp severity(:refresh_session), do: 1
  defp severity(:throttled), do: 2
  defp severity(:transient), do: 3
end
