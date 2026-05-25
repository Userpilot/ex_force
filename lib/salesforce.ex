defmodule Salesforce do
  @moduledoc """
  Per-app Salesforce OAuth client + config cache.

  The GenServer is a writer/coordinator only. State lives in a public ETS
  table so `get_app/1` is a lock-free, microsecond, concurrent read.
  All HTTPS work (register/refresh) runs in unlinked Tasks; each Task writes
  its result directly into ETS and self-schedules the next refresh, so the
  singleton mailbox never blocks on network I/O.
  """

  require Logger
  use GenServer

  @table :salesforce_apps

  @refresh_interval_ms 2 * 60 * 60 * 1000
  @refresh_jitter_ms 15 * 60 * 1000
  @refresh_retry_ms 5 * 60 * 1000

  #
  # External API
  #

  def get_app(app_token) when is_binary(app_token) do
    case :ets.lookup(@table, app_token) do
      [{^app_token, app}] -> app
      [] -> nil
    end
  rescue
    # Table not yet created (boot race) — treat as cache miss.
    ArgumentError -> nil
  end

  def register_app_token(app), do: GenServer.call(__MODULE__, {:register_app, app}, 30_000)

  def refresh_app_token(config),
    do: GenServer.call(__MODULE__, {:refresh_token, config}, 30_000)

  # Client

  def start_link(callback_fun) do
    GenServer.start_link(__MODULE__, callback_fun, name: __MODULE__)
  end

  def child_spec(callback_fun) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [callback_fun]}}
  end

  # Server (callbacks)

  @impl true
  def init(callback_fun) when is_function(callback_fun, 0) do
    :ets.new(@table, [
      :named_table,
      :public,
      :set,
      read_concurrency: true,
      write_concurrency: true
    ])

    {:ok, callback_fun, {:continue, :initial_load}}
  end

  @impl true
  def handle_continue(:initial_load, callback_fun) do
    # Fire all initial refreshes immediately, in parallel. Tasks run independently;
    # each one HTTPS-refreshes its tenant and writes the resulting client+config
    # directly into ETS before scheduling the next periodic refresh.
    for app <- safe_callback(callback_fun) do
      spawn_refresh(app.config)
    end

    {:noreply, callback_fun}
  end

  @impl true
  def handle_info({:do_refresh, config}, state) do
    spawn_refresh(config)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def handle_call({:register_app, app}, from, state) do
    Task.start(fn ->
      case init_client(app) do
        {:ok,
         %{
           client: client,
           response: %{refresh_token: refresh_token, access_token: access_token} = response
         }} ->
          config = Map.merge(app, %{refresh_token: refresh_token, access_token: access_token})
          :ets.insert(@table, {app.app_token, %{client: client, config: config}})
          Process.send_after(__MODULE__, {:do_refresh, config}, jittered_refresh_ms())
          GenServer.reply(from, response)

        {:error, reason} ->
          GenServer.reply(from, {:error, reason})
      end
    end)

    {:noreply, state}
  end

  def handle_call({:refresh_token, config}, from, state) do
    Task.start(fn ->
      case refresh_client(config) do
        {:ok, %{client: client, refresh_token: refresh_token, access_token: access_token}} ->
          new_config = Map.put(config, :access_token, access_token)
          :ets.insert(@table, {config.app_token, %{client: client, config: new_config}})
          Process.send_after(__MODULE__, {:do_refresh, new_config}, jittered_refresh_ms())
          GenServer.reply(from, refresh_token)

        {:error, reason} ->
          Logger.warning(
            "Salesforce token refresh failed for #{config.app_token}: #{inspect(reason)}, retrying in #{div(@refresh_retry_ms, 60_000)}m"
          )

          Process.send_after(__MODULE__, {:do_refresh, config}, @refresh_retry_ms)
          GenServer.reply(from, {:error, reason})
      end
    end)

    {:noreply, state}
  end

  #
  # Internals
  #

  # Spawned outside the GenServer mailbox so HTTPS never blocks reads.
  # The Task writes to ETS itself (the table is :public) before scheduling the
  # next periodic refresh, eliminating any race between the caller observing a
  # successful reply and the cache being populated.
  defp spawn_refresh(config) do
    Task.start(fn ->
      case refresh_client(config) do
        {:ok, %{client: client, access_token: access_token}} ->
          new_config = Map.put(config, :access_token, access_token)
          :ets.insert(@table, {config.app_token, %{client: client, config: new_config}})
          Process.send_after(__MODULE__, {:do_refresh, new_config}, jittered_refresh_ms())

        {:error, reason} ->
          Logger.warning(
            "Salesforce token refresh failed for #{config.app_token}: #{inspect(reason)}, retrying in #{div(@refresh_retry_ms, 60_000)}m"
          )

          Process.send_after(__MODULE__, {:do_refresh, config}, @refresh_retry_ms)
      end
    end)
  end

  defp jittered_refresh_ms do
    @refresh_interval_ms + :rand.uniform(@refresh_jitter_ms) - div(@refresh_jitter_ms, 2)
  end

  defp safe_callback(fun) do
    fun.()
  rescue
    e ->
      Logger.error("Salesforce initial_load callback raised: #{inspect(e)}")
      []
  end

  # the initializing of the genserver do authenticate with salesforce and build the client,
  # then it stores the client in ets table.
  # Authentication Response Example:
  # {:ok,
  # %ExForce.OAuthResponse{
  #   access_token: "00DDp0000018Wr2!AQ...",
  #   id: "https://login.salesforce.com/id/00DDp0000018Wr2MAE/005Dp000002NCZXIA4",
  #   instance_url: "https://userpilot-dev-ed.develop.my.salesforce.com",
  #   issued_at: ~U[2023-11-07 13:19:11.832Z],
  #   refresh_token: nil,
  #   scope: "api",
  #   signature: "*/*",
  #   token_type: "Bearer"
  # }}
  defp init_client(
         %{
           app_token: app_token,
           auth_url: auth_url,
           client_id: client_id,
           client_secret: client_secret,
           redirect_uri: redirect_uri,
           code: code,
           code_verifier: code_verifier,
           code_challenge_method: code_challenge_method
         } = _config
       ) do
    with {:ok,
          %{instance_url: instance_url, refresh_token: new_refresh_token, id: id} =
            oauth_response} <-
           ExForce.OAuth.get_token(auth_url,
             grant_type: "authorization_code",
             client_id: client_id,
             client_secret: client_secret,
             redirect_uri: redirect_uri,
             code: code,
             code_verifier: code_verifier,
             code_challenge_method: code_challenge_method
           ) do
      {:ok, version_maps} = ExForce.versions(instance_url)
      latest_version = version_maps |> Enum.map(&Map.fetch!(&1, "version")) |> List.last()

      with client = ExForce.build_client(oauth_response, api_version: latest_version),
           {:ok, body} <- ExForce.info(client, id) do
        {:ok,
         %{
           client: client,
           response: %{
             metadata: Map.put(body, "instance_url", instance_url),
             access_token: oauth_response.access_token,
             refresh_token: new_refresh_token
           }
         }}
      end
    else
      {:error, reason} ->
        Logger.warning(
          "Failed to authenticate for app_token #{app_token} with salesforce: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  defp refresh_client(
         %{
           app_token: app_token,
           auth_url: auth_url,
           client_id: client_id,
           client_secret: client_secret,
           refresh_token: refresh_token
         } = _config
       ) do
    with {:ok, %{instance_url: instance_url, access_token: access_token} = oauth_response} <-
           ExForce.OAuth.get_token(auth_url,
             grant_type: "refresh_token",
             client_id: client_id,
             client_secret: client_secret,
             refresh_token: refresh_token
           ) do
      {:ok, version_maps} = ExForce.versions(instance_url)
      latest_version = version_maps |> Enum.map(&Map.fetch!(&1, "version")) |> List.last()

      client = ExForce.build_client(oauth_response, api_version: latest_version)

      {:ok, %{client: client, refresh_token: refresh_token, access_token: access_token}}
    else
      {:error, reason} ->
        Logger.warning(
          "Failed to refresh for app_token #{app_token} with salesforce: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end
end
