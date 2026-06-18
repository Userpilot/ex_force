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
  # Retries are spread over [retry, retry + jitter) so a batch that fails
  # together (e.g. a pool-exhaustion burst) does not re-fire in lockstep.
  @refresh_retry_jitter_ms 5 * 60 * 1000
  # Initial refreshes are staggered ~this many ms apart so a (re)boot does not
  # fire every tenant's token refresh at the shared Finch pool simultaneously.
  @initial_load_stagger_ms 250

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
    do: GenServer.call(__MODULE__, {:refresh_token, config}, 60_000)

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
    # Stagger initial refreshes across a window instead of firing them all at
    # once. Each app is scheduled ~@initial_load_stagger_ms apart (plus jitter),
    # so a (re)boot never bursts the shared Finch pool against the Salesforce
    # auth host. Each app then self-schedules its periodic refresh after its
    # first successful refresh via the {:do_refresh, config} path.
    callback_fun
    |> safe_callback()
    |> Enum.with_index()
    |> Enum.each(fn {app, idx} ->
      delay = idx * @initial_load_stagger_ms + :rand.uniform(@initial_load_stagger_ms)
      Process.send_after(__MODULE__, {:do_refresh, app.config}, delay)
    end)

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
          Logger.warning(
            "Salesforce token registration failed for #{app.app_token}: #{inspect(reason)}"
          )

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
          retry_ms = jittered_retry_ms()

          Logger.warning(
            "Salesforce token refresh failed for #{config.app_token}: #{inspect(reason)}, retrying in #{div(retry_ms, 60_000)}m"
          )

          Process.send_after(__MODULE__, {:do_refresh, config}, retry_ms)
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
          retry_ms = jittered_retry_ms()

          Logger.warning(
            "Salesforce token refresh failed for #{config.app_token}: #{inspect(reason)}, retrying in #{div(retry_ms, 60_000)}m"
          )

          Process.send_after(__MODULE__, {:do_refresh, config}, retry_ms)
      end
    end)
  end

  defp jittered_refresh_ms do
    @refresh_interval_ms + :rand.uniform(@refresh_jitter_ms) - div(@refresh_jitter_ms, 2)
  end

  defp jittered_retry_ms do
    @refresh_retry_ms + :rand.uniform(@refresh_retry_jitter_ms)
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
           ),
         {:ok, version_maps} <- ExForce.versions(instance_url) do
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
           timed_step(app_token, "get_token", fn ->
             ExForce.OAuth.get_token(auth_url,
               grant_type: "refresh_token",
               client_id: client_id,
               client_secret: client_secret,
               refresh_token: refresh_token
             )
           end),
         {:ok, version_maps} <-
           timed_step(app_token, "versions", fn -> ExForce.versions(instance_url) end),
         {:ok, client} <-
           timed_step(app_token, "build_client", fn ->
             latest_version = version_maps |> Enum.map(&Map.fetch!(&1, "version")) |> List.last()
             {:ok, ExForce.build_client(oauth_response, api_version: latest_version)}
           end) do
      {:ok, %{client: client, refresh_token: refresh_token, access_token: access_token}}
    else
      {:error, reason} ->
        Logger.warning(
          "Failed to refresh for app_token #{app_token} with salesforce: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  # Times a single refresh step in isolation, logs its duration + outcome, and
  # returns the step result unchanged so the `with` pipeline controls flow.
  # Any raise inside the step is converted to {:error, _} so one failing step
  # neither crashes the refresh Task nor hides the timings of earlier steps.
  defp timed_step(app_token, step, fun) do
    {us, result} =
      :timer.tc(fn ->
        try do
          fun.()
        rescue
          e -> {:error, {:exception, Exception.message(e)}}
        end
      end)

    ok? = match?({:ok, _}, result)
    outcome = if ok?, do: "ok", else: "error"
    level = if ok?, do: :info, else: :warning

    Logger.log(
      level,
      "Salesforce refresh step=#{step} app_token=#{app_token} duration=#{div(us, 1000)}ms outcome=#{outcome}"
    )

    result
  end
end
