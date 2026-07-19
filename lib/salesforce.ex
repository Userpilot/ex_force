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
  # Jitter keeps a batch that failed together (e.g. a pool-exhaustion burst)
  # from re-firing in lockstep.
  @refresh_retry_jitter_ms 5 * 60 * 1000
  # Staggers boot-time refreshes so a (re)boot doesn't burst the Finch pool.
  @initial_load_stagger_ms 250
  # Rate/quota limits reset in minutes-hours, so throttled errors must never
  # use the fast retry.
  @throttled_retry_ms 30 * 60 * 1000
  @throttled_retry_jitter_ms 30 * 60 * 1000

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
    # Deliberately not refreshed in-line: each app is scheduled a stagger apart
    # so a (re)boot never fires every tenant's refresh at the Finch pool at once.
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
          handle_refresh_error(config, reason)
          GenServer.reply(from, {:error, reason})
      end
    end)

    {:noreply, state}
  end

  #
  # Internals
  #

  # Runs in a Task so HTTPS never blocks the GenServer mailbox; the Task writes
  # to ETS itself to avoid a race between the reply and the cache being populated.
  defp spawn_refresh(config) do
    Task.start(fn ->
      case refresh_client(config) do
        {:ok, %{client: client, access_token: access_token}} ->
          new_config = Map.put(config, :access_token, access_token)
          :ets.insert(@table, {config.app_token, %{client: client, config: new_config}})
          Process.send_after(__MODULE__, {:do_refresh, new_config}, jittered_refresh_ms())

        {:error, reason} ->
          handle_refresh_error(config, reason)
      end
    end)
  end

  # Permanent errors stop the refresh chain on purpose: retrying can never fix
  # revoked/invalid auth, it only hammers the token endpoint. The app stays in
  # ETS with its last client so callers still get a clear Salesforce error.
  defp handle_refresh_error(config, reason) do
    case ExForce.ErrorClass.classify(reason) do
      :permanent ->
        Logger.error(
          "Salesforce token refresh permanently failed for #{config.app_token}: #{inspect(reason)} — " <>
            "stopping retries; org must re-authenticate via the integrations page"
        )

      :throttled ->
        retry_ms = @throttled_retry_ms + :rand.uniform(@throttled_retry_jitter_ms)

        Logger.warning(
          "Salesforce token refresh throttled for #{config.app_token}: #{inspect(reason)}, retrying in #{div(retry_ms, 60_000)}m"
        )

        Process.send_after(__MODULE__, {:do_refresh, config}, retry_ms)

      _transient_or_session ->
        retry_ms = jittered_retry_ms()

        Logger.warning(
          "Salesforce token refresh failed for #{config.app_token}: #{inspect(reason)}, retrying in #{div(retry_ms, 60_000)}m"
        )

        Process.send_after(__MODULE__, {:do_refresh, config}, retry_ms)
    end
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
          %{instance_url: instance_url, refresh_token: new_refresh_token, id: id} = oauth_response} <-
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
           timed_step(app_token, "versions", fn ->
             ExForce.versions(instance_url)
           end),
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

  # Raises are converted to {:error, _} so one failing step neither crashes the
  # refresh Task nor hides the timings of the steps before it.
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
