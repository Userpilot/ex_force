defmodule SalesforceKB do
  @moduledoc """
  This genserver for salesforce that holds integrations config and clients for per app token
  """

  require Logger

  defmodule State do
    defstruct [
      :applications
    ]

    @type t :: %__MODULE__{
            applications: map()
          }
  end

  use GenServer

  @refresh_token_interval_ms 2 * 60 * 60 * 1000
  # Jitter keeps failed batches from re-firing in lockstep; throttled
  # (rate/quota) errors wait much longer because limits reset in minutes-hours.
  @refresh_retry_ms 5 * 60 * 1000
  @refresh_retry_jitter_ms 5 * 60 * 1000
  @throttled_retry_ms 30 * 60 * 1000
  @throttled_retry_jitter_ms 30 * 60 * 1000

  #
  # External API
  #

  def register_app_token(app) do
    GenServer.call(__MODULE__, {:register_app, app})
  end

  def refresh_app_token(config) do
    GenServer.call(__MODULE__, {:refresh_token, config})
  end

  def get_app(app_token) do
    GenServer.call(__MODULE__, {:get_app, app_token})
  end

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
    applications =
      callback_fun.()
      |> Enum.reduce(%{}, fn app, applications ->
        with {:ok, %{client: client, access_token: access_token}} <- refresh_client(app.config) do
          Map.put(applications, String.to_atom(app.app_token), %{
            config: Map.put(app.config, :access_token, access_token),
            client: client
          })
        else
          error ->
            Logger.warning(
              "Failed to authenticate for app_token #{app.app_token} with salesforce: #{inspect(error)}"
            )

            applications
        end
      end)

    {:ok, %State{applications: applications}}
  end

  @impl true
  def handle_info({:refresh_token, app_token}, %State{applications: applications} = state) do
    app = Map.get(applications, String.to_atom(app_token))

    case refresh_client(app.config) do
      {:ok, %{client: client, access_token: access_token}} ->
        applications =
          Map.put(applications, String.to_atom(app.config.app_token), %{
            config: Map.put(app.config, :access_token, access_token),
            client: client
          })

        {:noreply, %State{state | applications: applications}}

      {:error, reason} ->
        # Failures must reschedule, otherwise the refresh chain dies and the
        # client stays stale until a node restart. Only permanent errors stop
        # the chain (on purpose — retrying can't fix revoked auth).
        case ExForce.ErrorClass.classify(reason) do
          :permanent ->
            Logger.error(
              "SalesforceKB token refresh permanently failed for #{app_token}: #{inspect(reason)} — " <>
                "stopping retries; org must re-authenticate via the integrations page"
            )

          :throttled ->
            retry_ms = @throttled_retry_ms + :rand.uniform(@throttled_retry_jitter_ms)

            Logger.warning(
              "SalesforceKB token refresh throttled for #{app_token}: #{inspect(reason)}, retrying in #{div(retry_ms, 60_000)}m"
            )

            Process.send_after(self(), {:refresh_token, app_token}, retry_ms)

          _transient_or_session ->
            retry_ms = @refresh_retry_ms + :rand.uniform(@refresh_retry_jitter_ms)

            Logger.warning(
              "SalesforceKB token refresh failed for #{app_token}: #{inspect(reason)}, retrying in #{div(retry_ms, 60_000)}m"
            )

            Process.send_after(self(), {:refresh_token, app_token}, retry_ms)
        end

        {:noreply, state}
    end
  end

  @impl true
  def handle_call({:register_app, app}, _from, %State{applications: applications} = state) do
    case init_client(app) do
      {:ok,
       %{
         client: client,
         response: %{refresh_token: refresh_token, access_token: access_token} = response
       }} ->
        applications =
          Map.put(applications, String.to_atom(app.app_token), %{
            config: Map.merge(app, %{refresh_token: refresh_token, access_token: access_token}),
            client: client
          })

        {:reply, response, %State{state | applications: applications}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:refresh_token, config}, _from, %State{applications: applications} = state) do
    case refresh_client(config) do
      {:ok, %{client: client, refresh_token: refresh_token, access_token: access_token}} ->
        applications =
          Map.put(applications, String.to_atom(config.app_token), %{
            config: Map.put(config, :access_token, access_token),
            client: client
          })

        {:reply, refresh_token, %State{state | applications: applications}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:get_app, app_token}, _from, %State{applications: applications} = state) do
    app = Map.get(applications, String.to_atom(app_token), nil)

    {:reply, app, state}
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
         {:ok, version_maps} <- ExForce.versions(instance_url),
         latest_version = version_maps |> Enum.map(&Map.fetch!(&1, "version")) |> List.last(),
         client = ExForce.build_client(oauth_response, api_version: latest_version),
         {:ok, body} <- ExForce.info(client, id) do
      Process.send_after(self(), {:refresh_token, app_token}, @refresh_token_interval_ms)

      {:ok,
       %{
         client: client,
         response: %{
           metadata: Map.put(body, "instance_url", instance_url),
           access_token: oauth_response.access_token,
           refresh_token: new_refresh_token
         }
       }}
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
           ),
         {:ok, version_maps} <- ExForce.versions(instance_url),
         latest_version = version_maps |> Enum.map(&Map.fetch!(&1, "version")) |> List.last(),
         client = ExForce.build_client(oauth_response, api_version: latest_version) do
      Process.send_after(self(), {:refresh_token, app_token}, @refresh_token_interval_ms)
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
