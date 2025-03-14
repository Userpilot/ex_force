defmodule ExForce do
  @moduledoc """
  Simple wrapper for Salesforce REST API.

  ## Installation

  The package can be installed by adding `ex_force` to your list of dependencies in `mix.exs`:

  ```elixir
  def deps do
    [
      {:ex_force, "~> 0.3"}
    ]
  end
  ```

  Check out [Choosing a Tesla Adapter](https://github.com/chulkilee/ex_force/wiki/Choosing-a-Tesla-Adapter).

  ## Usage

  ```elixir
  {:ok, %{instance_url: instance_url} = oauth_response} =
    ExForce.OAuth.get_token(
      "https://login.salesforce.com",
      grant_type: "password",
      client_id: "client_id",
      client_secret: "client_secret",
      username: "username",
      password: "password" <> "security_token"
    )

  {:ok, version_maps} = ExForce.versions(instance_url)
  latest_version = version_maps |> Enum.map(&Map.fetch!(&1, "version")) |> List.last()

  client = ExForce.build_client(oauth_response, api_version: latest_version)

  names =
    ExForce.query_stream(client, "SELECT Name FROM Account")
    |> Stream.map(&Map.fetch!(&1.data, "Name"))
    |> Stream.take(50)
    |> Enum.to_list()
  ```

  Note that streams emit `ExForce.SObject` or an error tuple.
  """

  alias ExForce.{
    Client,
    QueryResult,
    Request,
    Response,
    SObject
  }

  @type client :: Client.t()
  @type sobject_id :: String.t()
  @type sobject_name :: String.t()
  @type field_name :: String.t()
  @type soql :: String.t()
  @type query_id :: String.t()
  @type sobject :: %{id: String.t(), attributes: %{type: String.t()}}
  @type fields :: list()

  defdelegate build_client(instance_url), to: Client
  defdelegate build_client(instance_url, opts), to: Client

  @doc """
  Lists available REST API versions at an instance.

  See [Versions](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_versions.htm)
  """
  @spec versions(String.t()) :: {:ok, list(map)} | {:error, any}
  def versions(instance_url) do
    case instance_url
         |> Client.build_client()
         |> Client.request(%Request{method: :get, url: "/services/data"}) do
      {:ok, %Response{status: 200, body: body}} when is_list(body) -> {:ok, body}
      {:ok, %Response{body: body}} -> {:error, body}
      {:error, _} = other -> other
    end
  end

  @doc """
  Lists basic account info

  """
  @spec info(String.t(), String.t()) :: {:ok, map} | {:error, any}
  def info(client, "https://login.salesforce.com" <> id_path) do
    case Client.request(client, %Request{method: :get, url: id_path}) do
      {:ok, %Response{status: 200, body: body}} -> {:ok, body}
      {:ok, %Response{body: body}} -> {:error, body}
      {:error, _} = other -> other
    end
  end

  def info(client, "https://test.salesforce.com" <> id_path) do
    case Client.request(client, %Request{method: :get, url: id_path}) do
      {:ok, %Response{status: 200, body: body}} -> {:ok, body}
      {:ok, %Response{body: body}} -> {:error, body}
      {:error, _} = other -> other
    end
  end

  @doc """
  Lists available resources for the specific API version.

  See [Resources by Version](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_discoveryresource.htm)
  """
  @spec resources(client, String.t()) :: {:ok, map} | {:error, any}
  def resources(client, version) do
    case Client.request(client, %Request{method: :get, url: "/services/data/v#{version}"}) do
      {:ok, %Response{status: 200, body: body}} -> {:ok, body}
      {:ok, %Response{body: body}} -> {:error, body}
      {:error, _} = other -> other
    end
  end

  @doc """
  Lists the available objects.

  See [Describe Global](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_describeGlobal.htm)
  """
  @spec describe_global(client) :: {:ok, map} | {:error, any}
  def describe_global(client) do
    case Client.request(client, %Request{method: :get, url: "sobjects"}) do
      {:ok, %Response{status: 200, body: body}} -> {:ok, body}
      {:ok, %Response{body: body}} -> {:error, body}
      {:error, _} = other -> other
    end
  end

  @doc """
  Retrieves extended metadata for the specified SObject.

  See [SObject Describe](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_sobject_describe.htm)
  """
  @spec describe_sobject(client, sobject_name) :: {:ok, map} | {:error, any}
  def describe_sobject(client, name) do
    case Client.request(client, %Request{method: :get, url: "sobjects/#{name}/describe"}) do
      {:ok, %Response{status: 200, body: body}} -> {:ok, body}
      {:ok, %Response{body: body}} -> {:error, body}
      {:error, _} = other -> other
    end
  end

  @doc """
  Retrieves basic metadata for the specific SObject.

  See [SObject Basic Information](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_sobject_basic_info.htm)
  """
  @spec basic_info(client, sobject_name) :: {:ok, map} | {:error, any}
  def basic_info(client, name) do
    case Client.request(client, %Request{method: :get, url: "sobjects/#{name}"}) do
      {:ok, %Response{status: 200, body: %{"recentItems" => recent_items} = body}} ->
        {:ok, Map.put(body, "recentItems", Enum.map(recent_items, &SObject.build/1))}

      {:ok, %Response{body: body}} ->
        {:error, body}

      {:error, _} = other ->
        other
    end
  end

  @doc """
  Retrieves a SObject by ID.

  See [SObject Rows](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_sobject_retrieve.htm)
  """
  @spec get_sobject(client, sobject_id, sobject_name, list) :: {:ok, SObject.t()} | {:error, any}
  def get_sobject(client, id, name, fields),
    do: do_get_sobject(client, "sobjects/#{name}/#{id}", fields)

  @doc """
  Retrieves a SObject based on the value of a specified extneral ID field.

  See [SObject Rows by External ID](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_sobject_upsert.htm)
  """
  @spec get_sobject_by_external_id(client, any, field_name, sobject_name, fields) ::
          {:ok, SObject.t()} | {:error, any}
  def get_sobject_by_external_id(client, field_value, field_name, sobject_name, fields),
    do:
      do_get_sobject(
        client,
        "sobjects/#{sobject_name}/#{field_name}/#{URI.encode(field_value)}",
        fields
      )

  @doc """
  Retrieves a SObject by relationship field.

  See [SObject Relationships](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_sobject_relationships.htm)
  """
  @spec get_sobject_by_relationship(
          client,
          sobject_id,
          sobject_name,
          field_name,
          list(field_name)
        ) :: {:ok, SObject.t() | QueryResult.t()} | {:error, any}
  def get_sobject_by_relationship(client, id, sobject_name, field_name, fields) do
    path = "sobjects/#{sobject_name}/#{id}/#{field_name}"

    case Client.request(client, %Request{
           method: :get,
           url: path,
           query: build_fields_query(fields)
         }) do
      {:ok, %Response{status: 200, body: %{"attributes" => _} = body}} ->
        {:ok, SObject.build(body)}

      {:ok, %Response{status: 200, body: %{"records" => _} = body}} ->
        {:ok, build_result_set(body)}

      {:ok, %Response{body: body}} ->
        {:error, body}

      {:error, _} = other ->
        other
    end
  end

  defp do_get_sobject(client, path, fields) do
    case Client.request(client, %Request{
           method: :get,
           url: path,
           query: build_fields_query(fields)
         }) do
      {:ok, %Response{status: 200, body: body}} -> {:ok, SObject.build(body)}
      {:ok, %Response{body: body}} -> {:error, body}
      {:error, _} = other -> other
    end
  end

  defp build_fields_query([]), do: []
  defp build_fields_query(fields), do: [fields: Enum.join(fields, ",")]

  @doc """
  Updates a SObject.

  See [SObject Rows](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_sobject_retrieve.htm)
  """
  @spec update_sobject(client, sobject_id, sobject_name, map) :: :ok | {:error, any}
  def update_sobject(client, id, name, attrs) do
    case Client.request(client, %Request{
           method: :patch,
           url: "sobjects/#{name}/#{id}",
           body: attrs
         }) do
      {:ok, %Response{status: 204, body: ""}} -> :ok
      {:ok, %Response{body: body}} -> {:error, body}
      {:error, _} = other -> other
    end
  end

  @doc """
  Updates multiple SObjects using the Composite API.

  It uses the Composite API to update multiple records (up to 200) in one call, returning a list of SaveResult objects.
  You can choose whether to roll back the entire request when an error occurs.
  If more than 200 records need to be updated at once, try using the Bulk API.

  See [Update Multiple Records with Fewer Round-Trips](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_composite_sobjects_collections_update.htm)
  """
  @spec update_sobjects(client, records :: list(sobject), all_or_none :: boolean) ::
          {:ok, any} | {:error, any}
  def update_sobjects(client, records, all_or_none \\ false) do
    body = %{records: records, allOrNone: all_or_none}

    case Client.request(client, %Request{method: :patch, url: "composite/sobjects", body: body}) do
      {:ok, %Response{status: 200, body: body}} -> {:ok, body}
      {:ok, %Response{body: body}} -> {:error, body}
      {:error, _} = other -> other
    end
  end

  @spec create_custom_object_schema(client, String.t(), schema: String.t()) ::
          {:ok, custom_object_id: String.t()} | {:error, any}
  def create_custom_object_schema(client, access_token, schema) do
    case Client.request(client, %Request{
           method: :post,
           url: "/services/Soap/m/",
           body: String.replace(schema, "{{session_id}}", access_token),
           headers: [{"content-type", "text/xml"}, {"SOAPAction", "create"}]
         }) do
      {:ok, %Response{status: 201, body: %{"id" => id, "success" => true}}} -> {:ok, id}
      {:ok, %Response{status: 200, body: body}} -> {:ok, body}
      {:ok, %Response{body: body}} -> {:error, body}
      {:error, _} = other -> other
    end
  end

  @doc """
  Creates a SObject.

  See [SObject Rows](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_sobject_basic_info.htm)
  """
  @spec create_sobject(client, sobject_name, map) :: {:ok, any()} | {:error, any}
  def create_sobject(client, name, attrs) do
    case Client.request(client, %Request{method: :post, url: "sobjects/#{name}/", body: attrs}) do
      {:ok, %Response{status: 201, body: body, time: time}} ->
        {:ok, %{body: body, time: time}}

      {:ok, %Response{body: body, time: time}} ->
        {:error, %{body: body, time: time}}

      {:error, _} = other ->
        other
    end
  end

  @doc """
  Deletes a SObject.

  [SObject Rows](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_sobject_retrieve.htm)
  """
  @spec delete_sobject(client, sobject_id, sobject_name) :: :ok | {:error, any}
  def delete_sobject(client, id, name) do
    case Client.request(client, %Request{method: :delete, url: "sobjects/#{name}/#{id}"}) do
      {:ok, %Response{status: 204, body: ""}} -> :ok
      {:ok, %Response{status: 404, body: body}} -> {:error, body}
      {:error, _} = other -> other
    end
  end

  @doc """
  Executes the SOQL query and get the result of it.

  [Query](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_query.htm)
  """
  @spec query(client, soql) :: {:ok, QueryResult.t()} | {:error, any}
  def query(client, soql) do
    case Client.request(client, %Request{method: :get, url: "query", query: [q: soql]}) do
      {:ok, %Response{status: 200, body: body}} -> {:ok, build_result_set(body)}
      {:ok, %Response{body: body}} -> {:error, body}
      {:error, _} = other -> other
    end
  end

  @spec query_stream(client, soql) :: Enumerable.t()
  def query_stream(client, soql), do: start_query_stream(client, &query/2, soql)

  @doc """
  Retrieves additional query results for the specified query ID.

  [Query](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_query.htm)
  """
  @spec query_retrieve(client, query_id | String.t()) :: {:ok, QueryResult.t()} | {:error, any}
  def query_retrieve(client, query_id_or_url) do
    path =
      if full_path?(query_id_or_url) do
        query_id_or_url
      else
        "query/#{query_id_or_url}"
      end

    case Client.request(client, %Request{method: :get, url: path}) do
      {:ok, %Response{status: 200, body: body}} -> {:ok, build_result_set(body)}
      {:ok, %Response{body: body}} -> {:error, body}
      {:error, _} = other -> other
    end
  end

  @doc """

  Run Tooling API query

  """
  @spec tooling_query(client, String.t()) :: {:ok, map} | {:error, any}
  def tooling_query(client, query) do
    case Client.request(client, %Request{method: :get, url: "tooling/query?q=#{query}"}) do
      {:ok, %Response{status: 200, body: %{"records" => records} = _body}} ->
        {:ok, records}

      {:ok, %Response{body: body}} ->
        {:error, body}

      {:error, _} = other ->
        other
    end
  end

  @doc """
  Executes the SOQL query and get the result of it, including deleted or archived objects.

  [QueryAll](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_queryall.htm)
  """
  @spec query_all(client, soql) :: {:ok, QueryResult.t()} | {:error, any}
  def query_all(client, soql) do
    case Client.request(client, %Request{method: :get, url: "queryAll", query: [q: soql]}) do
      {:ok, %Response{status: 200, body: body}} -> {:ok, build_result_set(body)}
      {:ok, %Response{body: body}} -> {:error, body}
      {:error, _} = other -> other
    end
  end

  @spec query_all_stream(client, soql) :: Enumerable.t()
  def query_all_stream(client, soql), do: start_query_stream(client, &query_all/2, soql)

  defp build_result_set(%{"records" => records, "totalSize" => total_size} = resp) do
    case resp do
      %{"done" => true} ->
        %QueryResult{
          done: true,
          total_size: total_size,
          records: records |> Enum.map(&SObject.build/1)
        }

      %{"done" => false, "nextRecordsUrl" => next_records_url} ->
        %QueryResult{
          done: false,
          next_records_url: next_records_url,
          total_size: total_size,
          records: records |> Enum.map(&SObject.build/1)
        }
    end
  end

  @spec start_query_stream(
          client,
          (client, soql -> {:ok, QueryResult.t()} | {:error, any}),
          soql
        ) :: Enumerable.t()

  defp start_query_stream(client, func, soql) do
    Stream.resource(
      fn -> {client, func.(client, soql)} end,
      &stream_next/1,
      fn _acc -> nil end
    )
  end

  @doc """
  Returns `Enumerable.t` from the `QueryResult`.
  """
  @spec stream_query_result(client, QueryResult.t()) :: Enumerable.t()
  def stream_query_result(client, %QueryResult{} = qr) do
    Stream.resource(
      fn -> {client, {:ok, qr}} end,
      &stream_next/1,
      fn _acc -> nil end
    )
  end

  def get_articles(client, params, locale) do
    case Client.request(client, %Request{
           method: :get,
           url:
             "support/knowledgeArticles?pageSize=100" <>
               maybe_search_query(params) <> maybe_page_number(params),
           headers: [{"Accept-Language", locale}]
         }) do
      {:ok, %Response{status: 200, body: body}} -> {:ok, body}
      {:ok, %Response{body: body}} -> {:error, body}
      {:error, _} = other -> other
    end
  end

  def get_article_by_id(client, id, locale) do
    case Client.request(client, %Request{
           method: :get,
           url: "support/knowledgeArticles/#{id}",
           headers: [{"Accept-Language", locale}]
         }) do
      {:ok, %Response{status: 200, body: body}} -> {:ok, body}
      {:ok, %Response{body: body}} -> {:error, body}
      {:error, _} = other -> other
    end
  end

  def get_knowledge_settings(client) do
    case Client.request(client, %Request{
           method: :get,
           url: "knowledgeManagement/settings"
         }) do
      {:ok, %Response{status: 200, body: body}} -> {:ok, body}
      {:ok, %Response{body: body}} -> {:error, body}
      {:error, _} = other -> other
    end
  end

  defp stream_next({client, :halt}), do: {:halt, client}

  defp stream_next({client, {:error, _} = error_tuple}), do: {[error_tuple], {client, :halt}}

  defp stream_next({client, {:ok, %QueryResult{records: records, done: true}}}),
    do: {records, {client, :halt}}

  defp stream_next(
         {client, {:ok, %QueryResult{records: records, done: false, next_records_url: url}}}
       ),
       do: {records, {client, {:retrieve, url}}}

  defp stream_next({client, {:retrieve, next_records_url}}),
    do: {[], {client, query_retrieve(client, next_records_url)}}

  defp full_path?(path), do: String.starts_with?(path, "/services/data/v")

  defp maybe_search_query(%{"search" => search}), do: "&q=#{URI.encode(search)}"
  defp maybe_search_query(_params), do: ""

  defp maybe_page_number(%{"page_number" => page_number}), do: "&pageNumber=#{page_number}"
  defp maybe_page_number(_params), do: ""

  @doc """
  Creates a custom field for a Salesforce object.
  """
  @spec create_custom_field(client, map()) :: {:ok, any} | {:error, any}
  def create_custom_field(client, schema) do
    case Client.request(client, %Request{
           method: :post,
           url: "tooling/sobjects/CustomField",
           body: schema
         }) do
      {:ok, %Response{status: 204}} -> {:ok, %{success: true}}
      {:ok, %Response{status: 200, body: body}} -> {:ok, body}
      {:ok, %Response{status: 201, body: body}} -> {:ok, body}
      {:ok, %Response{body: body}} -> {:error, body}
      {:error, _} = error -> error
    end
  end

  @doc """
  Bulk create objects using the Salesforce Composite API.

  attrs: [
    %{
    "Field" => "UserpilotInteraction__c.Platform__c",
    "ParentId" => "0PSak00000Dq2axGAB",
    "PermissionsEdit" => true,
    "PermissionsRead" => true,
    "SobjectType" => "UserpilotInteraction__c",
    "attributes" => %{"type" => "FieldPermissions"}
    }
  ]
  """
  def bulk_create_sobjects(client, attrs) when is_list(attrs) do
    case Client.request(client, %Request{
           method: :post,
           url: "composite/sobjects",
           body: %{
             "records" => attrs,
             "allOrNone" => false
           }
         }) do
      {:ok, %Response{status: 200, body: body}} ->
        errors =
          Enum.filter(body, fn record ->
            not record["success"] and record["errors"] != nil
          end)
          |> Enum.map(fn record ->
            %{
              message: get_in(record, ["errors", Access.at(0), "message"])
            }
          end)

        case errors do
          [] -> {:ok, body}
          errors -> {:error, %{errors: errors}}
        end

      {:ok, %Response{status: 201, body: body}} ->
        {:ok, body}

      {:ok, %Response{body: body}} ->
        {:error, body}

      {:error, _} = error ->
        error
    end
  end
end
