defmodule ExForce.API do
  require Logger

  @standard_objects [
    "Account",
    "Campaign",
    "Case",
    "Contact",
    "Contract",
    "Lead",
    "Opportunity",
    "Product",
    "Pricebook",
    "Quotev",
    "Solution",
    "Task",
    "User"
  ]

  @moduledoc """
  Simple wrapper for EXForce library for userpilot needs.
  """

  defp get_client(app_token) do
    case Salesforce.get_app(app_token) do
      %{client: client} ->
        {:ok, client}

      _ ->
        {:error,
         "Salesforce instance not initialized. Make sure you have setup your Salesforce for #{app_token}"}
    end
  end

  defp get_kb_client(app_token) do
    case SalesforceKB.get_app(app_token) do
      %{client: client} ->
        {:ok, client}

      _ ->
        {:error,
         "SalesforceKB instance not initialized. Make sure you have setup your SalesforceKB for #{app_token}"}
    end
  end

  @spec register_new_app(
          %{
            :app_token => String.t(),
            :auth_url => String.t(),
            :client_id => String.t(),
            :client_secret => any(),
            :redirect_uri => String.t(),
            :code => String.t(),
            :code_verifier => String.t(),
            optional(any()) => any()
          },
          atom()
        ) :: any()
  def register_new_app(config, :salesforce) do
    Salesforce.register_app_token(config)
  end

  def register_new_app(config, :salesforce_kb) do
    SalesforceKB.register_app_token(config)
  end

  @spec refresh_app_client(
          %{
            :app_token => String.t(),
            :auth_url => String.t(),
            :client_id => String.t(),
            :client_secret => any(),
            :refresh_token => String.t()
          },
          atom()
        ) :: any()
  def refresh_app_client(config, :salesforce) do
    Salesforce.refresh_app_token(config)
  end

  def refresh_app_client(config, :salesforce_kb) do
    SalesforceKB.refresh_app_token(config)
  end

  @spec get_available_objects(binary()) :: {:ok, list()} | {:error, any()}
  def get_available_objects(app_token) do
    with {:ok, client} <- get_client(app_token),
         {:ok, %{"sobjects" => objects}} <- ExForce.describe_global(client) do
      objects
      |> Enum.filter(&targeted_object?/1)
      |> Enum.reject(&untargeted_object?/1)
      |> Enum.map(&to_object(&1, :standard_object))
      |> merge_contact_and_lead_objects()
      |> case do
        objects when is_list(objects) ->
          {:ok, objects}

        _ ->
          {:error, "No available objects"}
      end
    else
      {:error, error} ->
        {:error, error}
    end
  end

  @spec get_available_custom_objects(binary()) :: {:ok, list()} | {:error, any()}
  def get_available_custom_objects(app_token) do
    with {:ok, client} <- get_client(app_token) do
      {:ok, %{"sobjects" => objects}} = ExForce.describe_global(client)

      objects
      |> Enum.filter(fn object -> object["custom"] == true end)
      |> Enum.reject(fn object -> String.contains?(object["name"], "Userpilot") end)
      |> Enum.map(&to_object(&1, :custom_object))
      |> then(&{:ok, &1})
    else
      {:error, error} ->
        {:error, error}
    end
  end

  @doc """

  Example:
  ExForce.API.get_object_unique_identifiers("NX-44d03690","Contact")
  """
  @spec get_object_unique_identifiers(
          %{:instance_url => binary(), optional(any()) => any()},
          binary()
        ) :: list()
  def get_object_unique_identifiers(app_token, object) do
    with {:ok, client} <- get_client(app_token) do
      {:ok, %{"fields" => fields}} = ExForce.describe_sobject(client, object)

      fields
      |> Enum.filter(fn field -> field["label"] == "Email" end)
      |> Enum.filter(fn field -> field["unique"] == true end)
      |> Enum.map(fn field ->
        %{title: field["label"], id: field["name"], type: field["type"]}
      end)
    end
  end

  @doc """

  Example:
  ExForce.API.get_object_attributes("NX-44d03690","Contact")
  """
  def get_object_attributes(app_token, object) do
    with {:ok, client} <- get_client(app_token),
         {:ok, %{"fields" => fields}} <- ExForce.describe_sobject(client, object) do
      fields
      |> Enum.map(&to_property/1)
    else
      error -> error
    end
  end

  @doc """

  Example:
  ExForce.API.get_object_attributes_kb("NX-44d03690")
  """
  def get_object_attributes_kb(app_token) do
    with {:ok, client} <- get_kb_client(app_token),
         {:ok, %{"fields" => fields}} <- ExForce.describe_sobject(client, "Knowledge__kav") do
      fields
      |> Enum.map(&to_property/1)
    else
      error -> error
    end
  end

  @doc """

  param_list is the list of parameters we want to retrieve from the object, eg: ["Name","Email"]

  Example:
  ExForce.API.get_objects_paginated("NX-44d03690","Contact",["Name","Email"],10,0)
  """
  @spec get_objects_paginated(
          binary(),
          binary(),
          charlist(),
          number(),
          number()
        ) :: list()
  def get_objects_paginated(app_token, object, param_list, per_page, page) do
    with {:ok, client} <- get_client(app_token) do
      param_list = maybe_append_id(param_list)

      ExForce.query_stream(
        client,
        "SELECT #{encode_param_list(param_list)} FROM #{object} LIMIT #{per_page} OFFSET #{per_page * page}"
      )
      |> Stream.map(fn
        {:error,
         [
           %{
             "errorCode" => code,
             "message" => message
           }
         ]} ->
          # re-auth
          Logger.error(
            "Error while fetching #{object} for #{app_token} from Salesforce: #{code} with message #{message}"
          )

          code

        result ->
          result.data
          |> Map.put("Id", result.id)
      end)
      |> Enum.to_list()
    end
  end

  @doc """

  param_list is the list of parameters we want to retrieve from the object, eg: ["Name","Email"]
  property_name is the property we need to search the values throw it, eg: Name, Id, Email, etc ..
  property_values is the values we need to search by them, eg: if the property_name is Email the values could be ["Foo@bar.co"].

  Example:
  ExForce.API.search_objects_by_property_values("NX-44d03690", "Contact", ["Name", "Email"], Email, ["foo1@bar.com", "foo2@bar.com"])
  """
  @spec search_objects_by_property_values(
          binary(),
          binary(),
          charlist(),
          binary(),
          charlist(),
          any()
        ) :: {:ok, list()} | {:error, binary()}
  def search_objects_by_property_values(
        app_token,
        object,
        param_list,
        property_name,
        property_values,
        last_modified \\ nil
      ) do
    with {:ok, client} <- get_client(app_token) do
      param_list = maybe_append_id(param_list)

      sf_sql =
        "SELECT #{encode_param_list(param_list)} FROM #{object} WHERE #{property_name} IN #{encode_property_values(property_values)}"
        |> maybe_add_last_modified(last_modified)

      case ExForce.query(
             client,
             sf_sql
           ) do
        {:ok, %ExForce.QueryResult{done: true, records: records}} ->
          {:ok, Enum.map(records, fn record -> record.data end)}

        {:error,
         [
           %{
             "errorCode" => code,
             "message" => message
           }
         ]} ->
          Logger.error(
            "Error while fetching #{object} for #{app_token} from Salesforce: #{code} with message #{message}"
          )

          {:error, code}

        {:error, error} ->
          Logger.error(
            "Error while fetching #{object} for #{app_token} from Salesforce: with message #{inspect(error)}"
          )

          {:error, error}
      end
    end
  end

  @doc """
  Example:
  ExForce.API.get_object_by_id("NX-44d03690","003Dp000005sjRJIAY","Contact")
  """
  @spec get_object_by_id(
          binary(),
          binary(),
          binary(),
          list()
        ) :: any()
  def get_object_by_id(app_token, id, sobject_name, fields \\ []) do
    with {:ok, client} <- get_client(app_token) do
      case ExForce.get_sobject(client, id, sobject_name, fields) do
        {:ok, %ExForce.SObject{data: data}} ->
          {:ok, data}

        {:error,
         [
           %{
             "errorCode" => code,
             "message" => _message
           }
         ]} ->
          # re-auth
          {:error, code}
      end
    end
  end

  @doc """
  Example:
  ExForce.API.get_object_by_external_id("NX-44d03690","Customer","Userpilot_Id__c","userpilot456")
  """
  @spec get_object_by_external_id(
          binary(),
          binary(),
          binary(),
          binary(),
          list()
        ) :: any()
  def get_object_by_external_id(
        app_token,
        sobject_name,
        field_name,
        field_value,
        fields
      ) do
    with {:ok, client} <- get_client(app_token) do
      case ExForce.get_sobject_by_external_id(
             client,
             field_value,
             field_name,
             sobject_name,
             fields
           ) do
        {:ok, %ExForce.SObject{data: data}} ->
          {:ok, data}

        {:error,
         [
           %{
             "errorCode" => code,
             "message" => _message
           }
         ]} ->
          # re-auth
          {:error, code}

        {:error, list} ->
          list
          |> List.last()
          |> String.split("/")
          |> List.last()
          |> (&get_object_by_id(app_token, &1, sobject_name)).()
      end
    end
  end

  @doc """
  Example:
  ExForce.API.search_object_by_field("NX-44d03690","Customer","Userpilot_Id__c","userpilot456")
  """
  @spec search_object_by_field(
          binary(),
          binary(),
          binary(),
          binary(),
          list()
        ) :: any()
  def search_object_by_field(
        app_token,
        sobject_name,
        field_name,
        field_value,
        fields
      )
      when fields != [] do
    with {:ok, client} <- get_client(app_token) do
      case ExForce.query(
             client,
             "SELECT #{Enum.join(fields, " ,")} FROM #{sobject_name} WHERE #{field_name} = '#{field_value}' LIMIT 1"
           ) do
        {:ok, %ExForce.QueryResult{done: true, records: list}} when list == [] ->
          {:error, "NOT_FOUND"}

        {:ok, %ExForce.QueryResult{done: true, records: list}} ->
          record = List.first(list)
          {:ok, Map.put(record.data, "Id", record.id)}

        {:error,
         [
           %{
             "errorCode" => code,
             "message" => _message
           }
         ]} ->
          {:error, code}
      end
    end
  end

  def search_object_by_field(
        app_token,
        sobject_name,
        field_name,
        field_value,
        _fields
      ) do
    with {:ok, client} <- get_client(app_token) do
      case ExForce.query(
             client,
             "SELECT FIELDS(STANDARD) FROM #{sobject_name} WHERE #{field_name} = '#{field_value}' LIMIT 1"
           ) do
        {:ok, %ExForce.QueryResult{done: true, records: list}} when list == [] ->
          {:error, "NOT_FOUND"}

        {:ok, %ExForce.QueryResult{done: true, records: list}} ->
          record = List.first(list)
          {:ok, Map.put(record.data, "Id", record.id)}

        {:error,
         [
           %{
             "errorCode" => code,
             "message" => _message
           }
         ]} ->
          {:error, code}
      end
    end
  end

  @spec create_apex_class(binary(), binary(), binary()) :: {:error, any()} | {:ok, binary()}
  def create_apex_class(
        app_token,
        class_name,
        class_body
      ) do
    with {:ok, client} <- get_client(app_token),
         {:ok, %{body: %{"id" => id}}} <-
           ExForce.create_sobject(client, "ApexClass", %{
             Name: class_name,
             Body: class_body
           }) do
      {:ok, id}
    else
      {:error, body} ->
        {:error, body}
    end
  end

  @spec create_apex_trigger(binary(), binary(), binary(), binary()) ::
          {:error, any()} | {:ok, binary()}
  def create_apex_trigger(
        app_token,
        trigger_name,
        trigger_body,
        trigger_object
      ) do
    with {:ok, client} <- get_client(app_token),
         {:ok, %{body: %{"id" => id}}} <-
           ExForce.create_sobject(client, "ApexTrigger", %{
             Name: trigger_name,
             TableEnumOrId: trigger_object,
             Body: trigger_body,
             Status: "Active"
           }) do
      {:ok, id}
    else
      {:error, body} ->
        {:error, body}
    end
  end

  @spec create_custom_event(binary(), binary(), map()) ::
          {:error, any()} | {:ok, binary()}
  def create_custom_event(
        app_token,
        custom_event_schema_name,
        body
      ) do
    with {:ok, client} <- get_client(app_token),
         {:ok, %{body: body, time: time}} <-
           ExForce.create_sobject(client, custom_event_schema_name, body) do
      {:ok, Map.put(body, :time, time)}
    else
      {:error, body} ->
        {:error, body}
    end
  end

  @spec delete_apex_class(any(), any()) :: :ok | {:error, any()}
  def delete_apex_class(
        app_token,
        class_id
      ) do
    with {:ok, client} <- get_client(app_token) do
      ExForce.delete_sobject(client, class_id, "ApexClass")
    end
  end

  @spec delete_apex_trigger(any(), any()) :: :ok | {:error, any()}
  def delete_apex_trigger(
        app_token,
        trigger_id
      ) do
    with {:ok, client} <- get_client(app_token) do
      ExForce.delete_sobject(client, trigger_id, "ApexTrigger")
    end
  end

  @spec create_custom_object_schema(String.t(), any()) ::
          {:error, any()} | {:ok, binary()}
  def create_custom_object_schema(app_token, schema) do
    with {:ok, client} <- get_client(app_token),
         %{config: %{access_token: access_token}} = Salesforce.get_app(app_token) do
      ExForce.create_custom_object_schema(client, access_token, schema)
    end
  end

  @spec get_installed_packages(String.t()) ::
          {:error, any()} | {:ok, binary()}
  def get_installed_packages(app_token) do
    with {:ok, client} <- get_client(app_token) do
      ExForce.tooling_query(
        client,
        URI.encode(
          "SELECT Id, SubscriberPackageId, SubscriberPackage.NamespacePrefix," <>
            "SubscriberPackage.Name, SubscriberPackageVersion.Id," <>
            "SubscriberPackageVersion.Name, SubscriberPackageVersion.MajorVersion," <>
            "SubscriberPackageVersion.MinorVersion," <>
            "SubscriberPackageVersion.PatchVersion," <>
            "SubscriberPackageVersion.BuildNumber, " <>
            "SubscriberPackageVersion.InstallValidationStatus " <>
            "FROM InstalledSubscriberPackage"
        )
      )
    end
  end

  def maybe_append_id(param_list) do
    if Enum.member?(param_list, "Id") do
      Enum.reject(param_list, &is_nil/1)
    else
      ["Id" | Enum.reject(param_list, &is_nil/1)]
    end
  end

  def get_category_groups(app_token, locale) do
    with {:ok, client} <- get_kb_client(app_token) do
      ExForce.get_knowledge_groups(client, locale)
    end
  end

  def query_articles(app_token, fields, locale) do
    with {:ok, client} <- get_kb_client(app_token) do
      case ExForce.query_stream(
             client,
             "SELECT #{Enum.join(fields, " ,")}, (SELECT DataCategoryGroupName, DataCategoryName FROM DataCategorySelections) FROM Knowledge__kav WHERE Language = '#{locale}' AND PublishStatus = 'Online' ORDER BY KnowledgeArticleId ASC"
           ) do
        stream ->
          stream
          |> Stream.map(fn
            {:error,
             [
               %{
                 "errorCode" => code,
                 "message" => message
               }
             ]} ->
              Logger.error(
                "Error while fetching articles for #{app_token} from Salesforce: #{code} with message #{message}"
              )

              {:error, code}

            result ->
              result
          end)
          |> Enum.to_list()
          |> then(&{:ok, &1})
      end
    end
  end

  def get_articles(app_token, params \\ nil, locale) do
    with {:ok, client} <- get_kb_client(app_token) do
      ExForce.get_articles(client, params, locale)
    end
  end

  def get_article_by_id(app_token, article_id, locale) do
    with {:ok, client} <- get_kb_client(app_token) do
      ExForce.get_article_by_id(client, article_id, locale)
    end
  end

  def get_knowledge_settings(app_token) do
    with {:ok, client} <- get_kb_client(app_token) do
      ExForce.get_knowledge_settings(client)
    end
  end

  def create_custom_field(app_token, schema) do
    with {:ok, client} <- get_client(app_token) do
      ExForce.create_custom_field(client, schema)
    end
  end

  def bulk_create_sobjects(app_token, attrs) do
    with {:ok, client} <- get_client(app_token) do
      ExForce.bulk_create_sobjects(client, attrs)
    end
  end

  defp encode_param_list(param_list), do: Enum.join(param_list, " ,")

  defp encode_property_values([] = _property_values), do: "('')"

  defp encode_property_values(property_values)
       when is_list(property_values),
       do: "(" <> Enum.map_join(property_values, ",", &encode_value(&1)) <> ")"

  defp encode_value(value) when is_list(value), do: "'" <> to_string(value) <> "'"
  defp encode_value(value) when is_integer(value), do: "'" <> Integer.to_string(value) <> "'"
  defp encode_value(value), do: "'" <> value <> "'"

  defp maybe_add_last_modified(query, nil), do: query

  defp maybe_add_last_modified(query, last_seen),
    do: query <> " AND LastModifiedDate >= #{last_seen}"

  defp targeted_object?(object),
    do:
      object["name"] in @standard_objects or
        String.ends_with?(object["name"], "__c")

  defp untargeted_object?(object),
    do: String.contains?(object["name"], "Userpilot")

  defp to_object(object, _type) do
    %{
      fully_qualified_name: object["name"],
      singular_name: object["label"],
      plural_name: object["labelPlural"],
      primary_object_id: object["name"],
      is_standard_object: not object["custom"],
      is_custom_object: object["custom"]
    }
  end

  defp to_property(field) do
    %{
      title: field["label"],
      id: field["name"],
      type: field["type"],
      is_custom_property: field["custom"]
    }
  end

  defp merge_contact_and_lead_objects(objects) do
    contact_object =
      objects |> Enum.find(fn object -> object[:fully_qualified_name] == "Contact" end)

    objects
    |> Enum.reject(fn object ->
      object[:fully_qualified_name] == "Lead" or object[:fully_qualified_name] == "Contact"
    end)
    |> then(fn objects ->
      [
        %{
          contact_object
          | fully_qualified_name: "Contact/Lead",
            primary_object_id: "Contact/Lead",
            singular_name: "Contact/Lead",
            plural_name: "Contacts/Leads"
        }
      ] ++ objects
    end)
  end
end
