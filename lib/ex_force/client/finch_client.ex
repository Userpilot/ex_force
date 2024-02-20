defmodule ExForce.Client.FinchClient do
  def request(type, url, body, headers, opts \\ []) do
    opts = Keyword.merge([receive_timeout: 5_000], opts)
    body = Jason.encode!(body)

    case :timer.tc(&do_send_request/5, [type, url, body, headers, opts]) do
      {time, {:ok, response}} ->
        {:ok, Map.put(response, :time, time)}

      {time, {:error, error}} ->
        {:error, Map.put(error, :time, time)}
    end
  end

  def post(url, body, headers, opts \\ []), do: request(:post, url, body, headers, opts)

  defp do_send_request(type, url, body, headers, opts),
    do:
      type
      |> Finch.build(url, headers, body)
      |> Finch.request(__MODULE__, opts)
      |> decode_response()

  defp decode_response({:ok, %Finch.Response{status: _status, body: _body} = response}),
    do: decode_response(response)

  defp decode_response({:error, %Finch.Error{__exception__: exception, reason: reason}}),
    do:
      {:error,
       %{
         status: nil,
         body: "#{inspect(reason)}: #{Exception.format(:error, exception)}",
         headers: nil
       }}

  defp decode_response({:error, %Mint.TransportError{reason: reason} = error}),
    do:
      {:error,
       %{status: nil, body: "#{inspect(reason)}: #{Exception.message(error)}", headers: nil}}

  defp decode_response(%Finch.Response{status: status, body: body, headers: headers} = _response)
       when status >= 200 and status < 300,
       do: {:ok, %{status: status, body: body, headers: headers}}

  defp decode_response(%Finch.Response{status: status, body: body, headers: headers} = _response),
    do: {:error, %{status: status, body: body, headers: headers}}
end
