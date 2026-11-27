defmodule Anthropix.StreamHandlerTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  @tool_input %{
    "object" => "deal",
    "fields" => %{"name" => "Acme renewal", "amount" => 12_000, "tags" => ["q4", "renewal"]}
  }

  @tool_use_message %{
    "id" => "msg_01CRLF",
    "type" => "message",
    "role" => "assistant",
    "model" => "claude-opus-5-5",
    "content" => [
      %{
        "type" => "tool_use",
        "id" => "toolu_01CRLF",
        "name" => "create_custom_record",
        "input" => @tool_input,
        "caller" => %{"type" => "code_execution_20260120", "tool_id" => "srvtoolu_01CRLF"}
      }
    ],
    "container" => %{"id" => "container_01CRLF", "expires_at" => "2026-11-27T19:00:00Z"},
    "stop_reason" => "tool_use",
    "stop_sequence" => nil,
    "usage" => %{"input_tokens" => 0, "output_tokens" => 0}
  }

  @tool_use_events [
    %{"type" => "message_start", "message" => @tool_use_message},
    %{"type" => "message_stop"}
  ]

  @sampled_events [
    %{
      "type" => "message_start",
      "message" => %{
        "id" => "msg_01LF",
        "type" => "message",
        "role" => "assistant",
        "model" => "claude-opus-5-5",
        "content" => [],
        "stop_reason" => nil,
        "stop_sequence" => nil,
        "usage" => %{"input_tokens" => 10, "output_tokens" => 1}
      }
    },
    %{"type" => "ping"},
    %{
      "type" => "content_block_start",
      "index" => 0,
      "content_block" => %{"type" => "text", "text" => ""}
    },
    %{
      "type" => "content_block_delta",
      "index" => 0,
      "delta" => %{"type" => "text_delta", "text" => "Checking"}
    },
    %{
      "type" => "content_block_delta",
      "index" => 0,
      "delta" => %{"type" => "text_delta", "text" => " the weather.\n\nOne moment."}
    },
    %{"type" => "content_block_stop", "index" => 0},
    %{
      "type" => "content_block_start",
      "index" => 1,
      "content_block" => %{
        "type" => "tool_use",
        "id" => "toolu_01LF",
        "name" => "get_weather",
        "input" => %{}
      }
    },
    %{
      "type" => "content_block_delta",
      "index" => 1,
      "delta" => %{"type" => "input_json_delta", "partial_json" => "{\"location\""}
    },
    %{
      "type" => "content_block_delta",
      "index" => 1,
      "delta" => %{"type" => "input_json_delta", "partial_json" => ": \"London\"}"}
    },
    %{"type" => "content_block_stop", "index" => 1},
    %{
      "type" => "message_delta",
      "delta" => %{"stop_reason" => "tool_use", "stop_sequence" => nil},
      "usage" => %{"output_tokens" => 20}
    },
    %{"type" => "message_stop"}
  ]

  @sampled_body %{
    "id" => "msg_01LF",
    "type" => "message",
    "role" => "assistant",
    "model" => "claude-opus-5-5",
    "content" => [
      %{"type" => "text", "text" => "Checking the weather.\n\nOne moment."},
      %{
        "type" => "tool_use",
        "id" => "toolu_01LF",
        "name" => "get_weather",
        "input" => %{"location" => "London"}
      }
    ],
    "stop_reason" => "tool_use",
    "stop_sequence" => nil,
    "usage" => %{"input_tokens" => 10, "output_tokens" => 1}
  }

  describe "CRLF line endings" do
    test "delivers message_start and message_stop and accumulates the body" do
      raw = to_sse(@tool_use_events, "\r\n")

      assert {events, {:ok, body}} = run_stream([raw])
      assert events == @tool_use_events
      assert [%{"type" => "message_start", "message" => message}, _] = events
      assert message["stop_reason"] == "tool_use"
      assert %{"id" => "container_01CRLF"} = message["container"]
      assert [%{"type" => "tool_use", "input" => @tool_input}] = message["content"]
      assert body == @tool_use_message
    end

    test "decodes identically when split into two chunks at every byte offset" do
      raw = to_sse(@tool_use_events, "\r\n")

      for i <- 1..(byte_size(raw) - 1) do
        chunks = [binary_part(raw, 0, i), binary_part(raw, i, byte_size(raw) - i)]
        assert {@tool_use_events, {:ok, @tool_use_message}} = run_stream(chunks), "split at #{i}"
      end
    end

    test "decodes a ~60KB event delivered in 1KB chunks" do
      input = %{"rows" => for(i <- 1..2000, do: %{"id" => i, "value" => "row-#{i}"})}
      message = put_in(@tool_use_message, ["content", Access.at(0), "input"], input)
      events = [%{"type" => "message_start", "message" => message}, %{"type" => "message_stop"}]
      raw = to_sse(events, "\r\n")
      assert byte_size(raw) > 60_000

      assert {^events, {:ok, ^message}} = run_stream(chunk_every(raw, 1024))
    end

    test "forwards and logs an error event" do
      error = %{
        "type" => "error",
        "error" => %{"type" => "overloaded_error", "message" => "Overloaded"}
      }

      events = [hd(@tool_use_events), error]
      raw = to_sse(events, "\r\n")

      log =
        capture_log(fn ->
          assert {^events, {:ok, @tool_use_message}} = run_stream(chunk_every(raw, 7))
        end)

      assert log =~ "overloaded_error - Overloaded"
    end
  end

  describe "CR-only line endings" do
    test "decodes the stream" do
      raw = to_sse(@tool_use_events, "\r")

      for i <- 1..(byte_size(raw) - 1) do
        chunks = [binary_part(raw, 0, i), binary_part(raw, i, byte_size(raw) - i)]
        assert {@tool_use_events, {:ok, @tool_use_message}} = run_stream(chunks), "split at #{i}"
      end
    end
  end

  test "decodes every line ending style delivered one byte per chunk" do
    for eol <- ["\r\n", "\r", "\n"] do
      raw = to_sse(@tool_use_events, eol)

      assert {@tool_use_events, {:ok, @tool_use_message}} = run_stream(chunk_every(raw, 1)),
             "eol #{inspect(eol)}"
    end
  end

  describe "LF line endings" do
    test "decodes a sampled stream delivered whole" do
      raw = to_sse(@sampled_events, "\n")
      assert {@sampled_events, {:ok, @sampled_body}} = run_stream([raw])
    end

    test "decodes a sampled stream split into two chunks at every byte offset" do
      raw = to_sse(@sampled_events, "\n")

      for i <- 1..(byte_size(raw) - 1) do
        chunks = [binary_part(raw, 0, i), binary_part(raw, i, byte_size(raw) - i)]
        assert {@sampled_events, {:ok, @sampled_body}} = run_stream(chunks), "split at #{i}"
      end
    end
  end

  defp to_sse(events, eol) do
    Enum.map_join(events, fn %{"type" => type} = event ->
      "event: #{type}#{eol}data: #{Jason.encode!(event)}#{eol}#{eol}"
    end)
  end

  defp chunk_every(raw, size) when byte_size(raw) <= size, do: [raw]

  defp chunk_every(raw, size) do
    <<chunk::binary-size(size), rest::binary>> = raw
    [chunk | chunk_every(rest, size)]
  end

  # Feeds each chunk to the `into:` callback separately, like a real HTTP
  # adapter. The `plug:` adapter would join them into a single callback.
  defp run_stream(chunks) do
    adapter = fn request ->
      response = Req.Response.new(status: 200, headers: [{"content-type", "text/event-stream"}])

      Enum.reduce(chunks, {request, response}, fn chunk, acc ->
        {:cont, acc} = request.into.({:data, chunk}, acc)
        acc
      end)
    end

    client = struct(Anthropix, req: Req.new(adapter: adapter))

    {:ok, task} =
      Anthropix.chat(client,
        model: "claude-opus-5-5",
        messages: [%{role: "user", content: "hi"}],
        stream: self()
      )

    result = Task.await(task)
    {collect_events(task.pid), result}
  end

  defp collect_events(pid) do
    receive do
      {^pid, {:data, data}} -> [data | collect_events(pid)]
    after
      0 -> []
    end
  end
end
