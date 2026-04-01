import type { ServerWebSocket } from "bun"

import { awaitApproval } from "~/lib/approval"
import { getConfig, isResponsesApiWebSearchEnabled } from "~/lib/config"
import { HTTPError } from "~/lib/error"
import { createHandlerLogger } from "~/lib/logger"
import { checkRateLimit } from "~/lib/rate-limit"
import { state } from "~/lib/state"
import { generateRequestIdFromPayload, getUUID } from "~/lib/utils"
import {
  createResponses,
  type ResponsesPayload,
} from "~/services/copilot/create-responses"

import { createStreamIdTracker, fixStreamIds } from "./stream-id-sync"
import {
  applyResponsesApiContextManagement,
  compactInputByLatestCompaction,
  getResponsesRequestOptions,
} from "./utils"

const logger = createHandlerLogger("ws-responses-handler")

const RESPONSES_ENDPOINT = "/responses"

/**
 * Per-connection state stored as WebSocket data.
 * `busy` prevents concurrent message processing on the same socket.
 */
export interface WebSocketData {
  busy: boolean
}

/**
 * Handle an incoming WebSocket text message on a /responses connection.
 *
 * Protocol:
 *  - Client sends `{"type": "response.create", ...payload}`
 *  - Server streams back individual JSON text messages (one per SSE event)
 *  - After `response.completed`, connection stays open for next request
 */
export async function handleWebSocketMessage(
  ws: ServerWebSocket<WebSocketData>,
  message: string | Buffer,
): Promise<void> {
  const text = typeof message === "string" ? message : message.toString("utf8")

  // Concurrent message protection
  if (ws.data.busy) {
    ws.send(
      JSON.stringify({
        type: "error",
        status: 409,
        error: {
          message:
            "A response is already being streamed. Please wait for it to complete before sending another request.",
        },
      }),
    )
    return
  }

  ws.data.busy = true

  try {
    await processMessage(ws, text)
  } catch (error) {
    sendError(ws, error)
  } finally {
    // eslint-disable-next-line require-atomic-updates
    ws.data.busy = false
  }
}

async function processMessage(
  ws: ServerWebSocket<WebSocketData>,
  text: string,
): Promise<void> {
  let parsed: Record<string, unknown>
  try {
    parsed = JSON.parse(text) as Record<string, unknown>
  } catch {
    ws.send(
      JSON.stringify({
        type: "error",
        status: 400,
        error: { message: "Invalid JSON" },
      }),
    )
    return
  }

  if (parsed.type !== "response.create") {
    ws.send(
      JSON.stringify({
        type: "error",
        status: 400,
        error: {
          message: `Unsupported message type: ${String(parsed.type)}. Expected "response.create".`,
        },
      }),
    )
    return
  }

  // Rate-limit check (throws HTTPError on rejection)
  await checkRateLimit(state)

  // Build the payload from the message fields (everything except `type`)
  const payload: ResponsesPayload = buildPayload(parsed)

  // Warmup/probe requests may omit `input`. The upstream API requires it,
  // so return an empty completed response instead of forwarding.
  const hasInput = Array.isArray(payload.input) ? payload.input.length > 0 : Boolean(payload.input)
  const hasPrevResponse = Boolean((payload as Record<string, unknown>).previous_response_id)
  if (!hasInput && !hasPrevResponse) {
    logger.debug("Probe request (no/empty input), returning empty response")
    ws.send(JSON.stringify({
      type: "response.completed",
      response: {
        id: `resp_probe_${Date.now()}`,
        object: "response",
        status: "completed",
        output: [],
        output_text: "",
        model: payload.model || "unknown",
        usage: { input_tokens: 0, output_tokens: 0, total_tokens: 0 },
      },
    }))
    return
  }

  // GitHub Copilot doesn't support previous_response_id — strip it
  delete (payload as Record<string, unknown>).previous_response_id

  logger.debug("WS Responses request payload:", JSON.stringify(payload))

  const requestId = generateRequestIdFromPayload({ messages: payload.input })
  logger.debug("Generated request ID:", requestId)

  const sessionId = getUUID(requestId)
  logger.debug("Extracted session ID:", sessionId)

  // Apply the same transforms as the HTTP handler
  useFunctionApplyPatch(payload)

  if (!isResponsesApiWebSearchEnabled()) {
    removeWebSearchTool(payload)
  }

  compactInputByLatestCompaction(payload)

  // Model validation
  const selectedModel = state.models?.data.find(
    (model) => model.id === payload.model,
  )
  const supportsResponses =
    selectedModel?.supported_endpoints?.includes(RESPONSES_ENDPOINT) ?? false

  if (!supportsResponses) {
    ws.send(
      JSON.stringify({
        type: "error",
        status: 400,
        error: {
          message:
            "This model does not support the responses endpoint. Please choose a different model.",
        },
      }),
    )
    return
  }

  applyResponsesApiContextManagement(
    payload,
    selectedModel?.capabilities.limits.max_prompt_tokens,
  )

  // Force streaming for WebSocket
  payload.stream = true

  logger.debug("Translated WS Responses payload:", JSON.stringify(payload))

  const { vision, initiator } = getResponsesRequestOptions(payload)

  if (state.manualApprove) {
    await awaitApproval()
  }

  const response = await createResponses(payload, {
    vision,
    initiator,
    requestId,
    sessionId,
  })

  // Stream response back as individual JSON text messages
  if (isAsyncIterable(response)) {
    const idTracker = createStreamIdTracker()

    for await (const chunk of response) {
      const rawData = (chunk as { data?: string }).data ?? ""
      const eventType = (chunk as { event?: string }).event

      const processedData = fixStreamIds(rawData, eventType, idTracker)

      // Send the processed JSON data as a WebSocket text message
      ws.send(processedData)
    }
  } else {
    // Non-streaming response (shouldn't happen since we force stream: true,
    // but handle gracefully)
    ws.send(JSON.stringify(response))
  }
}

function buildPayload(parsed: Record<string, unknown>): ResponsesPayload {
  // Extract all fields except `type` as the ResponsesPayload
  const { type: _type, ...rest } = parsed
  return rest as ResponsesPayload
}

function sendError(ws: ServerWebSocket<WebSocketData>, error: unknown): void {
  if (error instanceof HTTPError) {
    const status = error.response.status
    ws.send(
      JSON.stringify({
        type: "error",
        status,
        error: { message: error.message },
      }),
    )
    return
  }

  const message =
    error instanceof Error ? error.message : "Internal server error"
  ws.send(
    JSON.stringify({
      type: "error",
      status: 500,
      error: { message },
    }),
  )
}

// --- Helpers copied from handler.ts to avoid coupling ---

const isAsyncIterable = <T>(value: unknown): value is AsyncIterable<T> =>
  Boolean(value)
  && typeof (value as AsyncIterable<T>)[Symbol.asyncIterator] === "function"

const useFunctionApplyPatch = (payload: ResponsesPayload): void => {
  const config = getConfig()
  const usePatch = config.useFunctionApplyPatch ?? true
  if (usePatch) {
    logger.debug("Using function tool apply_patch for responses")
    if (Array.isArray(payload.tools)) {
      const toolsArr = payload.tools
      for (let i = 0; i < toolsArr.length; i++) {
        const t = toolsArr[i]
        if (t.type === "custom" && t.name === "apply_patch") {
          toolsArr[i] = {
            type: "function",
            name: t.name,
            description: "Use the `apply_patch` tool to edit files",
            parameters: {
              type: "object",
              properties: {
                input: {
                  type: "string",
                  description: "The entire contents of the apply_patch command",
                },
              },
              required: ["input"],
            },
            strict: false,
          }
        }
      }
    }
  }
}

const removeWebSearchTool = (payload: ResponsesPayload): void => {
  if (!Array.isArray(payload.tools) || payload.tools.length === 0) return

  payload.tools = payload.tools.filter((t) => {
    return t.type !== "web_search"
  })
}
