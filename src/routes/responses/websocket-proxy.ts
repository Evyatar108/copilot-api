import type { ServerWebSocket } from "bun"

import { randomUUID } from "node:crypto"

import { copilotBaseUrl, copilotHeaders } from "~/lib/api-config"
import { createHandlerLogger } from "~/lib/logger"
import { state } from "~/lib/state"

const logger = createHandlerLogger("ws-proxy")

/**
 * Per-connection state stored as WebSocket data.
 * Holds a reference to the upstream WebSocket so we can clean up on close.
 */
export interface WebSocketProxyData {
  upstream: WebSocket | null
  upstreamReady: boolean
  /** Messages queued while the upstream connection is still opening. */
  pendingMessages: Array<string>
  /** True once either side has initiated a close. */
  closing: boolean
}

/**
 * Build the headers needed for the upstream WebSocket handshake.
 * Mirrors what `createResponses` sends for HTTP requests, plus the
 * `OpenAI-Beta` header that signals the WebSocket protocol version.
 */
function buildUpstreamHeaders(): Record<string, string> {
  const requestId = randomUUID()
  const headers: Record<string, string> = {
    ...copilotHeaders(state, requestId),
    "OpenAI-Beta": "responses_websockets=2026-02-06",
  }
  return headers
}

/**
 * Called when a local WebSocket connection is opened (after upgrade).
 * Establishes the upstream WebSocket to the Copilot API and wires up
 * bidirectional message forwarding.
 */
export function onWebSocketOpen(ws: ServerWebSocket<WebSocketProxyData>): void {
  logger.debug("Local WebSocket connection opened, connecting upstream...")

  if (!state.copilotToken) {
    logger.error("No Copilot token available, closing local WebSocket")
    ws.send(
      JSON.stringify({
        type: "error",
        status: 401,
        error: { message: "Copilot token not available" },
      }),
    )
    ws.close(1008, "Copilot token not available")
    return
  }

  const upstreamUrl = `${copilotBaseUrl(state)}/responses`
    .replace(/^http:/, "ws:")
    .replace(/^https:/, "wss:")

  const headers = buildUpstreamHeaders()

  logger.info(`Connecting to upstream WebSocket: ${upstreamUrl}`)
  logger.info(`Copilot token present: ${Boolean(state.copilotToken)}`)
  logger.info(`Headers: ${JSON.stringify(Object.keys(headers))}`)

  let upstream: WebSocket
  try {
    // Direct WebSocket to GitHub Copilot (bypasses HTTP proxy).
    // HTTP calls still go through mitmproxy via HTTP_PROXY env var.
    upstream = new WebSocket(upstreamUrl, { headers } as any)
    logger.info("WebSocket constructor succeeded, waiting for open...")
  } catch (err) {
    const message =
      err instanceof Error ? err.message : "Failed to create upstream WebSocket"
    logger.error("Failed to create upstream WebSocket:", message)
    ws.send(
      JSON.stringify({
        type: "error",
        status: 502,
        error: { message: `Upstream connection failed: ${message}` },
      }),
    )
    ws.close(1011, "Upstream connection failed")
    return
  }

  ws.data.upstream = upstream

  upstream.addEventListener("open", () => {
    logger.debug("Upstream WebSocket connected")
    ws.data.upstreamReady = true

    // Flush any messages that arrived while we were connecting
    for (const msg of ws.data.pendingMessages) {
      try {
        upstream.send(msg)
      } catch (err) {
        logger.error("Failed to send queued message to upstream:", err)
      }
    }
    ws.data.pendingMessages = []
  })

  upstream.addEventListener("message", (event) => {
    // Forward upstream messages to the local client
    try {
      const data =
        typeof event.data === "string" ? event.data : String(event.data)
      ws.send(data)
    } catch (err) {
      logger.error("Failed to forward upstream message to local client:", err)
    }
  })

  upstream.addEventListener("error", (event) => {
    const errDetail = (event as any)?.message || (event as any)?.error?.message || JSON.stringify(event)
    logger.error("Upstream WebSocket error:", errDetail)
    if (!ws.data.closing) {
      ws.send(
        JSON.stringify({
          type: "error",
          status: 502,
          error: { message: `Upstream WebSocket error: ${errDetail}` },
        }),
      )
    }
  })

  upstream.addEventListener("close", (event) => {
    logger.debug(
      `Upstream WebSocket closed: code=${event.code} reason=${event.reason}`,
    )
    ws.data.upstream = null
    ws.data.upstreamReady = false

    if (!ws.data.closing) {
      ws.data.closing = true
      try {
        ws.close(event.code, event.reason || "Upstream closed")
      } catch {
        // Local socket may already be closed
      }
    }
  })
}

/**
 * Sanitize a message before forwarding to upstream.
 * Strips fields that GitHub Copilot doesn't support and handles
 * empty probe requests.
 */
function sanitizeMessage(text: string): string | null {
  try {
    const parsed = JSON.parse(text)
    if (typeof parsed !== "object" || parsed === null) return text

    // Strip previous_response_id — Copilot doesn't support it
    delete parsed.previous_response_id

    // Probe requests: empty or missing input with no conversation context
    if (parsed.type === "response.create") {
      const hasInput = Array.isArray(parsed.input) ? parsed.input.length > 0 : Boolean(parsed.input)
      if (!hasInput) {
        // Return null to signal "send synthetic response, don't forward"
        return null
      }
    }

    return JSON.stringify(parsed)
  } catch {
    return text
  }
}

/**
 * Called when the local client sends a message.
 * Forwards it to the upstream WebSocket after sanitization.
 */
export function onWebSocketMessage(
  ws: ServerWebSocket<WebSocketProxyData>,
  message: string | Buffer,
): void {
  const raw = typeof message === "string" ? message : message.toString("utf8")
  const text = sanitizeMessage(raw)

  // Probe request — return synthetic empty response
  if (text === null) {
    logger.debug("Probe request (no input), returning synthetic response")
    ws.send(JSON.stringify({
      type: "response.completed",
      response: {
        id: `resp_probe_${Date.now()}`,
        object: "response",
        status: "completed",
        output: [],
        output_text: "",
        model: "unknown",
        usage: { input_tokens: 0, output_tokens: 0, total_tokens: 0 },
      },
    }))
    return
  }

  if (!ws.data.upstream) {
    ws.send(
      JSON.stringify({
        type: "error",
        status: 502,
        error: { message: "No upstream connection available" },
      }),
    )
    return
  }

  if (!ws.data.upstreamReady) {
    // Queue the message until the upstream connection is ready
    ws.data.pendingMessages.push(text)
    return
  }

  try {
    ws.data.upstream.send(text)
  } catch (err) {
    const message =
      err instanceof Error ? err.message : "Failed to send to upstream"
    logger.error("Failed to forward message to upstream:", message)
    ws.send(
      JSON.stringify({
        type: "error",
        status: 502,
        error: { message: `Failed to send to upstream: ${message}` },
      }),
    )
  }
}

/**
 * Called when the local WebSocket closes.
 * Cleans up the upstream connection.
 */
export function onWebSocketClose(
  ws: ServerWebSocket<WebSocketProxyData>,
  code: number,
  reason: string,
): void {
  logger.debug(`Local WebSocket closed: code=${code} reason=${reason}`)
  ws.data.closing = true

  if (ws.data.upstream) {
    try {
      ws.data.upstream.close(code, reason)
    } catch {
      // Upstream may already be closed
    }
    ws.data.upstream = null
  }

  ws.data.upstreamReady = false
  ws.data.pendingMessages = []
}
