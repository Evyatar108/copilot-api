#!/usr/bin/env node

import { defineCommand } from "citty"
import clipboard from "clipboardy"
import consola from "consola"
import invariant from "tiny-invariant"

import { mergeConfigWithDefaults } from "./lib/config"
import { ensurePaths } from "./lib/paths"
import { initProxyFromEnv } from "./lib/proxy"
import { getConfiguredApiKeys } from "./lib/request-auth"
import { generateEnvScript } from "./lib/shell"
import { state } from "./lib/state"
import { setupCopilotToken, setupGitHubToken } from "./lib/token"
import {
  cacheMacMachineId,
  cacheModels,
  cacheVSCodeVersion,
  cacheVsCodeSessionId,
} from "./lib/utils"
import {
  onWebSocketClose,
  onWebSocketMessage,
  onWebSocketOpen,
  type WebSocketProxyData,
} from "./routes/responses/websocket-proxy"

const isWebSocketUpgradeRequest = (req: Request): boolean => {
  const url = new URL(req.url)
  const path = url.pathname
  if (path !== "/v1/responses" && path !== "/responses") return false
  return req.headers.get("upgrade")?.toLowerCase() === "websocket"
}

const checkWebSocketAuth = (req: Request): boolean => {
  const apiKeys = getConfiguredApiKeys()
  if (apiKeys.length === 0) return true

  // Check x-api-key header
  const xApiKey = req.headers.get("x-api-key")?.trim()
  if (xApiKey && apiKeys.includes(xApiKey)) return true

  // Check Authorization: Bearer <token>
  const authorization = req.headers.get("authorization")
  if (authorization) {
    const [scheme, ...rest] = authorization.trim().split(/\s+/)
    if (scheme.toLowerCase() === "bearer") {
      const bearerToken = rest.join(" ").trim()
      if (bearerToken && apiKeys.includes(bearerToken)) return true
    }
  }

  return false
}

interface RunServerOptions {
  port: number
  verbose: boolean
  accountType: string
  manual: boolean
  rateLimit?: number
  rateLimitWait: boolean
  githubToken?: string
  claudeCode: boolean
  showToken: boolean
  proxyEnv: boolean
}

export async function runServer(options: RunServerOptions): Promise<void> {
  // Ensure config is merged with defaults at startup
  mergeConfigWithDefaults()

  if (options.proxyEnv) {
    initProxyFromEnv()
  }

  state.verbose = options.verbose
  if (options.verbose) {
    consola.level = 5
    consola.info("Verbose logging enabled")
  }

  state.accountType = options.accountType
  if (options.accountType !== "individual") {
    consola.info(`Using ${options.accountType} plan GitHub account`)
  }

  state.manualApprove = options.manual
  state.rateLimitSeconds = options.rateLimit
  state.rateLimitWait = options.rateLimitWait
  state.showToken = options.showToken

  await ensurePaths()
  await cacheVSCodeVersion()
  cacheMacMachineId()
  cacheVsCodeSessionId()

  if (options.githubToken) {
    state.githubToken = options.githubToken
    consola.info("Using provided GitHub token")
  } else {
    await setupGitHubToken()
  }

  await setupCopilotToken()
  await cacheModels()

  consola.info(
    `Available models: \n${state.models?.data.map((model) => `- ${model.id}`).join("\n")}`,
  )

  const serverUrl = `http://localhost:${options.port}`

  if (options.claudeCode) {
    consola.log(
      "\n💡 Tip: The --claude-code flag simply generates a clipboard command for launching Claude Code. \n"
        + "All models remain fully accessible without this flag, just configure the model ID directly in your settings.json file.",
    )

    invariant(state.models, "Models should be loaded by now")

    const selectedModel = await consola.prompt(
      "Select a model to use with Claude Code",
      {
        type: "select",
        options: state.models.data.map((model) => model.id),
      },
    )

    const selectedSmallModel = await consola.prompt(
      "Select a small model to use with Claude Code",
      {
        type: "select",
        options: state.models.data.map((model) => model.id),
      },
    )

    const command = generateEnvScript(
      {
        ANTHROPIC_BASE_URL: serverUrl,
        ANTHROPIC_AUTH_TOKEN: "dummy",
        ANTHROPIC_MODEL: selectedModel,
        ANTHROPIC_DEFAULT_SONNET_MODEL: selectedModel,
        ANTHROPIC_DEFAULT_HAIKU_MODEL: selectedSmallModel,
        DISABLE_NON_ESSENTIAL_MODEL_CALLS: "1",
        CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: "1",
        CLAUDE_CODE_ATTRIBUTION_HEADER: "0",
        CLAUDE_CODE_ENABLE_PROMPT_SUGGESTION: "false",
      },
      "claude",
    )

    try {
      clipboard.writeSync(command)
      consola.success("Copied Claude Code command to clipboard!")
    } catch {
      consola.warn(
        "Failed to copy to clipboard. Here is the Claude Code command:",
      )
      consola.log(command)
    }
  }

  consola.box(
    `🌐 Usage Viewer: ${serverUrl}/usage-viewer?endpoint=${serverUrl}/usage`,
  )

  await startHttpServer(options.port)
}

async function startHttpServer(port: number): Promise<void> {
  const { server } = await import("./server")

  // E2E WebSocket: accept WS from Codex, open upstream WS to GitHub Copilot,
  // forward messages bidirectionally. Upstream WS bypasses the HTTP proxy
  // (Bun's WebSocket client doesn't support HTTP CONNECT tunneling).
  Bun.serve<WebSocketProxyData, Record<string, never>>({
    port,
    idleTimeout: 0,
    fetch(req, bunServer) {
      if (isWebSocketUpgradeRequest(req)) {
        if (!checkWebSocketAuth(req)) {
          return new Response(
            JSON.stringify({
              error: {
                message: "Unauthorized",
                type: "authentication_error",
              },
            }),
            { status: 401, headers: { "Content-Type": "application/json" } },
          )
        }

        const upgraded = bunServer.upgrade<WebSocketProxyData>(req, {
          data: {
            upstream: null,
            upstreamReady: false,
            pendingMessages: [],
            closing: false,
          },
        })
        if (upgraded) return undefined
        return new Response("WebSocket upgrade failed", { status: 500 })
      }

      return server.fetch(req)
    },
    websocket: {
      perMessageDeflate: true,
      open(ws) {
        onWebSocketOpen(ws)
      },
      message(ws, message) {
        onWebSocketMessage(ws, message)
      },
      close(ws, code, reason) {
        onWebSocketClose(ws, code, reason)
      },
    },
  })
}

export const start = defineCommand({
  meta: {
    name: "start",
    description: "Start the Copilot API server",
  },
  args: {
    port: {
      alias: "p",
      type: "string",
      default: "4141",
      description: "Port to listen on",
    },
    verbose: {
      alias: "v",
      type: "boolean",
      default: false,
      description: "Enable verbose logging",
    },
    "account-type": {
      alias: "a",
      type: "string",
      default: "individual",
      description: "Account type to use (individual, business, enterprise)",
    },
    manual: {
      type: "boolean",
      default: false,
      description: "Enable manual request approval",
    },
    "rate-limit": {
      alias: "r",
      type: "string",
      description: "Rate limit in seconds between requests",
    },
    wait: {
      alias: "w",
      type: "boolean",
      default: false,
      description:
        "Wait instead of error when rate limit is hit. Has no effect if rate limit is not set",
    },
    "github-token": {
      alias: "g",
      type: "string",
      description:
        "Provide GitHub token directly (must be generated using the `auth` subcommand)",
    },
    "claude-code": {
      alias: "c",
      type: "boolean",
      default: false,
      description:
        "Generate a command to launch Claude Code with Copilot API config",
    },
    "show-token": {
      type: "boolean",
      default: false,
      description: "Show GitHub and Copilot tokens on fetch and refresh",
    },
    "proxy-env": {
      type: "boolean",
      default: false,
      description: "Initialize proxy from environment variables",
    },
  },
  run({ args }) {
    const rateLimitRaw = args["rate-limit"]
    const rateLimit =
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
      rateLimitRaw === undefined ? undefined : Number.parseInt(rateLimitRaw, 10)

    return runServer({
      port: Number.parseInt(args.port, 10),
      verbose: args.verbose,
      accountType: args["account-type"],
      manual: args.manual,
      rateLimit,
      rateLimitWait: args.wait,
      githubToken: args["github-token"],
      claudeCode: args["claude-code"],
      showToken: args["show-token"],
      proxyEnv: args["proxy-env"],
    })
  },
})
