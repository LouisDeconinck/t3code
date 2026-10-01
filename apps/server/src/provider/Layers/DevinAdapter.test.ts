// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeFSP from "node:fs/promises";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import {
  ApprovalRequestId,
  DevinSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";

import { ServerConfig } from "../../config.ts";
import { execScriptSource, writeFakeCli } from "../../testUtils/fakeCli.ts";
import { devinModeId, makeDevinAdapter, selectDevinPermissionOptionId } from "./DevinAdapter.ts";

const decodeDevinSettings = Schema.decodeSync(DevinSettings);
const __dirname = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const mockAgentPath = NodePath.join(__dirname, "../../../scripts/acp-mock-agent.ts");

async function makeMockDevin(extraEnv: Record<string, string>) {
  const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "devin-acp-mock-"));
  return writeFakeCli({
    directory,
    name: "fake-devin",
    env: extraEnv,
    source: execScriptSource({ scriptPath: mockAgentPath, expectedArgs: ["acp"] }),
  });
}

async function readJsonLines(filePath: string) {
  const raw = await NodeFSP.readFile(filePath, "utf8");
  return raw
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

const devinAdapterTestLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-devin-adapter-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

it("maps runtime modes onto Devin session modes", () => {
  assert.equal(devinModeId("full-access"), "bypass");
  assert.equal(devinModeId("auto"), "smart");
  assert.equal(devinModeId("auto-accept-edits"), "accept-edits");
  assert.equal(devinModeId("approval-required"), "accept-edits");
});

it("scopes approvals to the session without persisting them or switching modes", () => {
  // Option lists captured from `devin acp` 3000.11.3 in accept-edits mode.
  const singleCommand = [
    { optionId: "allow_once", name: "Allow", kind: "allow_once" },
    {
      optionId: "allow_session",
      name: "Yes, allow `uname` commands (this session)",
      kind: "allow_always",
    },
    {
      optionId: "allow_always",
      name: "Yes, always allow `uname` commands in `repo`",
      kind: "allow_always",
    },
    {
      optionId: "allow_always_global",
      name: "Yes, always allow `uname` commands in all projects",
      kind: "allow_always",
    },
    { optionId: "switch_bypass", name: "Yes, switch to bypass mode", kind: "allow_always" },
    { optionId: "reject_once", name: "Reject", kind: "reject_once" },
  ] as const;
  const compoundCommand = [
    { optionId: "allow_once", name: "Allow", kind: "allow_once" },
    {
      optionId: "switch_accept_edits",
      name: "Yes, switch to accept edits mode",
      kind: "allow_always",
    },
    { optionId: "switch_bypass", name: "Yes, switch to bypass mode", kind: "allow_always" },
    { optionId: "reject_once", name: "Reject", kind: "reject_once" },
  ] as const;

  assert.equal(selectDevinPermissionOptionId(singleCommand, "acceptForSession"), "allow_session");
  assert.equal(selectDevinPermissionOptionId(singleCommand, "acceptAlways"), "allow_session");
  assert.equal(selectDevinPermissionOptionId(singleCommand, "accept"), "allow_once");
  assert.equal(selectDevinPermissionOptionId(compoundCommand, "acceptForSession"), "allow_once");
  assert.equal(selectDevinPermissionOptionId(compoundCommand, "decline"), "reject_once");
});

it.layer(devinAdapterTestLayer)("DevinAdapter", (it) => {
  it.effect("runs a turn on the selected model without calling ACP authenticate", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("devin-mock-thread");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "devin-acp-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const binaryPath = yield* Effect.promise(() =>
        makeMockDevin({ T3_ACP_REQUEST_LOG_PATH: requestLogPath }),
      );
      const adapter = yield* makeDevinAdapter(decodeDevinSettings({ binaryPath })).pipe(
        Effect.orDie,
      );

      const events: ProviderRuntimeEvent[] = [];
      const turnCompleted = yield* Deferred.make<void>();
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => events.push(event)).pipe(
          Effect.andThen(
            event.type === "turn.completed"
              ? Deferred.succeed(turnCompleted, undefined)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      const session = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("devin"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId: ProviderInstanceId.make("devin"), model: "composer-2" },
      });
      assert.equal(session.model, "composer-2");
      assert.deepStrictEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "mock-session-1",
      });

      yield* adapter.sendTurn({ threadId, input: "hello devin", attachments: [] });
      yield* Deferred.await(turnCompleted);
      yield* Fiber.interrupt(eventsFiber);

      const delta = events.find((event) => event.type === "content.delta");
      assert.equal(
        delta?.type === "content.delta" ? delta.payload.delta : undefined,
        "hello from mock",
      );

      const requests = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const methods = requests.flatMap((entry) =>
        typeof entry.method === "string" ? [entry.method] : [],
      );
      assert.notInclude(methods, "authenticate");
      assert.isTrue(
        requests.some(
          (entry) =>
            entry.method === "session/set_config_option" &&
            (entry.params as { configId?: unknown; value?: unknown } | undefined)?.configId ===
              "model" &&
            (entry.params as { value?: unknown }).value === "composer-2",
        ),
      );

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("answers approvals with the option id Devin offered", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("devin-approval-thread");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "devin-acp-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const binaryPath = yield* Effect.promise(() =>
        makeMockDevin({
          T3_ACP_REQUEST_LOG_PATH: requestLogPath,
          T3_ACP_EMIT_TOOL_CALLS: "1",
          T3_ACP_ALLOW_ONCE_OPTION_ID: "devin-allow-once",
        }),
      );
      const adapter = yield* makeDevinAdapter(decodeDevinSettings({ binaryPath })).pipe(
        Effect.orDie,
      );
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        event.type === "request.opened"
          ? adapter.respondToRequest(
              threadId,
              ApprovalRequestId.make(String(event.requestId)),
              "accept",
            )
          : Effect.void,
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("devin"),
        cwd: process.cwd(),
        runtimeMode: "approval-required",
      });
      yield* adapter.sendTurn({ threadId, input: "approve this", attachments: [] });

      const requests = yield* Effect.promise(() => readJsonLines(requestLogPath));
      assert.isTrue(
        requests.some(
          (entry) =>
            !("method" in entry) &&
            (entry.result as { outcome?: { optionId?: unknown } } | undefined)?.outcome
              ?.optionId === "devin-allow-once",
        ),
      );

      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );
});
