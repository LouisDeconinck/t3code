/**
 * DevinAdapter — Devin CLI (`devin acp`) via ACP.
 *
 * @module DevinAdapter
 */
import {
  ApprovalRequestId,
  type ChatAttachment,
  type DevinSettings,
  EventId,
  type ProviderApprovalDecision,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  RuntimeRequestId,
  type RuntimeMode,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import type * as EffectAcpSchema from "effect-acp/schema";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import { acpPermissionOutcome, mapAcpToAdapterError } from "../acp/AcpAdapterSupport.ts";
import {
  makeAcpAssistantItemEvent,
  makeAcpContentDeltaEvent,
  makeAcpPlanUpdatedEvent,
  makeAcpRequestOpenedEvent,
  makeAcpRequestResolvedEvent,
  makeAcpToolCallEvent,
} from "../acp/AcpCoreRuntimeEvents.ts";
import { parsePermissionRequest } from "../acp/AcpRuntimeModel.ts";
import * as AcpSessionRuntime from "../acp/AcpSessionRuntime.ts";
import { buildRuntimeInstructions } from "../RuntimeInstructions.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import type { ProviderAdapterError } from "../Errors.ts";

const PROVIDER = ProviderDriverKind.make("devin");
const DEVIN_RESUME_VERSION = 1 as const;

export interface DevinAdapterOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly instanceId?: ProviderInstanceId;
}

interface DevinSessionContext {
  readonly threadId: ThreadId;
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  readonly acp: AcpSessionRuntime.AcpSessionRuntime["Service"];
  readonly pendingApprovals: Map<ApprovalRequestId, Deferred.Deferred<ProviderApprovalDecision>>;
  readonly turns: Array<{ id: TurnId; items: Array<unknown> }>;
  activeTurnId: TurnId | undefined;
  /** >0 while a prompt runs; a sendTurn then steers the active turn. */
  promptsInFlight: number;
  stopped: boolean;
}

/**
 * Devin's ACP session modes. `devin acp` does not offer the CLI's Normal mode
 * (`ask` is read-only), so supervised threads use `accept-edits`, which still
 * asks before commands.
 */
export function devinModeId(runtimeMode: RuntimeMode): string {
  switch (runtimeMode) {
    case "full-access":
      return "bypass";
    case "auto":
      return "smart";
    default:
      return "accept-edits";
  }
}

/**
 * Devin labels every wider approval `allow_always`: this session, persisted per
 * project or globally, and session mode switches up to `bypass`. Only the
 * `allow_session` id matches a session-scoped approval; anything else is one-time.
 */
export function selectDevinPermissionOptionId(
  options: ReadonlyArray<EffectAcpSchema.PermissionOption>,
  decision: Exclude<ProviderApprovalDecision, "cancel">,
): string {
  if (decision === "decline") {
    return (
      options.find((option) => option.kind === "reject_once")?.optionId ??
      acpPermissionOutcome("decline")
    );
  }
  const sessionOption =
    decision === "accept"
      ? undefined
      : options.find((option) => option.optionId === "allow_session");
  return (
    sessionOption?.optionId ??
    options.find((option) => option.kind === "allow_once")?.optionId ??
    acpPermissionOutcome("accept")
  );
}

function parseDevinResume(raw: unknown): string | undefined {
  if (typeof raw !== "object" || raw === null) return undefined;
  const cursor = raw as { schemaVersion?: unknown; sessionId?: unknown };
  return cursor.schemaVersion === DEVIN_RESUME_VERSION && typeof cursor.sessionId === "string"
    ? cursor.sessionId.trim() || undefined
    : undefined;
}

export function makeDevinAdapter(settings: DevinSettings, options?: DevinAdapterOptions) {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("devin");
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const serverConfig = yield* ServerConfig;
    const crypto = yield* Crypto.Crypto;

    const sessions = new Map<ThreadId, DevinSessionContext>();
    const threadLocks = new Map<ThreadId, Semaphore.Semaphore>();
    const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const randomId = crypto.randomUUIDv4.pipe(Effect.orDie);
    const makeEventStamp = () =>
      Effect.all({ eventId: Effect.map(randomId, EventId.make), createdAt: nowIso });
    const offerRuntimeEvent = (event: ProviderRuntimeEvent) =>
      PubSub.publish(runtimeEventPubSub, event).pipe(Effect.asVoid);

    const withThreadLock = <A, E, R>(threadId: ThreadId, effect: Effect.Effect<A, E, R>) => {
      let semaphore = threadLocks.get(threadId);
      if (!semaphore) {
        semaphore = Semaphore.makeUnsafe(1);
        threadLocks.set(threadId, semaphore);
      }
      return semaphore.withPermit(effect);
    };

    const requireSession = (threadId: ThreadId) => {
      const ctx = sessions.get(threadId);
      return !ctx || ctx.stopped
        ? Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }))
        : Effect.succeed(ctx);
    };

    const applySessionConfiguration = (
      threadId: ThreadId,
      acp: AcpSessionRuntime.AcpSessionRuntime["Service"],
      model: string | undefined,
      runtimeMode: RuntimeMode,
    ) =>
      Effect.gen(function* () {
        if (model) {
          yield* acp
            .setModel(model)
            .pipe(
              Effect.mapError((cause) =>
                mapAcpToAdapterError(PROVIDER, threadId, "session/set_config_option", cause),
              ),
            );
        }
        const modeId = devinModeId(runtimeMode);
        const modeState = yield* acp.getModeState;
        if (modeState?.availableModes.some((mode) => mode.id === modeId)) {
          yield* acp
            .setMode(modeId)
            .pipe(
              Effect.mapError((cause) =>
                mapAcpToAdapterError(PROVIDER, threadId, "session/set_mode", cause),
              ),
            );
        }
      });

    const stopSessionInternal = (ctx: DevinSessionContext) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        ctx.stopped = true;
        for (const decision of ctx.pendingApprovals.values()) {
          yield* Deferred.succeed(decision, "cancel");
        }
        yield* Effect.ignore(Scope.close(ctx.scope, Exit.void));
        sessions.delete(ctx.threadId);
        yield* offerRuntimeEvent({
          type: "session.exited",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: { exitKind: "graceful" },
        });
      });

    const startSession: ProviderAdapterShape<ProviderAdapterError>["startSession"] = (input) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          if (input.provider !== undefined && input.provider !== PROVIDER) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
            });
          }
          if (!input.cwd?.trim()) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "cwd is required and must be non-empty.",
            });
          }
          const cwd = path.resolve(input.cwd.trim());
          const model =
            input.modelSelection?.instanceId === boundInstanceId
              ? input.modelSelection.model
              : undefined;
          const existing = sessions.get(input.threadId);
          if (existing) {
            yield* stopSessionInternal(existing);
          }

          const sessionScope = yield* Scope.make("sequential");
          let sessionScopeTransferred = false;
          yield* Effect.addFinalizer(() =>
            sessionScopeTransferred ? Effect.void : Scope.close(sessionScope, Exit.void),
          );
          const pendingApprovals: DevinSessionContext["pendingApprovals"] = new Map();
          let ctx: DevinSessionContext | undefined;

          const mcpSession = McpProviderSession.readMcpProviderSession(input.threadId);
          const resumeSessionId = parseDevinResume(input.resumeCursor);
          const acpContext = yield* Layer.build(
            AcpSessionRuntime.layer({
              spawn: {
                command: settings.binaryPath,
                args: ["acp"],
                cwd,
                env: McpProviderSession.withAgentDeviceEnvironment(
                  options?.environment ?? process.env,
                  mcpSession,
                ),
              },
              cwd,
              ...(resumeSessionId ? { resumeSessionId } : {}),
              clientInfo: { name: "t3-code", version: "0.0.0" },
              // No authMethodId: Devin reuses the `devin auth login` credentials,
              // while its ACP `authenticate` always starts a browser login.
              ...(mcpSession
                ? {
                    mcpServers: [
                      {
                        type: "http" as const,
                        name: "t3-code",
                        url: mcpSession.endpoint,
                        headers: [{ name: "Authorization", value: mcpSession.authorizationHeader }],
                      },
                    ],
                  }
                : {}),
            }).pipe(
              Layer.provide(
                Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner),
              ),
            ),
          ).pipe(
            Effect.provideService(Crypto.Crypto, crypto),
            Effect.provideService(Scope.Scope, sessionScope),
            Effect.mapError(
              (cause) =>
                new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: cause.message,
                  cause,
                }),
            ),
          );
          const acp = yield* Effect.service(AcpSessionRuntime.AcpSessionRuntime).pipe(
            Effect.provide(acpContext),
          );

          yield* acp.handleRequestPermission((params) =>
            Effect.gen(function* () {
              const permissionRequest = parsePermissionRequest(params);
              const requestId = ApprovalRequestId.make(yield* randomId);
              const runtimeRequestId = RuntimeRequestId.make(requestId);
              const decision = yield* Deferred.make<ProviderApprovalDecision>();
              pendingApprovals.set(requestId, decision);
              yield* offerRuntimeEvent(
                makeAcpRequestOpenedEvent({
                  stamp: yield* makeEventStamp(),
                  provider: PROVIDER,
                  threadId: input.threadId,
                  turnId: ctx?.activeTurnId,
                  requestId: runtimeRequestId,
                  permissionRequest,
                  detail: permissionRequest.detail ?? "Devin requested permission.",
                  args: params,
                  source: "acp.jsonrpc",
                  method: "session/request_permission",
                  rawPayload: params,
                }),
              );
              const resolved = yield* Deferred.await(decision);
              pendingApprovals.delete(requestId);
              yield* offerRuntimeEvent(
                makeAcpRequestResolvedEvent({
                  stamp: yield* makeEventStamp(),
                  provider: PROVIDER,
                  threadId: input.threadId,
                  turnId: ctx?.activeTurnId,
                  requestId: runtimeRequestId,
                  permissionRequest,
                  decision: resolved,
                }),
              );
              if (resolved === "cancel") {
                return { outcome: { outcome: "cancelled" as const } };
              }
              return {
                outcome: {
                  outcome: "selected" as const,
                  optionId: selectDevinPermissionOptionId(params.options, resolved),
                },
              };
            }),
          );

          const started = yield* acp
            .start()
            .pipe(
              Effect.mapError((error) =>
                mapAcpToAdapterError(PROVIDER, input.threadId, "session/start", error),
              ),
            );
          yield* applySessionConfiguration(input.threadId, acp, model, input.runtimeMode);

          const now = yield* nowIso;
          const session: ProviderSession = {
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            status: "ready",
            runtimeMode: input.runtimeMode,
            cwd,
            model,
            threadId: input.threadId,
            resumeCursor: { schemaVersion: DEVIN_RESUME_VERSION, sessionId: started.sessionId },
            createdAt: now,
            updatedAt: now,
          };
          const context: DevinSessionContext = {
            threadId: input.threadId,
            session,
            scope: sessionScope,
            acp,
            pendingApprovals,
            turns: [],
            activeTurnId: undefined,
            promptsInFlight: 0,
            stopped: false,
          };
          ctx = context;

          const stamp = () =>
            Effect.map(makeEventStamp(), (eventStamp) => ({
              stamp: eventStamp,
              provider: PROVIDER,
              threadId: context.threadId,
              turnId: context.activeTurnId,
            }));
          yield* Stream.runForEach(acp.getEvents(), (event) =>
            Effect.gen(function* () {
              switch (event._tag) {
                case "EventStreamBarrier":
                  return yield* Deferred.succeed(event.acknowledge, undefined);
                case "AssistantItemStarted":
                case "AssistantItemCompleted":
                  return yield* offerRuntimeEvent(
                    makeAcpAssistantItemEvent({
                      ...(yield* stamp()),
                      itemId: event.itemId,
                      lifecycle:
                        event._tag === "AssistantItemStarted" ? "item.started" : "item.completed",
                    }),
                  );
                case "PlanUpdated":
                  return yield* offerRuntimeEvent(
                    makeAcpPlanUpdatedEvent({
                      ...(yield* stamp()),
                      payload: event.payload,
                      source: "acp.jsonrpc",
                      method: "session/update",
                      rawPayload: event.rawPayload,
                    }),
                  );
                case "ToolCallUpdated":
                  return yield* offerRuntimeEvent(
                    makeAcpToolCallEvent({
                      ...(yield* stamp()),
                      toolCall: event.toolCall,
                      rawPayload: event.rawPayload,
                    }),
                  );
                case "ThoughtDelta":
                case "ContentDelta":
                  return yield* offerRuntimeEvent(
                    makeAcpContentDeltaEvent({
                      ...(yield* stamp()),
                      ...(event._tag === "ContentDelta" && event.itemId
                        ? { itemId: event.itemId }
                        : {}),
                      ...(event._tag === "ThoughtDelta"
                        ? { streamKind: "reasoning_text" as const }
                        : {}),
                      text: event.text,
                      rawPayload: event.rawPayload,
                    }),
                  );
                default:
                  return;
              }
            }),
          ).pipe(Effect.forkIn(sessionScope));

          sessions.set(input.threadId, context);
          sessionScopeTransferred = true;

          yield* offerRuntimeEvent({
            type: "session.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { resume: started.initializeResult },
          });
          yield* offerRuntimeEvent({
            type: "session.state.changed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { state: "ready", reason: "Devin ACP session ready" },
          });
          yield* offerRuntimeEvent({
            type: "thread.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { providerThreadId: started.sessionId },
          });
          return session;
        }).pipe(Effect.scoped),
      );

    const readImageAttachment = (attachment: ChatAttachment) =>
      Effect.gen(function* () {
        const attachmentPath = resolveAttachmentPath({
          attachmentsDir: serverConfig.attachmentsDir,
          attachment,
        });
        if (!attachmentPath) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "session/prompt",
            detail: `Invalid attachment id '${attachment.id}'.`,
          });
        }
        const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "session/prompt",
                detail: cause.message,
                cause,
              }),
          ),
        );
        return {
          type: "image",
          data: Buffer.from(bytes).toString("base64"),
          mimeType: attachment.mimeType,
        } satisfies EffectAcpSchema.ContentBlock;
      });

    const sendTurn: ProviderAdapterShape<ProviderAdapterError>["sendTurn"] = (input) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(input.threadId);
        const rawPrompt = input.input?.trim() ?? "";
        const images = (input.attachments ?? []).filter(
          (attachment) => attachment.type === "image",
        );
        if (!rawPrompt && images.length === 0) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: "Turn requires non-empty text or attachments.",
          });
        }
        const freshTurnId = TurnId.make(yield* randomId);
        // Reserve the turn before any async work so a concurrent send steers this turn.
        const steeringTurnId = ctx.promptsInFlight > 0 ? ctx.activeTurnId : undefined;
        const turnId = steeringTurnId ?? freshTurnId;
        ctx.promptsInFlight += 1;
        ctx.activeTurnId = turnId;

        return yield* Effect.gen(function* () {
          const model =
            (input.modelSelection?.instanceId === boundInstanceId
              ? input.modelSelection.model
              : undefined) ?? ctx.session.model;
          yield* applySessionConfiguration(input.threadId, ctx.acp, model, ctx.session.runtimeMode);
          const promptParts: Array<EffectAcpSchema.ContentBlock> = [];
          if (rawPrompt) promptParts.push({ type: "text", text: rawPrompt });
          for (const image of images) {
            promptParts.push(yield* readImageAttachment(image));
          }

          ctx.session = { ...ctx.session, model, activeTurnId: turnId, updatedAt: yield* nowIso };
          if (steeringTurnId === undefined) {
            yield* offerRuntimeEvent({
              type: "turn.started",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: input.threadId,
              turnId,
              payload: model ? { model } : {},
            });
          }

          // ACP commands parse the complete text, so slash commands go through untouched.
          const result = yield* ctx.acp
            .prompt({
              prompt: /^\/[^\s/]+(?:\s|$)/.test(rawPrompt)
                ? promptParts
                : [
                    ...promptParts,
                    { type: "text", text: buildRuntimeInstructions({ harness: "Devin", model }) },
                  ],
            })
            .pipe(
              Effect.mapError((error) =>
                mapAcpToAdapterError(PROVIDER, input.threadId, "session/prompt", error),
              ),
            );
          yield* ctx.acp.drainEvents;

          const turnRecord = ctx.turns.find((turn) => turn.id === turnId);
          if (turnRecord) {
            turnRecord.items.push({ prompt: promptParts, result });
          } else {
            ctx.turns.push({ id: turnId, items: [{ prompt: promptParts, result }] });
          }
          // A steer-superseded prompt resolving early must leave the merged turn running.
          if (ctx.promptsInFlight === 1) {
            yield* offerRuntimeEvent({
              type: "turn.completed",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: input.threadId,
              turnId,
              payload: {
                state: result.stopReason === "cancelled" ? "cancelled" : "completed",
                stopReason: result.stopReason ?? null,
              },
            });
          }
          return { threadId: input.threadId, turnId, resumeCursor: ctx.session.resumeCursor };
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              ctx.promptsInFlight = Math.max(0, ctx.promptsInFlight - 1);
              if (ctx.promptsInFlight === 0 && ctx.activeTurnId === turnId) {
                ctx.activeTurnId = undefined;
                const { activeTurnId: _settled, ...session } = ctx.session;
                ctx.session = session;
              }
            }),
          ),
        );
      });

    const interruptTurn: ProviderAdapterShape<ProviderAdapterError>["interruptTurn"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        for (const decision of ctx.pendingApprovals.values()) {
          yield* Deferred.succeed(decision, "cancel");
        }
        yield* Effect.ignore(ctx.acp.cancel);
      });

    const respondToRequest: ProviderAdapterShape<ProviderAdapterError>["respondToRequest"] = (
      threadId,
      requestId,
      decision,
    ) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const pending = ctx.pendingApprovals.get(requestId);
        if (!pending) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "session/request_permission",
            detail: `Unknown pending approval request: ${requestId}`,
          });
        }
        yield* Deferred.succeed(pending, decision);
      });

    const respondToUserInput: ProviderAdapterShape<ProviderAdapterError>["respondToUserInput"] = (
      _threadId,
      requestId,
    ) =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "user-input",
          detail: `Devin does not request structured user input: ${requestId}`,
        }),
      );

    const readThread: ProviderAdapterShape<ProviderAdapterError>["readThread"] = (threadId) =>
      Effect.map(requireSession(threadId), (ctx) => ({ threadId, turns: ctx.turns }));

    const rollbackThread: ProviderAdapterShape<ProviderAdapterError>["rollbackThread"] = (
      threadId,
    ) =>
      Effect.flatMap(requireSession(threadId), () =>
        Effect.fail(
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "thread/rollback",
            detail: "Devin ACP sessions do not support provider-side rollback.",
          }),
        ),
      );

    const stopSession: ProviderAdapterShape<ProviderAdapterError>["stopSession"] = (threadId) =>
      withThreadLock(threadId, Effect.flatMap(requireSession(threadId), stopSessionInternal));

    const stopAll = () => Effect.forEach([...sessions.values()], stopSessionInternal);

    yield* Effect.addFinalizer(() =>
      stopAll().pipe(Effect.andThen(PubSub.shutdown(runtimeEventPubSub))),
    );

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "in-session", supportsConversationRollback: false },
      startSession,
      sendTurn,
      interruptTurn,
      readThread,
      rollbackThread,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions: () => Effect.sync(() => Array.from(sessions.values(), (c) => c.session)),
      hasSession: (threadId) => Effect.sync(() => sessions.get(threadId)?.stopped === false),
      stopAll: () => Effect.asVoid(stopAll()),
      streamEvents: Stream.fromPubSub(runtimeEventPubSub),
    } satisfies ProviderAdapterShape<ProviderAdapterError>;
  });
}
