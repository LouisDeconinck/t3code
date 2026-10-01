import type { DevinSettings, ModelCapabilities, ServerProviderModel } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import { ChildProcess } from "effect/unstable/process";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import {
  AUTH_PROBE_TIMEOUT_MS,
  buildServerProvider,
  DEFAULT_TIMEOUT_MS,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ProviderProbeResult,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const DEVIN_PRESENTATION = {
  displayName: "Devin",
  supportsConversationRollback: false,
  badgeLabel: "Early Access",
  showInteractionModeToggle: false,
} as const;

const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({ optionDescriptors: [] });

// Slugs are ACP `model` config values, which differ from `devin models list` ids.
// Other models from the account's catalog can be added as custom models.
const DEVIN_BUILT_IN_MODELS: ReadonlyArray<ServerProviderModel> = [
  { slug: "swe-2-high", name: "SWE-2", isCustom: false, capabilities: EMPTY_CAPABILITIES },
];

function devinModels(settings: DevinSettings) {
  return providerModelsFromSettings(
    DEVIN_BUILT_IN_MODELS,
    settings.customModels,
    EMPTY_CAPABILITIES,
  );
}

/** `devin auth status` exits 0 whether or not a login is stored. */
export function parseDevinAuthStatus(output: string): ProviderProbeResult["auth"] {
  if (/^\s*Not logged in/im.test(output)) return { status: "unauthenticated" };
  if (/^\s*Logged in/im.test(output)) {
    return { status: "authenticated", type: "cached_token", label: "Devin account" };
  }
  return { status: "unknown" };
}

const runDevinCommand = (
  settings: DevinSettings,
  args: ReadonlyArray<string>,
  environment: NodeJS.ProcessEnv,
) =>
  Effect.gen(function* () {
    const spawnCommand = yield* resolveSpawnCommand(settings.binaryPath, args, {
      env: environment,
    });
    return yield* spawnAndCollect(
      settings.binaryPath,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  });

export const buildInitialDevinProviderSnapshot = (
  settings: DevinSettings,
): Effect.Effect<ServerProviderDraft> =>
  Effect.map(DateTime.now, (now) =>
    buildServerProvider({
      presentation: DEVIN_PRESENTATION,
      enabled: settings.enabled,
      checkedAt: DateTime.formatIso(now),
      models: devinModels(settings),
      probe: {
        installed: settings.enabled,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: settings.enabled
          ? "Checking Devin CLI availability..."
          : "Devin is disabled in T3 Code settings.",
      },
    }),
  );

export const checkDevinProviderStatus = Effect.fn("checkDevinProviderStatus")(function* (
  settings: DevinSettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  if (!settings.enabled) {
    return yield* buildInitialDevinProviderSnapshot(settings);
  }
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const build = (probe: ProviderProbeResult) =>
    buildServerProvider({
      presentation: DEVIN_PRESENTATION,
      enabled: true,
      checkedAt,
      models: devinModels(settings),
      probe,
    });

  const versionResult = yield* runDevinCommand(settings, ["version"], environment).pipe(
    Effect.timeoutOption(DEFAULT_TIMEOUT_MS),
    Effect.result,
  );
  if (Result.isFailure(versionResult)) {
    const missing = isCommandMissingCause(versionResult.failure);
    return build({
      installed: !missing,
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message: missing
        ? "Devin CLI (`devin`) is not installed or not on PATH."
        : "Failed to execute Devin CLI health check.",
    });
  }
  const versionOutput = Option.getOrUndefined(versionResult.success);
  if (!versionOutput || versionOutput.code !== 0) {
    return build({
      installed: true,
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message: "Devin CLI is installed but `devin version` failed.",
    });
  }
  const version = parseGenericCliVersion(versionOutput.stdout);

  const authResult = yield* runDevinCommand(settings, ["auth", "status"], environment).pipe(
    Effect.timeoutOption(AUTH_PROBE_TIMEOUT_MS),
    Effect.result,
  );
  const authOutput =
    Result.isSuccess(authResult) && Option.isSome(authResult.success)
      ? authResult.success.value
      : undefined;
  const auth = authOutput
    ? parseDevinAuthStatus(`${authOutput.stdout}\n${authOutput.stderr}`)
    : ({ status: "unknown" } as const);

  if (auth.status === "unauthenticated") {
    return build({
      installed: true,
      version,
      status: "error",
      auth,
      message: "Devin CLI is not logged in. Run `devin auth login`.",
    });
  }
  return build({ installed: true, version, status: "ready", auth });
});
