/**
 * DevinDriver — `ProviderDriver` for the Devin CLI, run through `devin acp`.
 *
 * Sessions reuse the credentials stored by `devin auth login`. Git text
 * generation is not supported yet, so snapshots opt out of it.
 *
 * @module provider/Drivers/DevinDriver
 */
import { DevinSettings, ProviderDriverKind, TextGenerationError } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeDevinAdapter } from "../Layers/DevinAdapter.ts";
import {
  buildInitialDevinProviderSnapshot,
  checkDevinProviderStatus,
} from "../Layers/DevinProvider.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";
import { withInstanceIdentity } from "./instanceIdentity.ts";

const decodeDevinSettings = Schema.decodeSync(DevinSettings);
const DRIVER_KIND = ProviderDriverKind.make("devin");

const unsupportedTextGeneration = (operation: string) =>
  Effect.fail(
    new TextGenerationError({ operation, detail: "Devin does not support text generation yet." }),
  );

export type DevinDriverEnv =
  | BackgroundPolicy.BackgroundPolicy
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | Path.Path
  | ServerConfig
  | ServerSettingsService;

export const DevinDriver: ProviderDriver<DevinSettings, DevinDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "Devin",
    supportsMultipleInstances: true,
  },
  configSchema: DevinSettings,
  defaultConfig: (): DevinSettings => decodeDevinSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const serverSettings = yield* ServerSettingsService;
      const processEnv = mergeProviderInstanceEnvironment(environment);
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        driverKind: DRIVER_KIND,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const effectiveConfig = { ...config, enabled } satisfies DevinSettings;
      const withoutTextGeneration = (snapshot: Parameters<typeof stampIdentity>[0]) => ({
        ...stampIdentity(snapshot),
        supportsTextGeneration: false,
      });

      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<DevinSettings>>({
        resolveMaintenance: () =>
          Effect.succeed(
            makeManualOnlyProviderMaintenanceCapabilities({
              provider: DRIVER_KIND,
              packageName: null,
            }),
          ),
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          buildInitialDevinProviderSnapshot(settings.provider).pipe(
            Effect.map(withoutTextGeneration),
          ),
        checkProvider: checkDevinProviderStatus(effectiveConfig, processEnv).pipe(
          Effect.map(withoutTextGeneration),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        ),
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: `Failed to build Devin snapshot: ${cause.message ?? String(cause)}`,
              cause,
            }),
        ),
      );
      const adapter = yield* makeDevinAdapter(effectiveConfig, {
        environment: processEnv,
        instanceId,
      });

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        adapter,
        textGeneration: {
          generateCommitMessage: () => unsupportedTextGeneration("generateCommitMessage"),
          generatePrContent: () => unsupportedTextGeneration("generatePrContent"),
          generateBranchName: () => unsupportedTextGeneration("generateBranchName"),
          generateThreadTitle: () => unsupportedTextGeneration("generateThreadTitle"),
        },
      } satisfies ProviderInstance;
    }),
};
