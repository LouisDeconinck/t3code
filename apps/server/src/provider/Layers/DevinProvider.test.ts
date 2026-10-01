import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import { DevinSettings } from "@t3tools/contracts";

import { writeFakeCli } from "../../testUtils/fakeCli.ts";
import { checkDevinProviderStatus, parseDevinAuthStatus } from "./DevinProvider.ts";

const decodeDevinSettings = Schema.decodeSync(DevinSettings);

describe("parseDevinAuthStatus", () => {
  it("reads both login states from output, since the command exits 0 either way", () => {
    expect(parseDevinAuthStatus("Logged in (via Devin).\n").status).toBe("authenticated");
    expect(parseDevinAuthStatus("Not logged in.\n").status).toBe("unauthenticated");
    expect(parseDevinAuthStatus("something else\n").status).toBe("unknown");
  });
});

const fakeDevin = (authStatusOutput: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-devin-status-" });
    return writeFakeCli({
      directory,
      name: "devin",
      source: [
        "const args = process.argv.slice(2).join(' ');",
        "if (args === 'version') process.stdout.write('devin 3000.11.3 (9c803229faa4)\\n');",
        // @effect-diagnostics-next-line preferSchemaOverJson:off
        `else if (args === 'auth status') process.stdout.write(${JSON.stringify(authStatusOutput)});`,
        "else process.exit(2);",
        "",
      ].join("\n"),
    });
  });

it.layer(NodeServices.layer)("checkDevinProviderStatus", (it) => {
  it.effect("reports a logged-in CLI as ready with its version", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const binaryPath = yield* fakeDevin("Logged in (via Devin).\n");
        const snapshot = yield* checkDevinProviderStatus(
          decodeDevinSettings({ enabled: true, binaryPath }),
        );
        expect(snapshot.status).toBe("ready");
        expect(snapshot.installed).toBe(true);
        expect(snapshot.version).toBe("3000.11.3");
        expect(snapshot.auth.status).toBe("authenticated");
        expect(snapshot.models.map((model) => model.slug)).toContain("swe-2-high");
      }),
    ),
  );

  it.effect("asks the user to log in when the CLI has no stored login", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const binaryPath = yield* fakeDevin("Not logged in.\n");
        const snapshot = yield* checkDevinProviderStatus(
          decodeDevinSettings({ enabled: true, binaryPath }),
        );
        expect(snapshot.status).toBe("error");
        expect(snapshot.auth.status).toBe("unauthenticated");
        expect(snapshot.message).toContain("devin auth login");
      }),
    ),
  );

  it.effect("reports a missing binary", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkDevinProviderStatus(
        decodeDevinSettings({ enabled: true, binaryPath: "/definitely/not/installed/devin" }),
      );
      expect(snapshot.installed).toBe(false);
      expect(snapshot.status).toBe("error");
    }),
  );
});
