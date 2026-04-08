// Probe for @polymarket/real-time-data-client — connects to the public
// Polymarket RTDS WebSocket, subscribes to the trades topic, waits for
// one frame, then disconnects cleanly.
//
// Public RTDS topics do not need credentials, so this probe is keyless.
// Exit codes follow candidates/probes/README.md.

const RTDS_WS_URL = "wss://ws-live-data.polymarket.com";
const SUBSCRIBE_TOPIC = "trades";
const FRAME_TIMEOUT_MS = 15000;

interface ProbeSummary {
  ok: boolean;
  tool: string;
  endpoint: string;
  topic: string;
  first_frame_bytes: number;
}

async function main(): Promise<number> {
  let RealTimeDataClient: any;
  try {
    // The package's default export shape has shifted across releases —
    // try the namespace import first, then a default fallback.
    const mod = require("@polymarket/real-time-data-client");
    RealTimeDataClient =
      mod?.RealTimeDataClient ?? mod?.default ?? mod?.Client ?? mod;
  } catch (err) {
    process.stderr.write(
      `@polymarket/real-time-data-client import failed: ${(err as Error).message}\n`,
    );
    return 1;
  }

  if (typeof RealTimeDataClient !== "function") {
    process.stderr.write(
      "@polymarket/real-time-data-client did not export a constructor\n",
    );
    return 1;
  }

  let client: any;
  try {
    client = new RealTimeDataClient({ endpoint: RTDS_WS_URL });
  } catch (err) {
    process.stderr.write(
      `RTDS client construction failed: ${(err as Error).message}\n`,
    );
    return 1;
  }

  const result = await new Promise<{ ok: boolean; bytes: number; error?: string }>(
    (resolve) => {
      let resolved = false;
      const finish = (value: { ok: boolean; bytes: number; error?: string }) => {
        if (resolved) return;
        resolved = true;
        try {
          client.disconnect?.();
          client.close?.();
        } catch (_err) {
          // ignore — we're already finishing
        }
        resolve(value);
      };

      const timer = setTimeout(() => {
        finish({ ok: false, bytes: 0, error: "no frame within timeout" });
      }, FRAME_TIMEOUT_MS);

      const onMessage = (msg: unknown) => {
        clearTimeout(timer);
        const bytes =
          typeof msg === "string"
            ? Buffer.byteLength(msg, "utf8")
            : msg
            ? Buffer.byteLength(JSON.stringify(msg), "utf8")
            : 0;
        finish({ ok: true, bytes });
      };

      try {
        // Subscribe API has varied — wire both the event-emitter form and
        // the on()/onMessage() form.
        if (typeof client.on === "function") {
          client.on("message", onMessage);
          client.on("trade", onMessage);
        }
        if (typeof client.onMessage === "function") {
          client.onMessage(onMessage);
        }
        if (typeof client.subscribe === "function") {
          client.subscribe([SUBSCRIBE_TOPIC]);
        } else if (typeof client.connect === "function") {
          client.connect();
        }
      } catch (err) {
        clearTimeout(timer);
        finish({ ok: false, bytes: 0, error: (err as Error).message });
      }
    },
  );

  if (!result.ok) {
    process.stderr.write(
      `real-time-data-client probe failed: ${result.error ?? "unknown"}\n`,
    );
    return 1;
  }

  const summary: ProbeSummary = {
    ok: true,
    tool: "real-time-data-client",
    endpoint: RTDS_WS_URL,
    topic: SUBSCRIBE_TOPIC,
    first_frame_bytes: result.bytes,
  };
  process.stdout.write(JSON.stringify(summary) + "\n");
  return 0;
}

main()
  .then((code) => process.exit(code))
  .catch((err) => {
    process.stderr.write(`RTDS probe crashed: ${(err as Error).message}\n`);
    process.exit(1);
  });
