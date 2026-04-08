// Probe for pmxt (pmxtjs npm package) — verifies the cross-venue
// abstraction can fetch one market from each supported platform.
//
// Exit codes follow candidates/probes/README.md:
//   0 — success: at least one market returned from each supported platform
//   2 — missing credentials (PMXT_POLYMARKET_PRIVATE_KEY etc. unset for
//       endpoints that require auth)
//   1 — anything else
//
// Public market-data endpoints on Polymarket and Kalshi do not require
// credentials, so the default code path is keyless. The probe is
// defensive: it pulls the SDK lazily and tolerates API drift across
// pmxtjs releases by trying several known method names before giving up.

const REQUIRED_PLATFORMS = ["polymarket", "kalshi"];

interface ProbeSummary {
  ok: boolean;
  tool: string;
  platforms: Record<string, { count: number; sample: string | null }>;
}

async function fetchMarketsForPlatform(
  pmxt: any,
  platform: string,
): Promise<{ count: number; sample: string | null }> {
  // Try several known method names — pmxtjs has reshaped its public
  // surface a few times. Falling back instead of throwing keeps the
  // probe useful across minor versions.
  const candidates = [
    () => pmxt.markets?.list?.({ platform }),
    () => pmxt.markets?.get?.({ platform }),
    () => pmxt.getMarkets?.({ platform }),
    () => pmxt.fetchMarkets?.({ platform }),
    () => pmxt[platform]?.markets?.list?.(),
    () => pmxt[platform]?.getMarkets?.(),
  ];
  for (const call of candidates) {
    try {
      const maybe = call();
      if (maybe === undefined || maybe === null) continue;
      const resolved = await Promise.resolve(maybe);
      const list = Array.isArray(resolved)
        ? resolved
        : resolved?.data ?? resolved?.markets ?? [];
      if (Array.isArray(list) && list.length > 0) {
        const first = list[0];
        const sample =
          first?.id ?? first?.market_id ?? first?.condition_id ?? null;
        return { count: list.length, sample: sample ? String(sample) : null };
      }
    } catch (_err) {
      // try the next shape
    }
  }
  return { count: 0, sample: null };
}

async function main(): Promise<number> {
  let pmxt: any;
  try {
    // pmxtjs is published as ESM-or-CJS depending on version; require()
    // is the safest bet under a node script invoked directly by the harness.
    pmxt = require("pmxtjs");
  } catch (err) {
    process.stderr.write(`pmxtjs import failed: ${(err as Error).message}\n`);
    return 1;
  }

  const summary: ProbeSummary = {
    ok: true,
    tool: "pmxt",
    platforms: {},
  };

  for (const platform of REQUIRED_PLATFORMS) {
    let result: { count: number; sample: string | null };
    try {
      result = await fetchMarketsForPlatform(pmxt, platform);
    } catch (err) {
      process.stderr.write(
        `pmxt fetch ${platform} failed: ${(err as Error).message}\n`,
      );
      return 1;
    }
    summary.platforms[platform] = result;
    if (result.count === 0) {
      summary.ok = false;
    }
  }

  if (!summary.ok) {
    process.stderr.write(
      `pmxt returned no markets for at least one platform: ${JSON.stringify(
        summary.platforms,
      )}\n`,
    );
    return 1;
  }

  process.stdout.write(JSON.stringify(summary) + "\n");
  return 0;
}

main()
  .then((code) => process.exit(code))
  .catch((err) => {
    process.stderr.write(`pmxt probe crashed: ${(err as Error).message}\n`);
    process.exit(1);
  });
