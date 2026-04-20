import { BacktestComparePage } from '@/components/backtest/BacktestComparePage';

type BacktestCompareRoutePageProps = {
  params: Promise<{ run_id: string }>;
};

export default async function BacktestCompareRoutePage({
  params
}: BacktestCompareRoutePageProps) {
  const { run_id: runId } = await params;
  return <BacktestComparePage runId={runId} />;
}
