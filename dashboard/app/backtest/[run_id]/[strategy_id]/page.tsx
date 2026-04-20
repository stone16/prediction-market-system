import { BacktestStrategyDetailView } from '@/components/backtest/BacktestStrategyDetailView';

type BacktestStrategyPageProps = {
  params: Promise<{ run_id: string; strategy_id: string }>;
};

export default async function BacktestStrategyPage({ params }: BacktestStrategyPageProps) {
  const { run_id: runId, strategy_id: strategyId } = await params;
  return <BacktestStrategyDetailView runId={runId} strategyId={strategyId} />;
}
