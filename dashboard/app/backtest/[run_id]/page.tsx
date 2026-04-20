import { BacktestRunView } from '@/components/backtest/BacktestRunView';

type BacktestRunPageProps = {
  params: Promise<{ run_id: string }>;
};

export default async function BacktestRunPage({ params }: BacktestRunPageProps) {
  const { run_id: runId } = await params;
  return <BacktestRunView runId={runId} />;
}
