export type DashboardSource = 'live' | 'mock';

export function getDashboardSource(): DashboardSource {
  return process.env.PMS_API_BASE_URL ? 'live' : 'mock';
}
