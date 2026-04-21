'use client';

import { useDashboardSource } from './SourceProvider';

const mockBannerText = 'MOCK DATA — backend disconnected. Set `PMS_API_BASE_URL` to connect.';

export function SourceBanner() {
  const source = useDashboardSource();

  if (source !== 'mock') {
    return null;
  }

  return (
    <div className="source-banner" data-testid="source-banner" role="status">
      {mockBannerText}
    </div>
  );
}

export { mockBannerText };
