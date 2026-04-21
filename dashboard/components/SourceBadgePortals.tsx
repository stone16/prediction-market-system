'use client';

import { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { SourceBadge } from './SourceBadge';
import { useDashboardSource } from './SourceProvider';

export function SourceBadgePortals() {
  const source = useDashboardSource();
  const [cards, setCards] = useState<HTMLElement[]>([]);

  useEffect(() => {
    if (source !== 'mock') {
      setCards([]);
      return undefined;
    }

    function collectCards() {
      setCards(Array.from(document.querySelectorAll<HTMLElement>('.card')));
    }

    collectCards();

    const observer = new MutationObserver(() => {
      collectCards();
    });

    observer.observe(document.body, {
      childList: true,
      subtree: true
    });

    return () => {
      observer.disconnect();
    };
  }, [source]);

  if (source !== 'mock') {
    return null;
  }

  return (
    <>
      {cards.map((card, index) =>
        createPortal(<SourceBadge source="mock" />, card, `source-badge-${index}`)
      )}
    </>
  );
}
