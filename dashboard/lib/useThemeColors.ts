'use client';

import { useEffect, useState } from 'react';

export type ThemeColors = {
  amber: string;
  coral: string;
  cyan: string;
  error: string;
  green: string;
};

const EMPTY_COLORS: ThemeColors = {
  amber: '',
  coral: '',
  cyan: '',
  error: '',
  green: ''
};

function readColor(variableName: string) {
  return getComputedStyle(document.documentElement).getPropertyValue(variableName).trim();
}

export function useThemeColors() {
  const [colors, setColors] = useState<ThemeColors>(EMPTY_COLORS);

  useEffect(() => {
    setColors({
      amber: readColor('--amber'),
      coral: readColor('--coral'),
      cyan: readColor('--cyan'),
      error: readColor('--error'),
      green: readColor('--green')
    });
  }, []);

  return colors;
}
