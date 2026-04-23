import { render, screen, waitFor } from '@testing-library/react';
import { useThemeColors } from '@/lib/useThemeColors';
import { expect, test } from 'vitest';

function ThemeColorProbe() {
  const colors = useThemeColors();

  return <pre data-testid="theme-colors">{JSON.stringify(colors)}</pre>;
}

test('useThemeColors reads dashboard semantic tokens from css variables', async () => {
  document.documentElement.style.setProperty('--coral', '#d84f3f');
  document.documentElement.style.setProperty('--green', '#1f8a70');
  document.documentElement.style.setProperty('--amber', '#d6a72d');
  document.documentElement.style.setProperty('--cyan', '#2d8aa6');
  document.documentElement.style.setProperty('--error', '#b23a2a');

  render(<ThemeColorProbe />);

  await waitFor(() => {
    expect(screen.getByTestId('theme-colors')).toHaveTextContent('"coral":"#d84f3f"');
    expect(screen.getByTestId('theme-colors')).toHaveTextContent('"green":"#1f8a70"');
    expect(screen.getByTestId('theme-colors')).toHaveTextContent('"amber":"#d6a72d"');
    expect(screen.getByTestId('theme-colors')).toHaveTextContent('"cyan":"#2d8aa6"');
    expect(screen.getByTestId('theme-colors')).toHaveTextContent('"error":"#b23a2a"');
  });
});
