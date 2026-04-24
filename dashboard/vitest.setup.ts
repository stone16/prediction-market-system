import { cleanup } from '@testing-library/react';
import '@testing-library/jest-dom/vitest';
import { afterEach, vi } from 'vitest';

function installMemoryLocalStorage(): void {
  const values = new Map<string, string>();
  const storage: Storage = {
    get length() {
      return values.size;
    },
    clear: () => values.clear(),
    getItem: (key: string) => values.get(key) ?? null,
    key: (index: number) => Array.from(values.keys())[index] ?? null,
    removeItem: (key: string) => {
      values.delete(key);
    },
    setItem: (key: string, value: string) => {
      values.set(key, value);
    },
  };

  Object.defineProperty(window, 'localStorage', {
    value: storage,
    configurable: true,
  });
}

if (typeof window.localStorage.clear !== 'function') {
  installMemoryLocalStorage();
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});
