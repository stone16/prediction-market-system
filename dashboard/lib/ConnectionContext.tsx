'use client';

import {
  createContext,
  useContext,
  useMemo,
  useState,
  type ReactNode
} from 'react';

export type ConnectionState = 'connected' | 'reconnecting' | 'disconnected';

type ConnectionContextValue = {
  lastFetchAt: number | null;
  markConnected: () => void;
  markDisconnected: () => void;
  markReconnecting: () => void;
  retryToken: () => void;
  retryVersion: number;
  state: ConnectionState;
};

const ConnectionContext = createContext<ConnectionContextValue | null>(null);

export function ConnectionProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<ConnectionState>('connected');
  const [lastFetchAt, setLastFetchAt] = useState<number | null>(null);
  const [retryVersion, setRetryVersion] = useState(0);

  const value = useMemo<ConnectionContextValue>(
    () => ({
      lastFetchAt,
      markConnected: () => {
        setState('connected');
        setLastFetchAt(Date.now());
      },
      markDisconnected: () => {
        setState('disconnected');
      },
      markReconnecting: () => {
        setState('reconnecting');
      },
      retryToken: () => {
        setRetryVersion((current) => current + 1);
      },
      retryVersion,
      state
    }),
    [lastFetchAt, retryVersion, state]
  );

  return <ConnectionContext.Provider value={value}>{children}</ConnectionContext.Provider>;
}

export function useConnection() {
  const context = useContext(ConnectionContext);

  if (!context) {
    throw new Error('useConnection must be used inside ConnectionProvider');
  }

  return context;
}
