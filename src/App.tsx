import { Component, type ReactNode } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { BrowserRouter, Route, Routes } from 'react-router-dom';
import { Toaster as Sonner } from '@/components/ui/sonner';
import { Toaster } from '@/components/ui/toaster';
import { TooltipProvider } from '@/components/ui/tooltip';
import Index from './pages/Index.tsx';
import { DiagnosticsPanel } from './components/DiagnosticsPanel';
import NotFound from './pages/NotFound.tsx';
import SetupRequired from './pages/SetupRequired.tsx';
import { supabaseConfigured } from '@/integrations/supabase/client';

// ── Error boundary — catches render crashes and shows a readable message ──────
interface ErrorBoundaryState { error: Error | null }
class ErrorBoundary extends Component<{ children: ReactNode }, ErrorBoundaryState> {
  state: ErrorBoundaryState = { error: null };
  static getDerivedStateFromError(error: Error) { return { error }; }
  render() {
    if (this.state.error) {
      return (
        <div className="min-h-screen bg-background flex items-center justify-center p-8">
          <div className="w-full max-w-lg bg-card border border-loss/30 rounded-lg p-6 space-y-4">
            <h2 className="text-base font-semibold text-loss">App crashed on startup</h2>
            <pre className="text-xs text-muted-foreground bg-muted/30 rounded p-3 overflow-auto max-h-48 whitespace-pre-wrap">
              {this.state.error.message}
              {'\n\n'}
              {this.state.error.stack}
            </pre>
            <p className="text-xs text-muted-foreground">
              Check that your <code className="bg-muted px-1 rounded">.env</code> file exists and has valid Supabase credentials,
              then close and re-run <code className="bg-muted px-1 rounded">start.bat</code>.
            </p>
            <button
              onClick={() => window.location.reload()}
              className="text-xs bg-accent text-accent-foreground px-4 py-2 rounded hover:opacity-90"
            >
              Reload
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: 1 } },
});

const App = () => {
  if (!supabaseConfigured) {
    return <SetupRequired />;
  }

  return (
    <ErrorBoundary>
      <QueryClientProvider client={queryClient}>
        <TooltipProvider>
          <Toaster />
          <Sonner position="top-center" richColors />
          <BrowserRouter>
            <Routes>
              <Route path="/" element={<Index />} />
              <Route path="*" element={<NotFound />} />
            </Routes>
          </BrowserRouter>
          <DiagnosticsPanel />
        </TooltipProvider>
      </QueryClientProvider>
    </ErrorBoundary>
  );
};

export default App;
