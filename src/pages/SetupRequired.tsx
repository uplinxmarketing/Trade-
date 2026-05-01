import { useState } from 'react';
import { Bot, Key, ExternalLink, CheckCircle2, Copy, Check } from 'lucide-react';

const SUPABASE_PROJECT_ID = 'wvrrsoggggmvopllyhxo';

const SetupRequired = () => {
  const [copied, setCopied] = useState<string | null>(null);

  const copy = (text: string, key: string) => {
    navigator.clipboard.writeText(text);
    setCopied(key);
    setTimeout(() => setCopied(null), 2000);
  };

  const envContent = `VITE_SUPABASE_URL=https://${SUPABASE_PROJECT_ID}.supabase.co\nVITE_SUPABASE_PUBLISHABLE_KEY=your-anon-key-here`;

  return (
    <div className="min-h-screen bg-background flex items-center justify-center p-6">
      <div className="w-full max-w-xl space-y-6">
        {/* Header */}
        <div className="text-center space-y-2">
          <div className="flex items-center justify-center gap-3 mb-4">
            <Bot className="w-10 h-10 text-accent" />
            <h1 className="text-2xl font-bold tracking-tight">DeepTrade AI</h1>
          </div>
          <div className="inline-flex items-center gap-2 bg-warn/10 border border-warn/30 text-warn rounded-md px-4 py-2 text-sm font-medium">
            <Key className="w-4 h-4" />
            Setup Required — Supabase not configured
          </div>
          <p className="text-sm text-muted-foreground mt-2">
            The app needs a <code className="bg-muted px-1.5 py-0.5 rounded text-xs">.env</code> file
            with your Supabase credentials to connect to the database.
          </p>
        </div>

        {/* Steps */}
        <div className="bg-card border border-border rounded-lg divide-y divide-border">
          {/* Step 1 */}
          <div className="p-4 space-y-2">
            <div className="flex items-center gap-2">
              <span className="w-6 h-6 rounded-full bg-accent/20 text-accent text-xs font-bold flex items-center justify-center">1</span>
              <h3 className="text-sm font-semibold">Get your Supabase Anon Key</h3>
            </div>
            <p className="text-xs text-muted-foreground pl-8">
              Open your Supabase project dashboard, go to{' '}
              <strong>Settings → API</strong> and copy the <strong>anon / public</strong> key.
            </p>
            <div className="pl-8">
              <a
                href={`https://supabase.com/dashboard/project/${SUPABASE_PROJECT_ID}/settings/api`}
                target="_blank"
                rel="noreferrer"
                className="inline-flex items-center gap-1.5 text-xs text-accent hover:underline"
              >
                <ExternalLink className="w-3 h-3" />
                Open Supabase API Settings
              </a>
            </div>
          </div>

          {/* Step 2 */}
          <div className="p-4 space-y-2">
            <div className="flex items-center gap-2">
              <span className="w-6 h-6 rounded-full bg-accent/20 text-accent text-xs font-bold flex items-center justify-center">2</span>
              <h3 className="text-sm font-semibold">Create a <code className="bg-muted px-1 rounded">.env</code> file</h3>
            </div>
            <p className="text-xs text-muted-foreground pl-8">
              In the <strong>DeepTrade folder</strong> (same folder as <code className="bg-muted px-1 rounded text-[10px]">start.bat</code>),
              create a new file named exactly <code className="bg-muted px-1 rounded text-[10px]">.env</code> and paste this:
            </p>
            <div className="pl-8">
              <div className="relative bg-muted/40 border border-border rounded-md p-3 font-mono text-[11px] leading-relaxed">
                <pre className="whitespace-pre-wrap text-foreground">
                  {`VITE_SUPABASE_URL=https://${SUPABASE_PROJECT_ID}.supabase.co`}{'\n'}
                  {`VITE_SUPABASE_PUBLISHABLE_KEY=`}<span className="text-warn">your-anon-key-here</span>
                </pre>
                <button
                  onClick={() => copy(envContent, 'env')}
                  className="absolute top-2 right-2 p-1.5 rounded bg-muted hover:bg-muted/80 text-muted-foreground hover:text-foreground transition-colors"
                  title="Copy to clipboard"
                >
                  {copied === 'env' ? <Check className="w-3.5 h-3.5 text-gain" /> : <Copy className="w-3.5 h-3.5" />}
                </button>
              </div>
              <p className="text-[10px] text-muted-foreground mt-1">
                Replace <code className="bg-muted px-1 rounded">your-anon-key-here</code> with the key from step 1.
              </p>
            </div>
          </div>

          {/* Step 3 */}
          <div className="p-4 space-y-2">
            <div className="flex items-center gap-2">
              <span className="w-6 h-6 rounded-full bg-accent/20 text-accent text-xs font-bold flex items-center justify-center">3</span>
              <h3 className="text-sm font-semibold">Restart the app</h3>
            </div>
            <p className="text-xs text-muted-foreground pl-8">
              Close the terminal window and double-click <strong>start.bat</strong> again.
              The app will load normally once the <code className="bg-muted px-1 rounded text-[10px]">.env</code> file is in place.
            </p>
          </div>
        </div>

        {/* Where to create the file */}
        <div className="bg-muted/20 border border-border rounded-lg p-4 space-y-1.5">
          <p className="text-[10px] uppercase tracking-widest text-muted-foreground font-semibold">Where to save the .env file</p>
          <div
            className="font-mono text-xs bg-muted/40 rounded px-3 py-2 flex items-center justify-between cursor-pointer hover:bg-muted/60"
            onClick={() => copy('C:\\DeepTrade\\.env', 'path')}
          >
            <span className="text-foreground">DeepTrade\.env</span>
            {copied === 'path' ? <Check className="w-3.5 h-3.5 text-gain" /> : <Copy className="w-3.5 h-3.5 text-muted-foreground" />}
          </div>
          <p className="text-[10px] text-muted-foreground">
            The <code>.env</code> file goes in the same folder as <code>start.bat</code> and <code>package.json</code>.
          </p>
        </div>

        <div className="flex items-center gap-2 text-[10px] text-muted-foreground justify-center">
          <CheckCircle2 className="w-3.5 h-3.5" />
          Your keys are used only for your local session and never shared.
        </div>
      </div>
    </div>
  );
};

export default SetupRequired;
