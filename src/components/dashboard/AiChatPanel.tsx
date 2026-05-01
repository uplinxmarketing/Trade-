import { useState, useRef, useEffect } from 'react';
import { Send, Bot, User, Loader2, KeyRound, Eye, EyeOff, Check } from 'lucide-react';
import { Button } from '@/components/ui/button';
import ReactMarkdown from 'react-markdown';
import type { ChatMessage } from '@/lib/binance-types';

const API_KEY_STORAGE = 'anthropic_chat_api_key';

const AiChatPanel = () => {
  const [messages, setMessages] = useState<ChatMessage[]>([
    { role: 'assistant', content: 'Hello! I\'m your AI trading assistant. Ask me about market trends, trading strategies, or portfolio analysis.\n\n*The trading bot runs without any API key. This chat is optional — add your Anthropic key below to enable it.*' },
  ]);
  const [input, setInput]         = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [apiKey, setApiKey]       = useState(() => localStorage.getItem(API_KEY_STORAGE) ?? '');
  const [keyDraft, setKeyDraft]   = useState('');
  const [showKeyInput, setShowKeyInput] = useState(false);
  const [showKey, setShowKey]     = useState(false);
  const [keySaved, setKeySaved]   = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: 'smooth' });
  }, [messages]);

  const saveKey = () => {
    const trimmed = keyDraft.trim();
    setApiKey(trimmed);
    localStorage.setItem(API_KEY_STORAGE, trimmed);
    setShowKeyInput(false);
    setKeySaved(true);
    setTimeout(() => setKeySaved(false), 3000);
  };

  const clearKey = () => {
    setApiKey('');
    setKeyDraft('');
    localStorage.removeItem(API_KEY_STORAGE);
    setShowKeyInput(false);
  };

  const sendMessage = async () => {
    if (!input.trim() || isLoading) return;
    if (!apiKey) {
      setShowKeyInput(true);
      return;
    }
    const userMsg: ChatMessage = { role: 'user', content: input.trim() };
    const newMessages = [...messages, userMsg];
    setMessages(newMessages);
    setInput('');
    setIsLoading(true);

    try {
      const resp = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ messages: newMessages, apiKey }),
      });

      if (!resp.ok || !resp.body) throw new Error(`Chat error ${resp.status}`);

      const reader  = resp.body.getReader();
      const decoder = new TextDecoder();
      let textBuffer = '';
      let assistantSoFar = '';

      const upsertAssistant = (chunk: string) => {
        assistantSoFar += chunk;
        setMessages(prev => {
          const last = prev[prev.length - 1];
          if (last?.role === 'assistant' && prev.length > newMessages.length) {
            return prev.map((m, i) => i === prev.length - 1 ? { ...m, content: assistantSoFar } : m);
          }
          return [...prev, { role: 'assistant', content: assistantSoFar }];
        });
      };

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        textBuffer += decoder.decode(value, { stream: true });
        let nl: number;
        while ((nl = textBuffer.indexOf('\n')) !== -1) {
          let line = textBuffer.slice(0, nl);
          textBuffer = textBuffer.slice(nl + 1);
          if (line.endsWith('\r')) line = line.slice(0, -1);
          if (line.startsWith(':') || line.trim() === '') continue;
          if (!line.startsWith('data: ')) continue;
          const jsonStr = line.slice(6).trim();
          if (jsonStr === '[DONE]') break;
          try {
            const parsed = JSON.parse(jsonStr);
            const content = parsed.choices?.[0]?.delta?.content;
            if (content) upsertAssistant(content);
          } catch {
            textBuffer = line + '\n' + textBuffer;
            break;
          }
        }
      }
    } catch (e) {
      console.error(e);
      setMessages(prev => [...prev, { role: 'assistant', content: 'Sorry, I encountered an error. Please try again.' }]);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="trading-card flex flex-col h-full animate-slide-in-right" style={{ animationDelay: '200ms' }}>
      <div className="p-3 border-b border-border flex items-center gap-2">
        <Bot className="w-4 h-4 text-primary" />
        <span className="text-sm font-medium">AI Assistant</span>
        <div className="pulse-dot ml-auto" />
        {/* API key status + toggle */}
        <button
          onClick={() => { setKeyDraft(apiKey); setShowKeyInput(!showKeyInput); }}
          className={`flex items-center gap-1 text-[10px] px-2 py-0.5 rounded border transition-colors ${
            apiKey ? 'border-gain/40 text-gain hover:bg-gain/10' : 'border-warn/40 text-warn hover:bg-warn/10'
          }`}
          title={apiKey ? 'API key saved — click to change' : 'Add API key to enable chat'}
        >
          {keySaved ? <Check className="w-3 h-3" /> : <KeyRound className="w-3 h-3" />}
          {keySaved ? 'Saved' : apiKey ? 'Key ✓' : 'Add key'}
        </button>
      </div>

      {/* Inline API key input */}
      {showKeyInput && (
        <div className="px-3 py-2.5 border-b border-border bg-muted/20 space-y-2">
          <p className="text-[10px] text-muted-foreground leading-relaxed">
            Paste your <span className="font-semibold text-foreground">Anthropic API key</span> to enable this chat.
            The trading bot works without it — this is only for the chat sidebar.
            Get a free key at <span className="text-accent">console.anthropic.com</span>.
          </p>
          <div className="flex gap-1">
            <div className="relative flex-1">
              <input
                type={showKey ? 'text' : 'password'}
                value={keyDraft}
                onChange={e => setKeyDraft(e.target.value)}
                onKeyDown={e => { if (e.key === 'Enter') saveKey(); if (e.key === 'Escape') setShowKeyInput(false); }}
                placeholder="sk-ant-api03-..."
                autoFocus
                className="w-full bg-muted/40 border border-border rounded px-2 py-1.5 text-xs font-mono text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-accent pr-8"
              />
              <button onClick={() => setShowKey(!showKey)}
                className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground">
                {showKey ? <EyeOff className="w-3 h-3" /> : <Eye className="w-3 h-3" />}
              </button>
            </div>
            <button onClick={saveKey} disabled={!keyDraft.trim()}
              className="px-2.5 py-1.5 bg-accent text-accent-foreground rounded text-[10px] font-semibold disabled:opacity-50 hover:bg-accent/90">
              Save
            </button>
            {apiKey && (
              <button onClick={clearKey}
                className="px-2 py-1.5 text-loss hover:bg-loss/10 rounded text-[10px] border border-loss/30">
                Clear
              </button>
            )}
          </div>
        </div>
      )}

      <div ref={scrollRef} className="flex-1 overflow-y-auto p-3 space-y-3 scrollbar-thin">
        {messages.map((msg, i) => (
          <div key={i} className={`flex gap-2 ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
            {msg.role === 'assistant' && (
              <div className="w-6 h-6 rounded-full bg-primary/15 flex items-center justify-center flex-shrink-0 mt-0.5">
                <Bot className="w-3.5 h-3.5 text-primary" />
              </div>
            )}
            <div className={`max-w-[85%] rounded-lg px-3 py-2 text-sm ${
              msg.role === 'user' ? 'bg-primary/15 text-foreground' : 'bg-secondary text-foreground'
            }`}>
              {msg.role === 'assistant' ? (
                <div className="prose prose-sm prose-invert max-w-none [&_p]:my-1 [&_pre]:bg-background [&_pre]:rounded [&_pre]:p-2 [&_code]:text-xs [&_code]:font-mono">
                  <ReactMarkdown>{msg.content}</ReactMarkdown>
                </div>
              ) : msg.content}
            </div>
            {msg.role === 'user' && (
              <div className="w-6 h-6 rounded-full bg-secondary flex items-center justify-center flex-shrink-0 mt-0.5">
                <User className="w-3.5 h-3.5 text-muted-foreground" />
              </div>
            )}
          </div>
        ))}
        {isLoading && messages[messages.length - 1]?.role === 'user' && (
          <div className="flex gap-2">
            <div className="w-6 h-6 rounded-full bg-primary/15 flex items-center justify-center flex-shrink-0">
              <Bot className="w-3.5 h-3.5 text-primary" />
            </div>
            <div className="bg-secondary rounded-lg px-3 py-2">
              <Loader2 className="w-4 h-4 animate-spin text-muted-foreground" />
            </div>
          </div>
        )}
      </div>

      <div className="p-3 border-t border-border">
        {!apiKey && (
          <p className="text-[10px] text-warn text-center mb-2">
            Add an Anthropic API key above to use this chat.{' '}
            <button onClick={() => { setKeyDraft(''); setShowKeyInput(true); }} className="underline">Add key →</button>
          </p>
        )}
        <form onSubmit={(e) => { e.preventDefault(); sendMessage(); }} className="flex gap-2">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder={apiKey ? 'Ask about markets…' : 'Add API key to chat…'}
            className="flex-1 bg-secondary border border-border rounded-lg px-3 py-2 text-sm text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
            disabled={isLoading}
          />
          <Button type="submit" size="icon" className="h-9 w-9" disabled={isLoading || !input.trim()}>
            <Send className="w-4 h-4" />
          </Button>
        </form>
      </div>
    </div>
  );
};

export default AiChatPanel;
