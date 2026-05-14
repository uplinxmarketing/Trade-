// Supabase is not used in this deployment — all data comes from the local backend API.
// This stub silently no-ops every call so imported components don't crash.

export const supabaseConfigured = true;

const noopQuery = (): any => {
  const q: any = {
    select: () => q,
    insert: () => q,
    update: () => q,
    upsert: () => q,
    delete: () => q,
    eq: () => q,
    gt: () => q,
    lt: () => q,
    gte: () => q,
    lte: () => q,
    order: () => q,
    limit: () => q,
    single: () => Promise.resolve({ data: null, error: null }),
    maybeSingle: () => Promise.resolve({ data: null, error: null }),
    then: (resolve: (v: any) => any) => Promise.resolve({ data: [], error: null }).then(resolve),
  };
  return q;
};

const noopChannel = () => ({
  on: () => noopChannel(),
  subscribe: (_cb?: any) => ({ unsubscribe: () => {} }),
});

export const supabase: any = {
  from: (_table: string) => noopQuery(),
  channel: (_name: string) => noopChannel(),
  removeChannel: (_ch: any) => Promise.resolve(),
  functions: {
    invoke: (_name: string, _opts?: any) => Promise.resolve({ data: null, error: { message: 'Supabase not configured' } }),
  },
  auth: {
    onAuthStateChange: (_cb: any) => ({ data: { subscription: { unsubscribe: () => {} } } }),
    getSession: () => Promise.resolve({ data: { session: null }, error: null }),
  },
};
