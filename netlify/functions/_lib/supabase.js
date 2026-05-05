const { createClient } = require('@supabase/supabase-js');

let client;

const resolveSupabaseAdminKey = () => {
  return (
    process.env.SUPABASE_SERVICE_ROLE_KEY
    || process.env.SUPABASE_SECRET_KEY
    || process.env.SUPABASE_SERVICE_KEY
    || ''
  );
};

const classifySupabaseKey = (key) => {
  const value = String(key || '');
  if (!value) {
    return 'missing';
  }

  if (value.startsWith('sb_publishable_')) {
    return 'publishable';
  }

  if (value.startsWith('sb_secret_')) {
    return 'secret';
  }

  if (/^eyJ/.test(value)) {
    return 'jwt-legacy';
  }

  return 'unknown';
};

const getSupabaseAdmin = () => {
  if (client) {
    return client;
  }

  const supabaseUrl = process.env.SUPABASE_URL;
  const serviceRoleKey = resolveSupabaseAdminKey();

  if (!supabaseUrl || !serviceRoleKey) {
    throw new Error('Missing SUPABASE_URL or one of SUPABASE_SERVICE_ROLE_KEY/SUPABASE_SECRET_KEY/SUPABASE_SERVICE_KEY.');
  }

  if (classifySupabaseKey(serviceRoleKey) === 'publishable') {
    throw new Error('Supabase admin key is publishable; use secret/service_role key for backend functions.');
  }

  client = createClient(supabaseUrl, serviceRoleKey, {
    auth: {
      persistSession: false,
      autoRefreshToken: false
    }
  });

  return client;
};

module.exports = {
  getSupabaseAdmin
};
