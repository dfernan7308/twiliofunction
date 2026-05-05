const { json } = require('./_lib/http');
const { getSupabaseAdmin } = require('./_lib/supabase');
const { requireAuth } = require('./_lib/auth');

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return json(200, { ok: true });
  }

  if (event.httpMethod !== 'GET') {
    return json(405, { error: 'Method not allowed' });
  }

  const auth = requireAuth(event, { adminOnly: true });
  if (!auth.ok) {
    return json(auth.statusCode, auth.body);
  }

  try {
    const supabase = getSupabaseAdmin();
    const { data, error } = await supabase
      .from('app_users')
      .select('id, username, email, role, phone, created_at, is_active')
      .order('created_at', { ascending: false });

    if (error) {
      return json(500, { error: 'Failed to fetch users', details: error.message });
    }

    return json(200, { users: data || [] });
  } catch (error) {
    return json(500, { error: 'Unexpected error', details: error.message });
  }
};
