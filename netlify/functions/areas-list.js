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

  const auth = requireAuth(event);
  if (!auth.ok) {
    return json(auth.statusCode, auth.body);
  }

  try {
    const supabase = getSupabaseAdmin();
    const { data, error } = await supabase
      .from('areas')
      .select('id, code, name, tags, is_active, created_at')
      .order('name', { ascending: true });

    if (error) {
      return json(500, { error: 'Failed to fetch areas', details: error.message });
    }

    return json(200, { areas: data || [] });
  } catch (error) {
    return json(500, { error: 'Unexpected error', details: error.message });
  }
};