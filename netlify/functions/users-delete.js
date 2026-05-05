const { json, parseJsonBody } = require('./_lib/http');
const { getSupabaseAdmin } = require('./_lib/supabase');
const { requireAuth } = require('./_lib/auth');

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') {
    return json(200, { ok: true });
  }

  if (event.httpMethod !== 'POST') {
    return json(405, { error: 'Method not allowed' });
  }

  const auth = requireAuth(event, { adminOnly: true });
  if (!auth.ok) {
    return json(auth.statusCode, auth.body);
  }

  try {
    const body = parseJsonBody(event);
    const id = String(body.id || '').trim();

    if (!id) {
      return json(400, { error: 'id is required' });
    }

    if (id === String(auth.user.sub || '')) {
      return json(400, { error: 'You cannot delete your own active user' });
    }

    const supabase = getSupabaseAdmin();
    const { error } = await supabase
      .from('app_users')
      .delete()
      .eq('id', id);

    if (error) {
      return json(500, { error: 'Failed to delete user', details: error.message });
    }

    return json(200, { ok: true });
  } catch (error) {
    return json(500, { error: 'Unexpected error', details: error.message });
  }
};
