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
    const id = Number(body.id);

    if (!id || Number.isNaN(id)) {
      return json(400, { error: 'id is required' });
    }

    const supabase = getSupabaseAdmin();
    const { error } = await supabase
      .from('incidents')
      .delete()
      .eq('id', id);

    if (error) {
      return json(500, { error: 'Failed to delete incident', details: error.message });
    }

    return json(200, { ok: true });
  } catch (error) {
    return json(500, { error: 'Unexpected error', details: error.message });
  }
};
