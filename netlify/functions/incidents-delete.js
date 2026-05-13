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

    if (!id || !/^-?\d+$/.test(id)) {
      return json(400, { error: 'id is required' });
    }

    const supabase = getSupabaseAdmin();
    const { data: deletedRows, error } = await supabase
      .from('incidents')
      .delete()
      .eq('id', id)
      .select('id');

    if (error) {
      return json(500, { error: 'Failed to delete incident', details: error.message });
    }

    if (!Array.isArray(deletedRows) || !deletedRows.length) {
      return json(404, { error: 'Incident not found' });
    }

    return json(200, { ok: true, deletedId: String(deletedRows[0].id) });
  } catch (error) {
    return json(500, { error: 'Unexpected error', details: error.message });
  }
};
