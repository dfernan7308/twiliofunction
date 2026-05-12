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

    const supabase = getSupabaseAdmin();

    const { count: linkedUsers, error: linkedUsersError } = await supabase
      .from('app_users')
      .select('id', { count: 'exact', head: true })
      .eq('area_id', id);

    if (linkedUsersError) {
      return json(500, { error: 'Failed to check linked users', details: linkedUsersError.message });
    }

    if (Number(linkedUsers || 0) > 0) {
      return json(409, { error: 'Cannot delete area with linked users', linkedUsers: Number(linkedUsers) });
    }

    const { error } = await supabase
      .from('areas')
      .delete()
      .eq('id', id);

    if (error) {
      return json(500, { error: 'Failed to delete area', details: error.message });
    }

    return json(200, { ok: true });
  } catch (error) {
    return json(500, { error: 'Unexpected error', details: error.message });
  }
};