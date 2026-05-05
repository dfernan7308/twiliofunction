const { json, parseJsonBody } = require('./_lib/http');
const { getSupabaseAdmin } = require('./_lib/supabase');
const { requireAuth, hashPassword, normalizePhone, normalizeEmail } = require('./_lib/auth');

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
    const username = String(body.username || '').trim();
    const email = normalizeEmail(body.email);
    const phone = String(body.phone || '').trim();
    const role = String(body.role || '').trim().toLowerCase();
    const password = String(body.password || '').trim();
    const isActive = typeof body.is_active === 'boolean' ? body.is_active : true;

    if (!id || !username || !email || !phone || !role) {
      return json(400, { error: 'id, username, email, phone and role are required' });
    }

    if (!['user', 'admin'].includes(role)) {
      return json(400, { error: 'role must be user or admin' });
    }

    const updatePayload = {
      username,
      email,
      phone,
      phone_normalized: normalizePhone(phone),
      role,
      is_active: isActive
    };

    if (password) {
      updatePayload.password_hash = await hashPassword(password);
    }

    const supabase = getSupabaseAdmin();
    const { data, error } = await supabase
      .from('app_users')
      .update(updatePayload)
      .eq('id', id)
      .select('id, username, email, role, phone, created_at, is_active')
      .maybeSingle();

    if (error) {
      const status = error.code === '23505' ? 409 : 500;
      return json(status, { error: 'Failed to update user', details: error.message });
    }

    if (!data) {
      return json(404, { error: 'User not found' });
    }

    return json(200, { user: data });
  } catch (error) {
    return json(500, { error: 'Unexpected error', details: error.message });
  }
};
