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
    const username = String(body.username || '').trim();
    const email = normalizeEmail(body.email);
    const password = String(body.password || '').trim();
    const phone = String(body.phone || '').trim();
    const areaId = String(body.area_id || body.areaId || '').trim();
    const role = String(body.role || 'user').trim().toLowerCase();

    if (!username || !email || !password || !phone || !areaId) {
      return json(400, { error: 'username, email, password, phone and area_id are required' });
    }

    if (!['user', 'admin'].includes(role)) {
      return json(400, { error: 'role must be user or admin' });
    }

    const passwordHash = await hashPassword(password);
    const phoneNormalized = normalizePhone(phone);

    const supabase = getSupabaseAdmin();

    const { data: area, error: areaError } = await supabase
      .from('areas')
      .select('id, is_active')
      .eq('id', areaId)
      .maybeSingle();

    if (areaError) {
      return json(500, { error: 'Failed to validate area', details: areaError.message });
    }

    if (!area) {
      return json(400, { error: 'Invalid area_id' });
    }

    if (!area.is_active) {
      return json(400, { error: 'Selected area is inactive' });
    }

    const { data, error } = await supabase
      .from('app_users')
      .insert({
        username,
        email,
        password_hash: passwordHash,
        phone,
        phone_normalized: phoneNormalized,
        area_id: areaId,
        role,
        is_active: true
      })
      .select('id, username, email, role, phone, area_id, area:areas(id, code, name), created_at, is_active')
      .single();

    if (error) {
      const status = error.code === '23505' ? 409 : 500;
      return json(status, { error: 'Failed to create user', details: error.message });
    }

    return json(201, { user: data });
  } catch (error) {
    return json(500, { error: 'Unexpected error', details: error.message });
  }
};
